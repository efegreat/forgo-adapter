package redisstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/server/worker"

	"github.com/redis/go-redis/v9"
)

const (
	// readBlock is how long a read waits for new entries. It bounds how long Consume takes to
	// return once its context ends.
	readBlock = time.Second
	// claimInterval is the pause between two scans for the entries to claim, once a scan reached
	// the end of the pending entries.
	claimInterval = time.Second
	// errorWait is the pause after a read or claim that failed for a reason that may pass.
	errorWait = time.Second

	// retryConsumer owns the entries that wait for a retry.
	retryConsumer = "forgo-retry"
	// claimStart starts a scan of the pending entries, and ends one when XAUTOCLAIM returns it.
	claimStart = "0-0"
)

// delConsumerScript deletes a consumer from its group while it has no pending entries. XGROUP
// DELCONSUMER drops the pending entries of the consumer from the group, so they would never be
// delivered again.
var delConsumerScript = redis.NewScript(`
if #redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]) > 0 then
	return 0
end
redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// Subscriber is the worker.ISubscriber of a SubscriberConfig. It reads and claims as a consumer of its
// own, which Close deletes from the group when it holds no pending entries.
type Subscriber struct {
	conf    SubscriberConfig
	client  redis.UniversalClient
	streams *streams
	// consumer is the name the subscriber reads and claims as.
	consumer string
	// reading is the STREAMS argument of the reads: new entries of the stream.
	reading  []string
	inflight inflight

	// block, claimEvery, beat and errorWait time the reads, the scans, the heartbeat and the
	// pauses after errors. Tests shorten them.
	block      time.Duration
	claimEvery time.Duration
	beat       time.Duration
	errorWait  time.Duration
}

func newSubscriber(client redis.UniversalClient, streams *streams, conf SubscriberConfig, consumer string) *Subscriber {
	return &Subscriber{
		conf:       conf,
		client:     client,
		streams:    streams,
		consumer:   consumer,
		reading:    []string{conf.Stream, ">"},
		inflight:   inflight{deliveries: make(map[*Delivery]struct{})},
		block:      readBlock,
		claimEvery: claimInterval,
		beat:       conf.ClaimIdle / 3,
		errorWait:  errorWait,
	}
}

// Consume reads the group and hands every entry to state.Handler, with at most state.Concurrency in
// flight. A read takes no more entries than there are free handler slots, so the rest of the
// backlog stays in the stream for the other consumers. Before it reads new entries, Consume claims
// the entries that were idle for ClaimIdle: those of consumers that stopped and the retries that
// are due.
//
// It returns once ctx ends and every handler finished, or with the error that stopped it, such as
// a deleted group. While Redis cannot be reached or fails over, it keeps trying, with a pause of a
// second after every failure.
func (s *Subscriber) Consume(ctx context.Context, state worker.ConsumerState) error {
	concurrency := max(state.Concurrency, 1)
	slots := process.NewChannelSemaphore(concurrency)
	var handlers sync.WaitGroup
	// Deferred first, so the heartbeat refreshes the entries of the last handlers until they end.
	defer s.heartbeat()()
	defer handlers.Wait()

	// The commands do not end with ctx: the entries of a read are handled even when ctx ended
	// meanwhile, instead of staying pending until another consumer claims them.
	rctx := context.WithoutCancel(ctx)
	cursor, nextClaim := claimStart, time.Time{}
	for ctx.Err() == nil {
		n := acquire(ctx, slots, concurrency)
		if n == 0 {
			break
		}

		var entries []redis.XMessage
		var err error
		for err == nil && len(entries) == 0 && !time.Now().Before(nextClaim) {
			entries, cursor, err = s.claim(rctx, cursor, n)
			if err == nil && len(entries) == 0 && cursor == claimStart {
				nextClaim = time.Now().Add(s.claimEvery)
			}
		}
		if err == nil && len(entries) == 0 {
			entries, err = s.read(rctx, n)
		}
		for range n - len(entries) {
			slots.Release()
		}
		if err != nil {
			if !transient(err) {
				return err
			}
			if !sleep(ctx, s.errorWait) {
				break
			}
			continue
		}

		for _, e := range entries {
			d := newDelivery(s, e)
			s.inflight.add(d)
			handlers.Go(func() {
				defer slots.Release()
				defer s.inflight.remove(d)
				state.Handler(ctx, d)
			})
		}
	}
	return nil
}

// Close deletes the subscriber's consumer from the group, so stopped instances do not pile up in
// it. A consumer that still has pending entries, such as one whose acknowledgement failed, is kept,
// because deleting it would drop its entries from the group: the other consumers claim them once
// they were idle for ClaimIdle. Close must follow Consume.
func (s *Subscriber) Close() error {
	if err := delConsumerScript.Run(context.Background(), s.client, []string{s.conf.Stream}, s.conf.Group, s.consumer).Err(); err != nil {
		return fmt.Errorf("redisstream: subscriber %s: delete consumer %s: %w", s.conf.Name, s.consumer, err)
	}
	return nil
}

// claim claims up to n entries of the group that were idle for ClaimIdle, scanning the pending
// entries from cursor. It returns the cursor of the next scan, which is claimStart once the scan
// reached the end.
func (s *Subscriber) claim(ctx context.Context, cursor string, n int) ([]redis.XMessage, string, error) {
	entries, next, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.conf.Stream,
		Group:    s.conf.Group,
		MinIdle:  s.conf.ClaimIdle,
		Start:    cursor,
		Count:    int64(n),
		Consumer: s.consumer,
	}).Result()
	if err != nil {
		return nil, cursor, fmt.Errorf("redisstream: subscriber %s: claim entries of group %s of stream %s: %w", s.conf.Name, s.conf.Group, s.conf.Stream, err)
	}
	return entries, next, nil
}

// read reads up to n new entries, waiting up to s.block for the first one.
func (s *Subscriber) read(ctx context.Context, n int) ([]redis.XMessage, error) {
	res, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.conf.Group,
		Consumer: s.consumer,
		Streams:  s.reading,
		Count:    int64(n),
		Block:    s.block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redisstream: subscriber %s: read group %s of stream %s: %w", s.conf.Name, s.conf.Group, s.conf.Stream, err)
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res[0].Messages, nil
}

// heartbeat refreshes the entries being handled every s.beat until stop is called, which waits for
// it.
func (s *Subscriber) heartbeat() (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(s.beat)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				s.refresh()
			}
		}
	})
	return func() {
		close(done)
		wg.Wait()
	}
}

// refresh claims the entries being handled for the subscriber's own consumer again, which resets
// their idle time without counting a delivery (JUSTID), so no other consumer claims them however
// long their handlers run. A failed refresh is repeated on the next beat: an entry is only claimed
// once it was not refreshed for ClaimIdle.
func (s *Subscriber) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), s.beat)
	defer cancel()
	s.inflight.locked(func(ids []string) {
		if len(ids) == 0 {
			return
		}
		_ = s.client.XClaimJustID(ctx, &redis.XClaimArgs{
			Stream:   s.conf.Stream,
			Group:    s.conf.Group,
			Consumer: s.consumer,
			Messages: ids,
		}).Err()
	})
}

// inflight is the set of deliveries a subscriber is handling, whose entries its heartbeat
// refreshes. It holds deliveries rather than entry IDs: a retried entry can be claimed again
// before the handler that retried it returned and removed its own delivery.
type inflight struct {
	mu         sync.Mutex
	deliveries map[*Delivery]struct{}
	ids        []string
}

func (f *inflight) add(d *Delivery) {
	f.mu.Lock()
	f.deliveries[d] = struct{}{}
	f.mu.Unlock()
}

// remove stops refreshing the entry of d. It waits for a refresh in progress, so that a refresh
// never claims an entry that was settled after the refresh took its ID, such as a retry handed to
// the retry consumer.
func (f *inflight) remove(d *Delivery) {
	f.mu.Lock()
	delete(f.deliveries, d)
	f.mu.Unlock()
}

// locked calls fn with the entry IDs of the deliveries, which stay locked until fn returns.
func (f *inflight) locked(fn func(ids []string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = f.ids[:0]
	for d := range f.deliveries {
		f.ids = append(f.ids, d.entry.ID)
	}
	fn(f.ids)
}

// acquire waits for a free slot, then takes the other free ones, up to n in all. It returns 0 when
// ctx ends first.
func acquire(ctx context.Context, slots *process.ChannelSemaphore, n int) int {
	if slots.WaitAcquire(ctx) != nil {
		return 0
	}
	got := 1
	for got < n && slots.Acquire() == nil {
		got++
	}
	return got
}

// sleep waits for d and reports whether ctx is still running.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// transient reports whether err may pass on its own: a lost connection, a timeout, or Redis
// loading its data or failing over. Any other error reply, such as NOGROUP for a deleted group or
// NOPERM, stops the subscriber, because trying again cannot fix it.
func transient(err error) bool {
	if errors.Is(err, redis.ErrClosed) {
		return false
	}
	var reply redis.Error
	if !errors.As(err, &reply) {
		return true
	}
	return redis.IsLoadingError(err) ||
		redis.IsReadOnlyError(err) ||
		redis.IsMasterDownError(err) ||
		redis.IsClusterDownError(err) ||
		redis.IsTryAgainError(err) ||
		redis.IsMaxClientsError(err)
}
