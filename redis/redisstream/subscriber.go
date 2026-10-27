package redisstream

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
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
	// sweepInterval is the pause between two sweeps of the stale consumers of a group.
	sweepInterval = 5 * time.Minute
	// staleIdle is how many times ClaimIdle a consumer without pending entries stays idle before a
	// sweep deletes it. Since Redis 7.2, a live consumer is never that idle: it reads about once a
	// second, or holds the entries it handles. Before, only the entries it gets reset its idle
	// time, so the live consumers of a quiet group are deleted too, and come back with their next
	// entry.
	staleIdle = 10

	// retryConsumer owns the entries that wait for a retry.
	retryConsumer = "forgo-retry"
	// claimStart starts a scan of the pending entries, and ends one when claim returns it.
	claimStart = "0-0"
)

// errClaimReply is returned for an XCLAIM reply that does not hold stream entries.
var errClaimReply = errors.New("unexpected XCLAIM reply")

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

// subscriber is the worker.ISubscriber of a SubscriberConfig. It reads and claims as a consumer of its
// own, which Close deletes from the group when it holds no pending entries.
type subscriber struct {
	conf    SubscriberConfig
	client  redis.UniversalClient
	streams *streams
	// consumer is the name the subscriber reads and claims as.
	consumer string
	// concurrency is the number of entries handled at once, at most.
	concurrency int
	// reading is the STREAMS argument of the reads: new entries of the stream.
	reading  []string
	inflight inflight

	// staleAfter is how long a consumer without pending entries stays idle before a sweep deletes
	// it.
	staleAfter time.Duration

	// block, claimEvery, beat, errorWait and sweepEvery time the reads, the scans, the heartbeat,
	// the pauses after errors and the sweeps. Tests shorten them.
	block      time.Duration
	claimEvery time.Duration
	beat       time.Duration
	errorWait  time.Duration
	sweepEvery time.Duration
}

func newSubscriber(client redis.UniversalClient, streams *streams, conf SubscriberConfig, consumer string, concurrency int) *subscriber {
	return &subscriber{
		conf:        conf,
		client:      client,
		streams:     streams,
		consumer:    consumer,
		concurrency: max(concurrency, 1),
		reading:     []string{conf.Stream, ">"},
		inflight:    inflight{deliveries: make(map[*delivery]struct{})},
		staleAfter:  staleIdle * conf.ClaimIdle,
		block:       readBlock,
		claimEvery:  claimInterval,
		beat:        conf.ClaimIdle / 3,
		errorWait:   errorWait,
		sweepEvery:  sweepInterval,
	}
}

// Consume reads the group and hands every entry to handle, with at most the subscriber's concurrency
// in flight. A read takes no more entries than there are free handler slots, so the rest of the
// backlog stays in the stream for the other consumers. Before it reads new entries, Consume claims
// the entries that were idle for ClaimIdle: those of consumers that stopped and the retries that
// are due. Once a scan of them reached the end, and at most every sweepInterval, it sweeps the
// stale consumers of the group.
//
// It returns once ctx ends and every handler finished, or with the error that stopped it, such as
// a deleted group. While Redis cannot be reached or fails over, it keeps trying, with a pause of a
// second after every failure.
func (s *subscriber) Consume(ctx context.Context, handle func(context.Context, worker.IDelivery)) error {
	concurrency := s.concurrency
	slots := process.NewChannelSemaphore(concurrency)
	var handlers sync.WaitGroup
	// Deferred first, so the heartbeat refreshes the entries of the last handlers until they end.
	defer s.heartbeat()()
	defer handlers.Wait()

	// The commands do not end with ctx: the entries of a read are handled even when ctx ended
	// meanwhile, instead of staying pending until another consumer claims them.
	rctx := context.WithoutCancel(ctx)
	cursor, nextClaim, nextSweep := claimStart, time.Time{}, time.Time{}
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
				now := time.Now()
				nextClaim = now.Add(s.claimEvery)
				if !now.Before(nextSweep) {
					s.sweep(rctx)
					nextSweep = now.Add(s.sweepEvery)
				}
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
				handle(ctx, d)
			})
		}
	}
	return nil
}

// Close deletes the subscriber's consumer from the group, so stopped instances do not pile up in
// it. A consumer that still has pending entries, such as one whose acknowledgement failed, is kept,
// because deleting it would drop its entries from the group: the other consumers claim them once
// they were idle for ClaimIdle. Close must follow Consume.
func (s *subscriber) Close() error {
	if err := delConsumerScript.Run(context.Background(), s.client, []string{s.conf.Stream}, s.conf.Group, s.consumer).Err(); err != nil {
		return fmt.Errorf("redisstream: subscriber %s: delete consumer %s: %w", s.conf.Name, s.consumer, err)
	}
	return nil
}

// sweep deletes the consumers of the group that have no pending entries and were idle for
// staleAfter, such as those of instances that crashed before Close could delete them. It keeps its
// own consumer and the retry consumer. Deleting a consumer that is still alive is harmless: Redis
// creates it again with the next entry it reads or claims. Every deletion goes through
// delConsumerScript, so a consumer that took entries since XINFO is kept. A failed sweep is left
// to the next one.
func (s *subscriber) sweep(ctx context.Context) {
	info, err := s.client.XInfoConsumers(ctx, s.conf.Stream, s.conf.Group).Result()
	if err != nil {
		return
	}
	pipe := s.client.Pipeline()
	for _, c := range info {
		if c.Pending == 0 && c.Idle > s.staleAfter && c.Name != s.consumer && c.Name != retryConsumer {
			// EVAL rather than Run, whose fallback from EVALSHA does not work in a pipeline.
			delConsumerScript.Eval(ctx, pipe, []string{s.conf.Stream}, s.conf.Group, c.Name)
		}
	}
	if pipe.Len() > 0 {
		_, _ = pipe.Exec(ctx)
	}
}

// claim claims up to n entries of the group that were idle for ClaimIdle, scanning the pending
// entries from cursor. It returns the cursor of the next scan, which is claimStart once the scan
// reached the end.
//
// Redis finds the entries that are due (XPENDING … IDLE), in one round trip however many entries
// wait for a retry, and claims them (XCLAIM), checking their idle time again, so an entry that
// another consumer claimed in between is skipped. XAUTOCLAIM would take a round trip for every ten
// pending entries per free slot, and no new entry is read until a scan ends.
func (s *subscriber) claim(ctx context.Context, cursor string, n int) ([]redis.XMessage, string, error) {
	due, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.conf.Stream,
		Group:  s.conf.Group,
		Idle:   s.conf.ClaimIdle,
		Start:  cursor,
		End:    "+",
		Count:  int64(n),
	}).Result()
	if err != nil {
		return nil, cursor, s.claimError(err)
	}
	next := claimStart
	if len(due) == n {
		next = after(due[n-1].ID)
	}
	if len(due) == 0 {
		return nil, next, nil
	}

	args := make([]any, 0, 5+len(due))
	args = append(args, "XCLAIM", s.conf.Stream, s.conf.Group, s.consumer, s.conf.ClaimIdle.Milliseconds())
	for _, p := range due {
		args = append(args, p.ID)
	}
	reply, err := s.client.Do(ctx, args...).Slice()
	if err != nil {
		return nil, cursor, s.claimError(err)
	}
	entries, trimmed, err := claimed(reply)
	if err != nil {
		return nil, cursor, s.claimError(err)
	}
	if trimmed {
		s.dropTrimmed(ctx, due, entries)
	}
	return entries, next, nil
}

func (s *subscriber) claimError(err error) error {
	return fmt.Errorf("redisstream: subscriber %s: claim entries of group %s of stream %s: %w", s.conf.Name, s.conf.Group, s.conf.Stream, err)
}

// claimed converts the reply of XCLAIM to entries. It reports whether the reply held a null, which
// Redis before 7.0 replies for a pending entry that was trimmed from the stream, after claiming it.
// The typed XClaim of go-redis fails the whole reply on a null.
func claimed(reply []any) (entries []redis.XMessage, trimmed bool, err error) {
	entries = make([]redis.XMessage, 0, len(reply))
	for _, r := range reply {
		if r == nil {
			trimmed = true
			continue
		}
		e, ok := r.([]any)
		if !ok || len(e) < 2 {
			return nil, false, errClaimReply
		}
		id, ok := e[0].(string)
		fields, ok2 := e[1].([]any)
		if !ok || !ok2 || len(fields)%2 != 0 {
			return nil, false, errClaimReply
		}
		values := make(map[string]any, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			k, ok := fields[i].(string)
			if !ok {
				return nil, false, errClaimReply
			}
			values[k] = fields[i+1]
		}
		entries = append(entries, redis.XMessage{ID: id, Values: values})
	}
	return entries, trimmed, nil
}

// dropTrimmed acknowledges the entries of due that the claim did not return and that are no longer
// in the stream. Before 7.0, Redis keeps a pending entry that was trimmed from the stream in the
// group, and every claim takes it again; since 7.0, the claim drops it. An entry that the claim
// skipped because another consumer claimed it first is still in the stream, so it is left alone.
// A failure is left to a later claim: the trimmed entries stay pending to this consumer until they
// are idle for ClaimIdle again.
func (s *subscriber) dropTrimmed(ctx context.Context, due []redis.XPendingExt, entries []redis.XMessage) {
	returned := make(map[string]bool, len(entries))
	for _, e := range entries {
		returned[e.ID] = true
	}
	pipe := s.client.Pipeline()
	ranges := make(map[string]*redis.XMessageSliceCmd, len(due)-len(entries))
	for _, p := range due {
		if !returned[p.ID] {
			ranges[p.ID] = pipe.XRange(ctx, s.conf.Stream, p.ID, p.ID)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return
	}
	var gone []string
	for id, r := range ranges {
		if len(r.Val()) == 0 {
			gone = append(gone, id)
		}
	}
	if len(gone) > 0 {
		_ = s.client.XAck(ctx, s.conf.Stream, s.conf.Group, gone...).Err()
	}
}

// after returns the entry ID that follows id, where a scan that ended with id goes on, or
// claimStart for an ID it cannot parse. XPENDING takes exclusive ranges only since Redis 6.2, and
// miniredis not at all.
func after(id string) string {
	ms, seq, ok := strings.Cut(id, "-")
	m, err := strconv.ParseUint(ms, 10, 64)
	if !ok || err != nil {
		return claimStart
	}
	n, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return claimStart
	}
	if n == math.MaxUint64 {
		return strconv.FormatUint(m+1, 10) + "-0"
	}
	return ms + "-" + strconv.FormatUint(n+1, 10)
}

// read reads up to n new entries, waiting up to s.block for the first one.
func (s *subscriber) read(ctx context.Context, n int) ([]redis.XMessage, error) {
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
func (s *subscriber) heartbeat() (stop func()) {
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
func (s *subscriber) refresh() {
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
	deliveries map[*delivery]struct{}
	ids        []string
}

func (f *inflight) add(d *delivery) {
	f.mu.Lock()
	f.deliveries[d] = struct{}{}
	f.mu.Unlock()
}

// remove stops refreshing the entry of d. It waits for a refresh in progress, so that a refresh
// never claims an entry that was settled after the refresh took its ID, such as a retry handed to
// the retry consumer.
func (f *inflight) remove(d *delivery) {
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

// transient reports whether err may pass on its own: a lost connection, a timeout, Redis loading
// its data or failing over, or Redis refusing commands for a while. A failover also unblocks the
// reads of a master that turns into a replica (UNBLOCKED), and Redis refuses commands while a
// script runs too long (BUSY), after a snapshot failed (MISCONF), or while a master lacks the
// replicas it must write to (NOREPLICAS). Any other error reply, such as NOGROUP for a deleted
// group or NOPERM, stops the subscriber, because trying again cannot fix it.
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
		redis.IsMaxClientsError(err) ||
		redis.IsNoReplicasError(err) ||
		redis.HasErrorPrefix(err, "UNBLOCKED ") ||
		redis.HasErrorPrefix(err, "BUSY ") || // not BUSYGROUP
		redis.HasErrorPrefix(err, "MISCONF ")
}
