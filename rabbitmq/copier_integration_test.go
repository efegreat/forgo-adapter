package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// publishDirect publishes n messages to the test's exchange, with the IDs prefix-0 to prefix-n-1,
// on a connection of its own, around the test's network, and waits for the broker's confirms. It
// returns the IDs.
func (r *recoveryTest) publishDirect(t *testing.T, prefix string, n int) []string {
	t.Helper()
	conn, err := amqp.Dial(r.url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, n)
	confirms := make([]*amqp.DeferredConfirmation, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-%d", prefix, i)
		msg := publishing(worker.Message{ID: ids[i], Name: "m", Payload: []byte(`{}`)}, amqp.Persistent)
		if confirms[i], err = ch.PublishWithDeferredConfirmWithContext(context.Background(), r.name, "", false, false, msg); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, c := range confirms {
		if acked, err := c.WaitContext(ctx); err != nil || !acked {
			t.Fatalf("publish %s: acked %v, %v", ids[i], acked, err)
		}
	}
	return ids
}

// drain takes every message of queue, and returns how many it took of each message ID.
func drain(t *testing.T, url, queue string) map[string]int {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]int{}
	for {
		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get from %s: %v", queue, err)
		}
		if !ok {
			return ids
		}
		ids[msg.MessageId]++
	}
}

// waitQueued waits until queue holds n ready messages.
func waitQueued(t *testing.T, url, queue string, n int) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if q.Messages == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s holds %d messages, want %d", queue, q.Messages, n)
		}
	}
}

// burst consumes the n messages of the test's queue with n handlers, which wait until every one of
// them started, then settle their delivery with settle at once. It returns how long the
// settlements took from the moment the last handler started, and their errors.
func (r *recoveryTest) burst(t *testing.T, n int, settle func(context.Context, worker.IDelivery) error) (time.Duration, []error) {
	t.Helper()
	s, err := r.broker.NewSubscriber("sub", worker.SubscriberOptions{Concurrency: n})
	if err != nil {
		t.Fatal(err)
	}
	var started atomic.Int32
	all := make(chan struct{})
	var allStarted time.Time
	results := make(chan error, 2*n)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(ctx, func(ctx context.Context, d worker.IDelivery) {
			if started.Add(1) == int32(n) {
				allStarted = time.Now()
				close(all)
			}
			<-all
			sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			results <- settle(sctx, d)
		})
	}()
	t.Cleanup(func() {
		stop()
		<-done
		_ = s.Close()
	})

	errs := make([]error, 0, n)
	for range n {
		select {
		case err := <-results:
			errs = append(errs, err)
		case err := <-done:
			t.Fatalf("Consume returned %v after %d settlements", err, len(errs))
		case <-time.After(30 * time.Second):
			t.Fatalf("%d of %d settlements", len(errs), n)
		}
	}
	return time.Since(allStarted), errs
}

// The handlers of a subscriber that retry or reject their deliveries at once do not wait in line
// for one confirm after the other: the copier waits for their confirms at once. Every read of the
// publishing connection waits for delay, so copies one after the other would take a delay each.
func TestIntegrationCopiesInParallel(t *testing.T) {
	const (
		copies = 32
		delay  = 100 * time.Millisecond
	)
	tests := []struct {
		name   string
		queue  QueueConfig
		settle func(context.Context, worker.IDelivery) error
		// into is the queue the copies go to.
		into func(QueueConfig) string
	}{
		{
			name:   "retry",
			queue:  QueueConfig{Durable: true, Retry: RetryConfig{Initial: time.Minute, Max: time.Minute}},
			settle: func(ctx context.Context, d worker.IDelivery) error { return d.Retry(ctx, 1) },
			into:   func(q QueueConfig) string { return q.Retry.queues(q.Name)[0].name },
		},
		{
			name:   "dead letter",
			queue:  QueueConfig{Durable: true, DeadLetter: DeadLetterConfig{Enabled: true}},
			settle: func(ctx context.Context, d worker.IDelivery) error { return d.Reject(ctx) },
			into:   func(q QueueConfig) string { return q.DeadLetter.Queue },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRecoveryTest(t, Config{}, tt.queue, SubscriberConfig{})
			ids := r.publishDirect(t, "msg", copies)
			r.network.slowPublishing(delay)

			elapsed, errs := r.burst(t, copies, tt.settle)
			for i, err := range errs {
				if err != nil {
					t.Errorf("settlement %d: %v", i, err)
				}
			}
			if serial := copies * delay; elapsed >= serial/2 {
				t.Errorf("%d copies took %s, at least %s one after the other", copies, elapsed, serial)
			}

			q, _ := queueConfig(r.broker.conf, r.name)
			waitQueued(t, r.url, r.name, 0)
			got := drain(t, r.url, tt.into(q))
			for _, id := range ids {
				if got[id] != 1 {
					t.Errorf("%s copied %d times", id, got[id])
				}
			}
		})
	}
}

// checkIdle checks that the copier has no copy in flight and holds no return.
func checkIdle(t *testing.T, c *copier) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); len(c.room) > 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d copies still in flight", len(c.room))
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.returned) > 0 {
		t.Fatalf("the copier holds returns for copies that are done: %v", c.returned)
	}
}

// The broker returns, and then confirms, the copies no queue takes. With twice as many copies at once
// as the copier lets in flight, the copies of its subscriber's handlers, half of them to a queue
// that does not exist, every copy learns whether a queue took it, the copier's returns never fill
// up and hold the connection back, and the connection goes on.
func TestIntegrationUnroutableCopies(t *testing.T) {
	const copies = 128
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true, Retry: RetryConfig{Initial: time.Minute, Max: time.Minute}}, SubscriberConfig{})
	s, err := r.broker.NewSubscriber("sub", worker.SubscriberOptions{Concurrency: copies / 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	c := s.(*subscriber).copier
	// The broker can return every copy in flight before it confirms one, which this broker does
	// not do on demand: the listener must have room for all of them.
	if cap(c.room) != copies/2 || cap(c.returns) != cap(c.room) {
		t.Fatalf("the copier lets %d copies in flight with room for %d returns, want the %d of the subscriber's handlers for both", cap(c.room), cap(c.returns), copies/2)
	}
	q, _ := queueConfig(r.broker.conf, r.name)
	retryQueue := q.Retry.queues(q.Name)[0].name
	missing := r.name + ".missing"

	// Each read of the publishing connection waits, so that the returns of many copies arrive before
	// their confirms.
	r.network.slowPublishing(20 * time.Millisecond)
	errs := make([]error, copies)
	var wg sync.WaitGroup
	for i := range copies {
		wg.Go(func() {
			to := retryQueue
			if i%2 == 1 {
				to = missing
			}
			msg := publishing(worker.Message{ID: fmt.Sprintf("copy-%d", i), Payload: []byte(`{}`)}, amqp.Persistent)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs[i] = c.publish(ctx, "", to, msg, msg.MessageId)
		})
	}
	wg.Wait()
	for i, err := range errs {
		switch {
		case i%2 == 0 && err != nil:
			t.Errorf("copy-%d to %s: %v", i, retryQueue, err)
		case i%2 == 1 && !errors.Is(err, errUnroutable):
			t.Errorf("copy-%d to a missing queue = %v, want errUnroutable", i, err)
		}
	}
	checkIdle(t, c)
	got := drain(t, r.url, retryQueue)
	if len(got) != copies/2 {
		t.Errorf("%s holds %d copies, want %d", retryQueue, len(got), copies/2)
	}
	for i := 0; i < copies; i += 2 {
		if id := fmt.Sprintf("copy-%d", i); got[id] != 1 {
			t.Errorf("%s holds %s %d times", retryQueue, id, got[id])
		}
	}
	r.network.slowPublishing(0)
	r.publish(t, "after", 1)
}

// A loss of the publishing connection while the copier has copies in flight, again and again, loses
// no copy: the copies the loss cuts short requeue their deliveries, which are retried once the
// client recovered the connection. The copier still copies at once afterwards, with no copy left in
// flight.
func TestIntegrationCopierRecovery(t *testing.T) {
	const (
		copies = 32
		rounds = 4
	)
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true, Retry: RetryConfig{Initial: time.Minute, Max: time.Minute}}, SubscriberConfig{})
	s, err := r.broker.NewSubscriber("sub", worker.SubscriberOptions{Concurrency: copies})
	if err != nil {
		t.Fatal(err)
	}
	c := s.(*subscriber).copier
	var mu sync.Mutex
	retried := map[string]int{}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(ctx, func(ctx context.Context, d worker.IDelivery) {
			rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if d.Retry(rctx, 1) == nil {
				mu.Lock()
				retried[d.Message().ID]++
				mu.Unlock()
			}
		})
	}()
	t.Cleanup(func() {
		stop()
		<-done
		_ = s.Close()
	})

	// Each read of the publishing connection waits, so that the loss finds copies in flight.
	r.network.slowPublishing(20 * time.Millisecond)
	var ids []string
	for round := range rounds {
		ids = append(ids, r.publishDirect(t, fmt.Sprintf("round%d", round), copies)...)
		time.Sleep(time.Duration(rand.IntN(80)) * time.Millisecond)
		r.network.cutPublishing()
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			mu.Lock()
			missing := 0
			for _, id := range ids {
				if retried[id] == 0 {
					missing++
				}
			}
			mu.Unlock()
			if missing == 0 {
				break
			}
			select {
			case err := <-done:
				t.Fatalf("Consume returned %v in round %d", err, round)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: %d of %d messages not retried", round, missing, len(ids))
			}
		}
	}

	q, _ := queueConfig(r.broker.conf, r.name)
	retryQueue := q.Retry.queues(q.Name)[0].name
	got := drain(t, r.url, retryQueue)
	for _, id := range ids {
		if got[id] == 0 {
			t.Errorf("%s is not in %s", id, retryQueue)
		}
	}
	checkIdle(t, c)

	const delay = 100 * time.Millisecond
	r.network.slowPublishing(delay)
	start := time.Now()
	var wg sync.WaitGroup
	for i := range copies {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			msg := publishing(worker.Message{ID: fmt.Sprintf("after-%d", i), Payload: []byte(`{}`)}, amqp.Persistent)
			if err := c.publish(cctx, "", retryQueue, msg, msg.MessageId); err != nil {
				t.Errorf("copy after the recoveries: %v", err)
			}
		})
	}
	wg.Wait()
	if elapsed, serial := time.Since(start), copies*delay; elapsed >= serial/2 {
		t.Errorf("%d copies took %s after the recoveries, at least %s one after the other", copies, elapsed, serial)
	}
}
