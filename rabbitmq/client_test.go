package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeConsumeChannel is a consumeChannel that delivers what the test sends, as the client does: each
// Consume opens a stream, and Cancel ends the current one once it handed out what it buffers.
type fakeConsumeChannel struct {
	mu         sync.Mutex
	prefetch   int
	qosErr     error
	consumeErr error
	cancelErr  error
	closed     bool
	queue      string
	tag        string
	cancels    int
	streams    []chan amqp.Delivery
	// ended holds the streams that already ended, which Cancel leaves alone, as the client does.
	ended map[chan amqp.Delivery]bool
	// log, when set, records the calls, to check their order against other fakes.
	log *callLog
}

func (c *fakeConsumeChannel) Qos(prefetchCount, _ int, _ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prefetch = prefetchCount
	return c.qosErr
}

func (c *fakeConsumeChannel) Consume(queue, tag string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log.add("consume " + tag)
	c.queue, c.tag = queue, tag
	if c.consumeErr != nil {
		return nil, c.consumeErr
	}
	s := make(chan amqp.Delivery, 16)
	c.streams = append(c.streams, s)
	return s, nil
}

func (c *fakeConsumeChannel) Cancel(tag string, _ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log.add("cancel " + tag)
	if c.cancelErr != nil {
		return c.cancelErr
	}
	c.cancels++
	// A closed channel with a buffer still hands out what it holds, like the client's stream.
	c.end(c.streams[len(c.streams)-1])
	return nil
}

// end ends stream s once. The caller holds c.mu.
func (c *fakeConsumeChannel) end(s chan amqp.Delivery) {
	if c.ended[s] {
		return
	}
	if c.ended == nil {
		c.ended = make(map[chan amqp.Delivery]bool)
	}
	c.ended[s] = true
	close(s)
}

// cancelByBroker ends the i-th stream, as the client does when the broker cancels its consumer.
func (c *fakeConsumeChannel) cancelByBroker(t *testing.T, i int) {
	t.Helper()
	s := c.stream(t, i)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.end(s)
}

func (c *fakeConsumeChannel) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeConsumeChannel) Close() error { return nil }

// stream waits for the i-th stream that Consume opened.
func (c *fakeConsumeChannel) stream(t *testing.T, i int) chan amqp.Delivery {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		c.mu.Lock()
		if len(c.streams) > i {
			s := c.streams[i]
			c.mu.Unlock()
			return s
		}
		c.mu.Unlock()
	}
	t.Fatalf("no stream %d", i)
	return nil
}

func (c *fakeConsumeChannel) setClosed(closed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = closed
}

// callLog records calls across fakes. A nil log records nothing.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(call string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call)
}

func (l *callLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.calls, ", ")
}

// tagsOf returns the consumer tags of the subscriber name, forgo-<name>-1 first, as a broker hands
// them out to its only subscriber.
func tagsOf(name string) func() string {
	var seq uint64
	return func() string {
		seq++
		return consumerTag(name, seq)
	}
}

func newTestSubscriber(ch *fakeConsumeChannel) *subscriber {
	return &subscriber{subscription: newSubscription(SubscriberConfig{Name: "s", Queue: "q"}, ch, tagsOf("s")), concurrency: 1}
}

// consume runs s.Consume in the background with the handler, with at most concurrency in flight,
// and returns its result.
func consume(s *subscriber, concurrency int, handler func(context.Context, worker.IDelivery)) (context.CancelFunc, <-chan error) {
	s.concurrency = concurrency
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(ctx, handler)
	}()
	return cancel, done
}

func waitConsume(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return")
		return nil
	}
}

func TestPrefetch(t *testing.T) {
	tests := []struct {
		configured, concurrency, want int
	}{
		{configured: 0, concurrency: 8, want: 8},
		{configured: 0, concurrency: 0, want: 1},
		{configured: 0, concurrency: 100000, want: 65535},
		{configured: 50, concurrency: 8, want: 50},
		{configured: -1, concurrency: 8, want: 0},
	}
	for _, tt := range tests {
		if got := prefetch(tt.configured, tt.concurrency); got != tt.want {
			t.Errorf("prefetch(%d, %d) = %d, want %d", tt.configured, tt.concurrency, got, tt.want)
		}
	}
}

func TestSubscriberConsume(t *testing.T) {
	ch := &fakeConsumeChannel{}
	s := &subscriber{subscription: newSubscription(SubscriberConfig{Name: "billing", Queue: "billing"}, ch, tagsOf("billing"))}

	ack := &fakeAcknowledger{}
	var (
		mu  sync.Mutex
		ids []string
	)
	handled := make(chan struct{}, 3)
	cancel, done := consume(s, 4, func(ctx context.Context, d worker.IDelivery) {
		mu.Lock()
		ids = append(ids, d.Message().ID)
		mu.Unlock()
		_ = d.Complete(ctx)
		handled <- struct{}{}
	})

	stream := ch.stream(t, 0)
	for _, id := range []string{"a", "b", "c"} {
		stream <- amqp.Delivery{MessageId: id, Acknowledger: ack, DeliveryTag: 1}
	}
	for range 3 {
		<-handled
	}
	cancel()
	if err := waitConsume(t, done); err != nil {
		t.Fatalf("Consume = %v", err)
	}

	if ch.prefetch != 4 || ch.queue != "billing" || ch.tag != "forgo-billing-1" {
		t.Fatalf("prefetch %d on queue %q under tag %q, want 4 on billing under forgo-billing-1", ch.prefetch, ch.queue, ch.tag)
	}
	if len(ids) != 3 || len(ack.calls) != 3 {
		t.Fatalf("handled %v with acknowledgements %v", ids, ack.calls)
	}
	if s.consuming {
		t.Fatal("the subscription still counts as consuming once Consume returned")
	}
}

// A delivery that arrives while every slot is taken waits for one, and Consume goes on once a
// handler returns.
func TestSubscriberWaitsForAFreeSlot(t *testing.T) {
	ch := &fakeConsumeChannel{}
	s := newTestSubscriber(ch)

	ack := &fakeAcknowledger{}
	release := make(chan struct{})
	started := make(chan string, 3)
	cancel, done := consume(s, 1, func(ctx context.Context, d worker.IDelivery) {
		started <- d.Message().ID
		if d.Message().ID == "a" {
			<-release
		}
		_ = d.Complete(ctx)
	})
	defer cancel()

	stream := ch.stream(t, 0)
	stream <- amqp.Delivery{MessageId: "a", Acknowledger: ack, DeliveryTag: 1}
	stream <- amqp.Delivery{MessageId: "b", Acknowledger: ack, DeliveryTag: 2}
	waitStarted(t, started, "a")
	waitReceived(t, stream)
	select {
	case id := <-started:
		t.Fatalf("handler of %s started while the only slot was taken", id)
	case err := <-done:
		t.Fatalf("Consume = %v while every slot was taken, want it to wait for one", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	waitStarted(t, started, "b")
	stream <- amqp.Delivery{MessageId: "c", Acknowledger: ack, DeliveryTag: 3}
	waitStarted(t, started, "c")

	cancel()
	if err := waitConsume(t, done); err != nil {
		t.Fatalf("Consume = %v", err)
	}
	if got, want := strings.Join(ack.calls, ", "), "ack 1 multiple=false, ack 2 multiple=false, ack 3 multiple=false"; got != want {
		t.Fatalf("acknowledgements %q, want %q", got, want)
	}
}

// Once ctx ends while Consume waits for a slot, Consume leaves the waiting delivery unsettled, for
// the broker to redeliver, and returns nil once the running handlers returned.
func TestSubscriberStopsWhileWaitingForASlot(t *testing.T) {
	ch := &fakeConsumeChannel{}
	s := newTestSubscriber(ch)

	ack := &fakeAcknowledger{}
	release := make(chan struct{})
	started := make(chan string, 2)
	cancel, done := consume(s, 1, func(ctx context.Context, d worker.IDelivery) {
		started <- d.Message().ID
		<-release
		_ = d.Complete(ctx)
	})

	stream := ch.stream(t, 0)
	stream <- amqp.Delivery{MessageId: "a", Acknowledger: ack, DeliveryTag: 1}
	stream <- amqp.Delivery{MessageId: "b", Acknowledger: ack, DeliveryTag: 2}
	waitStarted(t, started, "a")
	waitReceived(t, stream)

	cancel()
	select {
	case err := <-done:
		t.Fatalf("Consume = %v before the running handler returned", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := waitConsume(t, done); err != nil {
		t.Fatalf("Consume = %v, want nil", err)
	}

	select {
	case id := <-started:
		t.Fatalf("handler of %s started after ctx ended", id)
	default:
	}
	if got, want := strings.Join(ack.calls, ", "), "ack 1 multiple=false"; got != want {
		t.Fatalf("acknowledgements %q, want %q", got, want)
	}
}

// waitStarted waits for the next handler to start, and checks that it handles the message id.
func waitStarted(t *testing.T, started <-chan string, id string) {
	t.Helper()
	select {
	case got := <-started:
		if got != id {
			t.Fatalf("handler of %s started, want %s", got, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("handler of %s did not start", id)
	}
}

// waitReceived waits until Consume received every delivery the test sent on stream.
func waitReceived(t *testing.T, stream chan amqp.Delivery) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); len(stream) > 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("Consume left %d deliveries on the stream", len(stream))
		}
	}
}

func TestSubscriberConsumeErrors(t *testing.T) {
	tests := []struct {
		name    string
		ch      *fakeConsumeChannel
		wantMsg string
	}{
		{name: "prefetch refused", ch: &fakeConsumeChannel{qosErr: errors.New("refused")}, wantMsg: "subscriber s: set prefetch: refused"},
		{name: "consume refused", ch: &fakeConsumeChannel{consumeErr: errors.New("exclusive")}, wantMsg: "subscriber s: consume queue q: exclusive"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newTestSubscriber(tt.ch).Consume(context.Background(), func(context.Context, worker.IDelivery) {})
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Consume = %v, want one containing %q", err, tt.wantMsg)
			}
		})
	}
}

// A stream that ends with its channel, without a recovery replacing it, because the recovery is
// disabled or gave up, ends Consume.
func TestSubscriberStreamClosed(t *testing.T) {
	ch := &fakeConsumeChannel{}
	_, done := consume(newTestSubscriber(ch), 1, func(context.Context, worker.IDelivery) {})
	ch.setClosed(true)
	ch.cancelByBroker(t, 0)

	if err := waitConsume(t, done); !errors.Is(err, errDeliveriesClosed) {
		t.Fatalf("Consume = %v, want errDeliveriesClosed", err)
	}
	if len(ch.streams) != 1 {
		t.Fatalf("%d streams, want 1: Consume subscribed again on a closed channel", len(ch.streams))
	}
}

// The broker cancels the consumer while its channel stays open, as a quorum queue does once a
// delivery outlives its consumer timeout. Consume subscribes again, and the delivery that was in
// flight is still acknowledged on the channel, under its own tag.
func TestSubscriberConsumesAgainWhenCancelled(t *testing.T) {
	ch := &fakeConsumeChannel{log: &callLog{}}
	s := newTestSubscriber(ch)
	ack := &fakeAcknowledger{}
	type handled struct {
		id  string
		err error
	}
	results := make(chan handled, 10)
	started, release := make(chan struct{}), make(chan struct{})
	cancel, done := consume(s, 2, func(ctx context.Context, d worker.IDelivery) {
		if d.Message().ID == "in-flight" {
			close(started)
			<-release
		}
		results <- handled{id: d.Message().ID, err: d.Complete(ctx)}
	})

	ch.stream(t, 0) <- amqp.Delivery{MessageId: "in-flight", Acknowledger: ack, DeliveryTag: 1}
	<-started
	ch.cancelByBroker(t, 0)
	ch.stream(t, 1) <- amqp.Delivery{MessageId: "after", Acknowledger: ack, DeliveryTag: 2}
	close(release)

	got := map[string]error{}
	for range 2 {
		select {
		case h := <-results:
			got[h.id] = h.err
		case <-time.After(5 * time.Second):
			t.Fatalf("handled %v, want in-flight and after", got)
		}
	}
	for _, id := range []string{"in-flight", "after"} {
		if err, ok := got[id]; !ok || err != nil {
			t.Fatalf("%s completed with %v, handled %v", id, err, got)
		}
	}
	ack.mu.Lock()
	calls := strings.Join(ack.calls, ", ")
	ack.mu.Unlock()
	if !strings.Contains(calls, "ack 1 multiple=false") || !strings.Contains(calls, "ack 2 multiple=false") {
		t.Fatalf("acknowledgements %q, want tags 1 and 2", calls)
	}
	// The broker keeps the cancelled consumer's tag in use. A quorum queue keeps the consumer too,
	// so the subscriber cancels it first.
	if ch.tag != "forgo-s-2" {
		t.Fatalf("subscribed again under %q, want forgo-s-2", ch.tag)
	}
	if got, want := ch.log.String(), "consume forgo-s-1, cancel forgo-s-1, consume forgo-s-2"; got != want {
		t.Fatalf("calls %q, want %q", got, want)
	}

	cancel()
	if err := waitConsume(t, done); err != nil {
		t.Fatalf("Consume = %v", err)
	}
}

// A consumer that the broker cancelled and that cannot subscribe again, such as one whose queue was
// deleted, is lost.
func TestSubscriberLostWhenCancelled(t *testing.T) {
	notFound := &amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no queue 'q' in vhost '/'"}
	ch := &fakeConsumeChannel{}
	s := newTestSubscriber(ch)
	_, done := consume(s, 1, func(context.Context, worker.IDelivery) {})
	ch.stream(t, 0)
	ch.mu.Lock()
	ch.consumeErr = notFound
	ch.mu.Unlock()
	ch.cancelByBroker(t, 0)

	err := waitConsume(t, done)
	if !errors.Is(err, errConsumerLost) || !errors.Is(err, notFound) || !strings.Contains(err.Error(), "consumer forgo-s-2 of queue q, replacing forgo-s-1, which the broker cancelled") {
		t.Fatalf("Consume = %v, want errConsumerLost with NOT_FOUND", err)
	}
	// The broker closed the channel: its recovery does not subscribe the lost subscription again.
	if detached, err := s.detach(); detached || err != nil {
		t.Fatalf("detach of a lost subscription = %v, %v", detached, err)
	}
}

// A consumer that the broker cancelled and that the subscriber cannot cancel itself, because its
// channel failed meanwhile, is lost.
func TestSubscriberLostWhenCancelFails(t *testing.T) {
	cancelErr := &amqp.Error{Code: amqp.ChannelError, Reason: "channel closed"}
	ch := &fakeConsumeChannel{cancelErr: cancelErr}
	s := newTestSubscriber(ch)
	_, done := consume(s, 1, func(context.Context, worker.IDelivery) {})
	ch.cancelByBroker(t, 0)

	err := waitConsume(t, done)
	if !errors.Is(err, errConsumerLost) || !errors.Is(err, cancelErr) || !strings.Contains(err.Error(), "consumer forgo-s-1 of queue q, which the broker cancelled: cancel it") {
		t.Fatalf("Consume = %v, want errConsumerLost with the cancellation's error", err)
	}
	if len(ch.streams) != 1 {
		t.Fatalf("%d streams, want 1: Consume subscribed again", len(ch.streams))
	}
}

func TestSubscriberDropsAcksAcrossRecovery(t *testing.T) {
	ch := &fakeConsumeChannel{}
	s := newTestSubscriber(ch)

	ack := &fakeAcknowledger{}
	received := make(chan worker.IDelivery, 1)
	cancel, done := consume(s, 1, func(_ context.Context, d worker.IDelivery) {
		received <- d
	})

	ch.stream(t, 0) <- amqp.Delivery{MessageId: "old", Acknowledger: ack, DeliveryTag: 3}
	d := <-received

	// The channel is lost and recovered while the handler still runs.
	ch.setClosed(true)
	s.lose(false)

	if err := d.Complete(context.Background()); !errors.Is(err, errStaleDelivery) {
		t.Fatalf("Complete after recovery = %v, want errStaleDelivery", err)
	}
	if len(ack.calls) != 0 {
		t.Fatalf("the late acknowledgement reached the recovered channel: %v", ack.calls)
	}
	cancel()
	waitConsume(t, done)
}

// The recovery of a channel ends the stream of the lost channel, whose buffered deliveries never
// reach a handler, and Consume goes on with the deliveries of the new stream.
func TestSubscriberResumesOnNewStream(t *testing.T) {
	ch := &fakeConsumeChannel{}
	s := newTestSubscriber(ch)

	oldAck, newAck := &fakeAcknowledger{}, &fakeAcknowledger{}
	type handled struct {
		id  string
		err error
	}
	results := make(chan handled, 10)
	started, release := make(chan struct{}), make(chan struct{})
	cancel, done := consume(s, 1, func(ctx context.Context, d worker.IDelivery) {
		if d.Message().ID == "in-flight" {
			close(started)
			<-release
		}
		results <- handled{id: d.Message().ID, err: d.Complete(ctx)}
	})

	old := ch.stream(t, 0)
	old <- amqp.Delivery{MessageId: "in-flight", Acknowledger: oldAck, DeliveryTag: 1}
	<-started

	// The channel is lost: the client still buffers two deliveries of it when it reopens it.
	ch.setClosed(true)
	s.lose(false)
	old <- amqp.Delivery{MessageId: "buffered-1", Acknowledger: oldAck, DeliveryTag: 2}
	old <- amqp.Delivery{MessageId: "buffered-2", Acknowledger: oldAck, DeliveryTag: 3}
	ch.setClosed(false)

	if detached, err := s.detach(); !detached || err != nil {
		t.Fatalf("detach = %v, %v", detached, err)
	}
	s.resubscribe(func() error { return nil })
	ch.stream(t, 1) <- amqp.Delivery{MessageId: "new", Acknowledger: newAck, DeliveryTag: 2}
	close(release)

	got := map[string]error{}
	for range 2 {
		select {
		case h := <-results:
			got[h.id] = h.err
		case <-time.After(5 * time.Second):
			t.Fatalf("handled %v, want in-flight and new", got)
		}
	}
	if err, ok := got["in-flight"]; !ok || !errors.Is(err, errStaleDelivery) {
		t.Fatalf("the in-flight delivery completed with %v, want errStaleDelivery", err)
	}
	if err, ok := got["new"]; !ok || err != nil {
		t.Fatalf("the delivery of the new stream completed with %v", err)
	}
	select {
	case h := <-results:
		t.Fatalf("a delivery of the lost channel reached a handler: %s", h.id)
	case <-time.After(50 * time.Millisecond):
	}
	if len(oldAck.calls) != 0 || len(newAck.calls) != 1 || newAck.calls[0] != "ack 2 multiple=false" {
		t.Fatalf("acknowledgements: lost channel %v, reopened channel %v", oldAck.calls, newAck.calls)
	}

	cancel()
	if err := waitConsume(t, done); err != nil {
		t.Fatalf("Consume = %v", err)
	}
}

// Consume waits for the new stream once the recovery ended the old one, however long it takes.
func TestSubscriberWaitsForResubscribe(t *testing.T) {
	ch := &fakeConsumeChannel{}
	s := newTestSubscriber(ch)
	ack := &fakeAcknowledger{}
	handled := make(chan string, 1)
	cancel, done := consume(s, 1, func(ctx context.Context, d worker.IDelivery) {
		_ = d.Complete(ctx)
		handled <- d.Message().ID
	})
	ch.stream(t, 0)

	ch.setClosed(true)
	s.lose(false)
	ch.setClosed(false)
	if _, err := s.detach(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("Consume returned %v before the recovery subscribed again", err)
	case <-time.After(50 * time.Millisecond):
	}

	s.resubscribe(func() error { return nil })
	ch.stream(t, 1) <- amqp.Delivery{MessageId: "new", Acknowledger: ack, DeliveryTag: 1}
	select {
	case id := <-handled:
		if id != "new" {
			t.Fatalf("handled %s", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the delivery of the new stream was not handled")
	}

	cancel()
	if err := waitConsume(t, done); err != nil {
		t.Fatalf("Consume = %v", err)
	}
}

func TestSubscriberStopsWhenConsumerLost(t *testing.T) {
	refused := &amqp.Error{Code: amqp.AccessRefused, Reason: "queue 'q' in exclusive use"}
	tests := []struct {
		name    string
		recover func(s *subscriber, ch *fakeConsumeChannel)
		want    []error
		wantMsg string
	}{
		{
			name: "subscribing again failed",
			recover: func(s *subscriber, ch *fakeConsumeChannel) {
				ch.mu.Lock()
				ch.consumeErr = refused
				ch.mu.Unlock()
				s.resubscribe(func() error { return nil })
			},
			want:    []error{errConsumerLost, refused},
			wantMsg: "consumer forgo-s-1 of queue q",
		},
		{
			name: "declaring the queue again failed",
			recover: func(s *subscriber, _ *fakeConsumeChannel) {
				s.resubscribe(func() error { return refused })
			},
			want: []error{errConsumerLost, refused},
		},
		{
			name:    "the recovery gave up",
			recover: func(s *subscriber, _ *fakeConsumeChannel) { s.giveUp(func() bool { return false }) },
			want:    []error{errConsumerLost, errRecoveryFailed},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := &fakeConsumeChannel{}
			s := newTestSubscriber(ch)
			_, done := consume(s, 1, func(context.Context, worker.IDelivery) {})
			ch.stream(t, 0)
			if _, err := s.detach(); err != nil {
				t.Fatal(err)
			}

			tt.recover(s, ch)

			err := waitConsume(t, done)
			for _, want := range tt.want {
				if !errors.Is(err, want) {
					t.Fatalf("Consume = %v, want it to wrap %v", err, want)
				}
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Consume = %v, want one containing %q", err, tt.wantMsg)
			}

			// A lost subscription is never subscribed again.
			if detached, err := s.detach(); detached || err != nil {
				t.Fatalf("detach of a lost subscription = %v, %v", detached, err)
			}
		})
	}
}

func TestSubscriptionLose(t *testing.T) {
	ch := &fakeConsumeChannel{}
	s := newSubscription(SubscriberConfig{Name: "s", Queue: "q"}, ch, tagsOf("s"))

	// The hook of a channel that is open again, because its connection's recovery reopened it,
	// keeps the new stream.
	s.lose(false)
	if s.guard.generation() != 0 {
		t.Fatal("a late hook marked the reopened channel's deliveries stale")
	}
	ch.setClosed(true)
	s.lose(false)
	if s.guard.generation() != 1 {
		t.Fatal("the hook of a closed channel did not mark its deliveries stale")
	}
	ch.setClosed(false)
	s.lose(true)
	if s.guard.generation() != 2 {
		t.Fatal("a lost connection did not mark the deliveries stale")
	}
}

func TestSubscriptionDetach(t *testing.T) {
	t.Run("not consuming", func(t *testing.T) {
		ch := &fakeConsumeChannel{}
		s := newSubscription(SubscriberConfig{Name: "s", Queue: "q"}, ch, tagsOf("s"))
		if detached, err := s.detach(); detached || err != nil || ch.cancels != 0 {
			t.Fatalf("detach without a consumer = %v, %v, %d cancels", detached, err, ch.cancels)
		}
	})

	t.Run("cancel failed", func(t *testing.T) {
		ch := &fakeConsumeChannel{cancelErr: amqp.ErrClosed}
		s := newSubscription(SubscriberConfig{Name: "s", Queue: "q"}, ch, tagsOf("s"))
		if _, err := s.start(); err != nil {
			t.Fatal(err)
		}
		detached, err := s.detach()
		if detached || !errors.Is(err, amqp.ErrClosed) || !strings.Contains(err.Error(), "subscriber s: cancel") {
			t.Fatalf("detach = %v, %v", detached, err)
		}
		if s.replacing {
			t.Fatal("a failed detach waits for a new stream")
		}
	})

	t.Run("twice", func(t *testing.T) {
		ch := &fakeConsumeChannel{}
		s := newSubscription(SubscriberConfig{Name: "s", Queue: "q"}, ch, tagsOf("s"))
		if _, err := s.start(); err != nil {
			t.Fatal(err)
		}
		// The client's recovery failed after detach, and starts over.
		for range 2 {
			if detached, err := s.detach(); !detached || err != nil {
				t.Fatalf("detach = %v, %v", detached, err)
			}
		}
		if ch.cancels != 1 {
			t.Fatalf("%d cancellations, want 1", ch.cancels)
		}
	})

	t.Run("after Consume returned", func(t *testing.T) {
		ch := &fakeConsumeChannel{}
		s := newSubscription(SubscriberConfig{Name: "s", Queue: "q"}, ch, tagsOf("s"))
		if _, err := s.start(); err != nil {
			t.Fatal(err)
		}
		s.stop()
		if detached, err := s.detach(); !detached || err != nil {
			t.Fatalf("detach = %v, %v", detached, err)
		}
		declared := false
		s.resubscribe(func() error { declared = true; return nil })
		if declared || len(ch.streams) != 1 || s.stream != nil {
			t.Fatalf("a subscription nobody reads subscribed again: declared %v, %d streams", declared, len(ch.streams))
		}
	})
}

// settledConfirmation is a confirmation that is already settled.
type settledConfirmation struct {
	ack bool
	err error
}

func (c settledConfirmation) WaitContext(context.Context) (bool, error) {
	return c.ack, c.err
}

func TestPublisherAwait(t *testing.T) {
	notFound := &amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no exchange 'orders' in vhost '/'"}
	tests := []struct {
		name    string
		conf    settledConfirmation
		returns []amqp.Return
		closed  bool
		lost    error
		wantErr error
		wantMsg string
	}{
		{name: "confirmed", conf: settledConfirmation{ack: true}},
		{name: "nacked", conf: settledConfirmation{ack: false}, wantErr: errNacked},
		{name: "channel lost", conf: settledConfirmation{ack: false}, lost: notFound, wantErr: errChannelLost, wantMsg: "id: Exception (404)"},
		{name: "confirmed although the channel was lost", conf: settledConfirmation{ack: true}, lost: notFound},
		{name: "not confirmed in time", conf: settledConfirmation{err: context.DeadlineExceeded}, wantErr: context.DeadlineExceeded},
		{
			name:    "returned unroutable",
			conf:    settledConfirmation{ack: true},
			returns: []amqp.Return{{MessageId: "id", Exchange: "orders", RoutingKey: "order.x", ReplyText: "NO_ROUTE"}},
			wantErr: errUnroutable,
			wantMsg: `to exchange "orders" with routing key "order.x": NO_ROUTE`,
		},
		{
			name:    "return of an earlier publish is ignored",
			conf:    settledConfirmation{ack: true},
			returns: []amqp.Return{{MessageId: "earlier"}},
		},
		{name: "returns closed", conf: settledConfirmation{ack: true}, closed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returns := make(chan amqp.Return, returnBuffer)
			for _, r := range tt.returns {
				returns <- r
			}
			if tt.closed {
				close(returns)
			}
			p := &publisher{returns: returns, gate: &gate{}}
			if tt.lost != nil {
				p.gate.close(tt.lost)
				p.gate.open()
			}

			err := p.await(context.Background(), tt.conf, "id", 0)
			if tt.wantErr == nil && err != nil {
				t.Fatalf("await = %v", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("await = %v, want %v", err, tt.wantErr)
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("await = %v, want one containing %q", err, tt.wantMsg)
			}
		})
	}
}

func TestPublisherWithoutMandatoryIgnoresReturns(t *testing.T) {
	p := &publisher{} // nil returns: never mandatory
	if err := p.await(context.Background(), settledConfirmation{ack: true}, "id", 0); err != nil {
		t.Fatal(err)
	}
	p.dropReturns()
}

func TestPublisherDropsStaleReturns(t *testing.T) {
	returns := make(chan amqp.Return, returnBuffer)
	returns <- amqp.Return{MessageId: "timed-out"}
	returns <- amqp.Return{MessageId: "timed-out-too"}
	p := &publisher{returns: returns}

	p.dropReturns()
	if len(returns) != 0 {
		t.Fatalf("%d stale returns left", len(returns))
	}
}

// recordingPublishChannel records the publish and returns a confirmation that never settles.
type recordingPublishChannel struct {
	exchange, key string
	mandatory     bool
	msg           amqp.Publishing
	err           error
	notConfirming bool
	publishes     atomic.Int32
	closed        atomic.Bool
}

func (c *recordingPublishChannel) PublishWithDeferredConfirmWithContext(_ context.Context, exchange, key string, mandatory, _ bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	c.publishes.Add(1)
	c.exchange, c.key, c.mandatory, c.msg = exchange, key, mandatory, msg
	if c.err != nil || c.notConfirming {
		return nil, c.err
	}
	return new(amqp.DeferredConfirmation), nil
}

func (c *recordingPublishChannel) IsClosed() bool { return c.closed.Load() }

// closingPublishChannel fails its first publish with amqp.ErrClosed, as a channel the client closed
// before it ran the recovery hooks, and records the later ones.
type closingPublishChannel struct {
	recordingPublishChannel
	failed atomic.Bool
}

func (c *closingPublishChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	if c.failed.CompareAndSwap(false, true) {
		return nil, amqp.ErrClosed
	}
	return c.recordingPublishChannel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (c *recordingPublishChannel) Close() error { return nil }

func TestPublisherPublish(t *testing.T) {
	ch := &recordingPublishChannel{}
	p := &publisher{conf: PublisherConfig{Mandatory: true, DeliveryMode: amqp.Persistent}, ch: ch, gate: &gate{}}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := p.Publish(ctx, worker.Message{ID: "id", Name: "order.placed", Destination: "orders", Key: "order.placed"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish without a confirm = %v, want the context's error", err)
	}
	if ch.exchange != "orders" || ch.key != "order.placed" || !ch.mandatory || ch.msg.MessageId != "id" || ch.msg.DeliveryMode != amqp.Persistent {
		t.Fatalf("published %+v", ch)
	}

	ch.err = amqp.ErrClosed
	if err := p.Publish(context.Background(), worker.Message{ID: "id"}); !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("Publish on a closed channel = %v", err)
	}

	ch.err, ch.notConfirming = nil, true
	if err := p.Publish(context.Background(), worker.Message{ID: "id"}); !errors.Is(err, errNotConfirming) {
		t.Fatalf("Publish without confirm mode = %v, want errNotConfirming", err)
	}
}

// While the broker blocks the connection, Publish sends nothing and waits, until its context ends or
// the broker unblocks the connection.
func TestPublisherWaitsWhileBlocked(t *testing.T) {
	ch := &recordingPublishChannel{}
	p := &publisher{conf: PublisherConfig{DeliveryMode: amqp.Persistent}, ch: ch, gate: &gate{}, flow: newFlow()}
	p.flow.block("low on memory")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Publish(ctx, worker.Message{ID: "id"}); !errors.Is(err, errBlocked) || ch.publishes.Load() != 0 {
		t.Fatalf("Publish while blocked = %v after %d publishes, want errBlocked without a publish", err, ch.publishes.Load())
	}

	ch.err = amqp.ErrClosed // settles the publish at once
	done := make(chan error, 1)
	go func() { done <- p.Publish(context.Background(), worker.Message{ID: "id"}) }()
	time.Sleep(20 * time.Millisecond)
	if ch.publishes.Load() != 0 {
		t.Fatal("Publish sent while blocked")
	}
	p.flow.unblock()
	select {
	case err := <-done:
		if !errors.Is(err, amqp.ErrClosed) || ch.publishes.Load() != 1 {
			t.Fatalf("Publish once unblocked = %v after %d publishes", err, ch.publishes.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish kept waiting once unblocked")
	}
}

// A publisher that waits for recovery, as the subscribers' retry and dead-letter copies do, waits
// while its connection is down, instead of failing at once, and sends once the channel is back.
func TestPublisherWaitsForRecoveryWhileDown(t *testing.T) {
	ch := &recordingPublishChannel{err: amqp.ErrClosed}
	p := &publisher{conf: PublisherConfig{DeliveryMode: amqp.Persistent}, ch: ch, gate: &gate{}, waitsForRecovery: true}
	p.gate.close(amqp.ErrClosed)
	ch.closed.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := p.Publish(ctx, worker.Message{ID: "id"})
	if !errors.Is(err, context.DeadlineExceeded) || ch.publishes.Load() != 0 {
		t.Fatalf("Publish while the connection is down = %v after %d publishes, want the context's error", err, ch.publishes.Load())
	}

	done := make(chan error, 1)
	go func() { done <- p.Publish(context.Background(), worker.Message{ID: "id"}) }()
	time.Sleep(20 * time.Millisecond)
	// The client reopened the channel. Publish reads ch only once the gate opens.
	ch.err, ch.notConfirming = nil, true
	ch.closed.Store(false)
	p.gate.open()
	select {
	case err := <-done:
		if !errors.Is(err, errNotConfirming) || ch.publishes.Load() != 1 {
			t.Fatalf("Publish once the connection is back = %v after %d publishes", err, ch.publishes.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish kept waiting once the connection was back")
	}
}

// The client closes a lost channel before it runs the recovery hooks. A publisher that waits for
// recovery and finds its channel closed while the gate is still open waits for the hooks, and sends
// once the channel is back, while one that does not wait fails at once.
func TestPublisherWaitsForTheHooksOfAClosedChannel(t *testing.T) {
	newChannel := func() *closingPublishChannel {
		return &closingPublishChannel{recordingPublishChannel: recordingPublishChannel{notConfirming: true}}
	}
	ch := newChannel()
	p := &publisher{conf: PublisherConfig{DeliveryMode: amqp.Persistent}, ch: ch, gate: &gate{}, waitsForRecovery: true}
	done := make(chan error, 1)
	go func() { done <- p.Publish(context.Background(), worker.Message{ID: "id"}) }()
	for !ch.failed.Load() {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Publish on a channel closed before the hooks ran = %v, want it to wait", err)
	default:
	}

	// The hooks close the gate, and open it once the client reopened the channel.
	p.gate.close(amqp.ErrClosed)
	time.Sleep(20 * time.Millisecond)
	if n := ch.publishes.Load(); n != 0 {
		t.Fatalf("Publish sent %d messages while the channel was reopened", n)
	}
	p.gate.open()
	select {
	case err := <-done:
		if !errors.Is(err, errNotConfirming) || ch.publishes.Load() != 1 {
			t.Fatalf("Publish once the channel is back = %v after %d publishes", err, ch.publishes.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish kept waiting once the channel was back")
	}

	ch = newChannel()
	p = &publisher{conf: PublisherConfig{DeliveryMode: amqp.Persistent}, ch: ch, gate: &gate{}}
	if err := p.Publish(context.Background(), worker.Message{ID: "id"}); !errors.Is(err, amqp.ErrClosed) || ch.publishes.Load() != 0 {
		t.Fatalf("Publish of a publisher that does not wait = %v after %d publishes, want amqp.ErrClosed at once", err, ch.publishes.Load())
	}
}

// Publish fails at once while the channel is closed, sends nothing while the client reopens it, and
// goes on once it is back.
func TestPublisherWaitsForRecovery(t *testing.T) {
	ch := &recordingPublishChannel{err: amqp.ErrClosed}
	p := &publisher{conf: PublisherConfig{DeliveryMode: amqp.Persistent}, ch: ch, gate: &gate{}}
	p.gate.close(amqp.ErrClosed)

	ch.closed.Store(true)
	if err := p.Publish(context.Background(), worker.Message{ID: "id"}); !errors.Is(err, amqp.ErrClosed) || ch.publishes.Load() != 0 {
		t.Fatalf("Publish while the connection is down = %v after %d publishes, want amqp.ErrClosed at once", err, ch.publishes.Load())
	}

	// The client reopens the channel.
	ch.closed.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := p.Publish(ctx, worker.Message{ID: "id"})
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wait for the recovery of the channel") {
		t.Fatalf("Publish while the channel is reopened = %v, want the context's error", err)
	}
	if ch.publishes.Load() != 0 {
		t.Fatal("Publish sent while the channel was reopened")
	}

	done := make(chan error, 1)
	go func() { done <- p.Publish(context.Background(), worker.Message{ID: "id"}) }()
	time.Sleep(20 * time.Millisecond)
	if ch.publishes.Load() != 0 {
		t.Fatal("Publish sent while the channel was reopened")
	}
	p.gate.open()
	select {
	case err := <-done:
		if !errors.Is(err, amqp.ErrClosed) || ch.publishes.Load() != 1 {
			t.Fatalf("Publish once the channel is back = %v after %d publishes", err, ch.publishes.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish kept waiting once the channel was back")
	}
}
