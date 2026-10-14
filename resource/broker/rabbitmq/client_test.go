package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeConsumeChannel is a consumeChannel whose deliveries the test sends.
type fakeConsumeChannel struct {
	prefetch   int
	qosErr     error
	consumeErr error
	queue      string
	tag        string
	msgs       chan amqp.Delivery
}

func (c *fakeConsumeChannel) Qos(prefetchCount, _ int, _ bool) error {
	c.prefetch = prefetchCount
	return c.qosErr
}

func (c *fakeConsumeChannel) Consume(queue, tag string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	c.queue, c.tag = queue, tag
	return c.msgs, c.consumeErr
}

func (c *fakeConsumeChannel) Close() error { return nil }

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
	ch := &fakeConsumeChannel{msgs: make(chan amqp.Delivery, 3)}
	guard := &ackGuard{}
	s := &subscriber{conf: SubscriberConfig{Name: "billing", Queue: "billing"}, ch: ch, guard: guard, lost: newConsumerLoss()}

	ack := &fakeAcknowledger{}
	var (
		mu  sync.Mutex
		ids []string
	)
	handled := make(chan struct{}, 3)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(ctx, worker.ConsumerState{
			Concurrency: 4,
			Handler: func(ctx context.Context, d worker.IDelivery) {
				mu.Lock()
				ids = append(ids, d.Message().ID)
				mu.Unlock()
				_ = d.Complete(ctx)
				handled <- struct{}{}
			},
		})
	}()

	for _, id := range []string{"a", "b", "c"} {
		ch.msgs <- amqp.Delivery{MessageId: id, Acknowledger: ack, DeliveryTag: 1}
	}
	for range 3 {
		<-handled
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Consume = %v", err)
	}

	if ch.prefetch != 4 || ch.queue != "billing" {
		t.Fatalf("prefetch %d on queue %q, want 4 on billing", ch.prefetch, ch.queue)
	}
	if len(ids) != 3 || len(ack.calls) != 3 {
		t.Fatalf("handled %v with acknowledgements %v", ids, ack.calls)
	}
}

func TestSubscriberConsumeErrors(t *testing.T) {
	closed := make(chan amqp.Delivery)
	close(closed)

	tests := []struct {
		name    string
		ch      *fakeConsumeChannel
		wantErr error
		wantMsg string
	}{
		{name: "deliveries closed", ch: &fakeConsumeChannel{msgs: closed}, wantErr: errDeliveriesClosed},
		{name: "prefetch refused", ch: &fakeConsumeChannel{qosErr: errors.New("refused")}, wantMsg: "subscriber s: set prefetch: refused"},
		{name: "consume refused", ch: &fakeConsumeChannel{consumeErr: errors.New("exclusive")}, wantMsg: "subscriber s: consume queue q: exclusive"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &subscriber{conf: SubscriberConfig{Name: "s", Queue: "q"}, ch: tt.ch, guard: &ackGuard{}, lost: newConsumerLoss()}
			err := s.Consume(context.Background(), worker.ConsumerState{Concurrency: 1, Handler: func(context.Context, worker.IDelivery) {}})
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("Consume = %v, want %v", err, tt.wantErr)
			}
			if tt.wantMsg != "" && (err == nil || !strings.Contains(err.Error(), tt.wantMsg)) {
				t.Fatalf("Consume = %v, want one containing %q", err, tt.wantMsg)
			}
		})
	}
}

func TestSubscriberDropsAcksAcrossRecovery(t *testing.T) {
	ch := &fakeConsumeChannel{msgs: make(chan amqp.Delivery, 1)}
	guard := &ackGuard{}
	s := &subscriber{conf: SubscriberConfig{Name: "s", Queue: "q"}, ch: ch, guard: guard, lost: newConsumerLoss()}

	ack := &fakeAcknowledger{}
	received := make(chan worker.IDelivery, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(ctx, worker.ConsumerState{Concurrency: 1, Handler: func(_ context.Context, d worker.IDelivery) {
			received <- d
		}})
	}()

	ch.msgs <- amqp.Delivery{MessageId: "old", Acknowledger: ack, DeliveryTag: 3}
	d := <-received

	// The channel is lost and recovered while the handler still runs.
	guard.invalidate()

	if err := d.Complete(context.Background()); !errors.Is(err, errStaleDelivery) {
		t.Fatalf("Complete after recovery = %v, want errStaleDelivery", err)
	}
	if len(ack.calls) != 0 {
		t.Fatalf("the late acknowledgement reached the recovered channel: %v", ack.calls)
	}
	cancel()
	<-done
}

func TestSubscriberStopsWhenConsumerLost(t *testing.T) {
	// The delivery channel stays open: the recovery reopened the channel without the consumer.
	ch := &fakeConsumeChannel{msgs: make(chan amqp.Delivery)}
	lost := newConsumerLoss()
	s := &subscriber{conf: SubscriberConfig{Name: "s", Queue: "q"}, ch: ch, tag: "forgo-s-1", guard: &ackGuard{}, lost: lost}

	done := make(chan error, 1)
	go func() {
		done <- s.Consume(context.Background(), worker.ConsumerState{Concurrency: 1, Handler: func(context.Context, worker.IDelivery) {}})
	}()

	refused := &amqp.Error{Code: amqp.AccessRefused, Reason: "queue 'q' in exclusive use"}
	lost.report(amqp.TopologyRecoveryEntity{EntityType: amqp.TopologyEntityConsumer, EntityName: "forgo-s-1", Err: refused})

	select {
	case err := <-done:
		if !errors.Is(err, errConsumerLost) || !errors.Is(err, refused) {
			t.Fatalf("Consume = %v, want errConsumerLost wrapping the broker's error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Consume kept waiting on a consumer the recovery could not re-subscribe")
	}
	if ch.tag != "forgo-s-1" {
		t.Fatalf("consumed under tag %q, want the tag the recovery reports", ch.tag)
	}
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
	tests := []struct {
		name    string
		conf    settledConfirmation
		returns []amqp.Return
		closed  bool
		wantErr error
		wantMsg string
	}{
		{name: "confirmed", conf: settledConfirmation{ack: true}},
		{name: "nacked", conf: settledConfirmation{ack: false}, wantErr: errNacked},
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
			p := &publisher{returns: returns}

			err := p.await(context.Background(), tt.conf, "id")
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
	if err := p.await(context.Background(), settledConfirmation{ack: true}, "id"); err != nil {
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
}

func (c *recordingPublishChannel) PublishWithDeferredConfirmWithContext(_ context.Context, exchange, key string, mandatory, _ bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	c.exchange, c.key, c.mandatory, c.msg = exchange, key, mandatory, msg
	if c.err != nil || c.notConfirming {
		return nil, c.err
	}
	return new(amqp.DeferredConfirmation), nil
}

func (c *recordingPublishChannel) Close() error { return nil }

func TestPublisherPublish(t *testing.T) {
	ch := &recordingPublishChannel{}
	p := &publisher{conf: PublisherConfig{Mandatory: true, DeliveryMode: amqp.Persistent}, ch: ch}

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
