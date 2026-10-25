package rabbitmq

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeConfirmedPublisher records the publishes of a retrier.
type fakeConfirmedPublisher struct {
	exchange, key, id string
	msg               amqp.Publishing
	calls             int
	err               error
}

func (p *fakeConfirmedPublisher) publish(_ context.Context, exchange, key string, msg amqp.Publishing, id string) error {
	p.calls++
	p.exchange, p.key, p.msg, p.id = exchange, key, msg, id
	return p.err
}

func (p *fakeConfirmedPublisher) Close() error { return nil }

func TestRetryQueues(t *testing.T) {
	tests := []struct {
		name   string
		retry  RetryConfig
		delays []time.Duration
	}{
		{name: "disabled"},
		{name: "doubles up to max", retry: RetryConfig{Initial: time.Second, Max: 10 * time.Second}, delays: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}},
		{name: "max on a doubling", retry: RetryConfig{Initial: time.Second, Max: 4 * time.Second}, delays: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}},
		{name: "max equals initial", retry: RetryConfig{Initial: time.Minute, Max: time.Minute}, delays: []time.Duration{time.Minute}},
		{name: "default max", retry: RetryConfig{Initial: 2 * time.Minute}, delays: []time.Duration{2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute}},
		{name: "default max under initial", retry: RetryConfig{Initial: time.Hour}, delays: []time.Duration{time.Hour}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var delays []time.Duration
			for _, q := range tt.retry.sanitize().queues("orders") {
				delays = append(delays, q.delay)
				if want := "orders.retry." + q.delay.String(); q.name != want {
					t.Fatalf("queue %q, want %q", q.name, want)
				}
				if q.args[messageTTLArg] != q.delay.Milliseconds() || q.args[deadLetterExchangeArg] != "" || q.args[deadLetterRoutingKeyArg] != "orders" {
					t.Fatalf("queue %s args %v", q.name, q.args)
				}
			}
			if !reflect.DeepEqual(delays, tt.delays) {
				t.Fatalf("delays %v, want %v", delays, tt.delays)
			}
		})
	}

	// Retry queues are at-least-once quorum queues unless Args say otherwise.
	argTests := []struct {
		name string
		args map[string]any
		want amqp.Table
	}{
		{name: "default", want: amqp.Table{queueTypeArg: "quorum", deadLetterStrategyArg: "at-least-once", overflowArg: "reject-publish"}},
		{name: "classic", args: map[string]any{queueTypeArg: "classic"}, want: amqp.Table{queueTypeArg: "classic"}},
		{
			name: "quorum with an overflow of its own",
			args: map[string]any{overflowArg: "drop-head", "x-delivery-limit": 5},
			want: amqp.Table{queueTypeArg: "quorum", deadLetterStrategyArg: "at-least-once", overflowArg: "drop-head", "x-delivery-limit": 5},
		},
	}
	for _, tt := range argTests {
		t.Run("args "+tt.name, func(t *testing.T) {
			given := maps.Clone(tt.args)
			retry := RetryConfig{Initial: time.Second, Max: time.Second, Args: tt.args}
			args := retry.queues("orders")[0].args
			for _, arg := range []string{messageTTLArg, deadLetterExchangeArg, deadLetterRoutingKeyArg} {
				delete(args, arg)
			}
			if !reflect.DeepEqual(args, tt.want) {
				t.Fatalf("args %v, want %v", args, tt.want)
			}
			// Configs are shared values: Args itself is never modified.
			if !reflect.DeepEqual(tt.args, given) {
				t.Fatalf("Args became %v", tt.args)
			}
		})
	}

	cfg := Config{Queues: []QueueConfig{{Name: "plain"}, {Name: "orders", Retry: RetryConfig{Initial: time.Second}}}}.Sanitize()
	if q, ok := queueConfig(cfg, "orders"); !ok || len(q.Retry.queues(q.Name)) == 0 {
		t.Fatal("queueConfig lost the queue's Retry")
	}
	if q, ok := queueConfig(cfg, "plain"); !ok || q.Retry.queues(q.Name) != nil {
		t.Fatal("a queue without Retry has retry queues")
	}
	if _, ok := queueConfig(cfg, "undeclared"); ok {
		t.Fatal("queueConfig found a queue that is not configured")
	}
}

func TestDeliveryRetry(t *testing.T) {
	queues := RetryConfig{Initial: time.Second, Max: 4 * time.Second}.queues("orders")
	errDown := errors.New("connection lost")
	tests := []struct {
		name       string
		attempt    int
		publishErr error
		stale      bool
		wantQueue  string // empty: nothing published
		wantAcks   []string
		wantErr    error
	}{
		{name: "before the attempt was counted", attempt: 0, wantQueue: "orders.retry.1s", wantAcks: []string{"ack 7 multiple=false"}},
		{name: "first attempt", attempt: 1, wantQueue: "orders.retry.1s", wantAcks: []string{"ack 7 multiple=false"}},
		{name: "second attempt", attempt: 2, wantQueue: "orders.retry.2s", wantAcks: []string{"ack 7 multiple=false"}},
		{name: "beyond the last delay", attempt: 9, wantQueue: "orders.retry.4s", wantAcks: []string{"ack 7 multiple=false"}},
		{
			name: "publish failed", attempt: 1, publishErr: errDown, wantQueue: "orders.retry.1s",
			wantAcks: []string{"nack 7 multiple=false requeue=true"}, wantErr: errDown,
		},
		{name: "stale", attempt: 1, stale: true, wantErr: errStaleDelivery},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack := &fakeAcknowledger{}
			guard := &ackGuard{}
			pub := &fakeConfirmedPublisher{err: tt.publishErr}
			d := newDelivery(amqp.Delivery{
				Acknowledger: ack, DeliveryTag: 7, ContentType: "application/json", Body: []byte(`{}`),
				Expiration: "60000", UserId: "producer",
				Headers: amqp.Table{"tenant": "a"}, // no x-msg-id: the delivery generates an ID
			}, guard, 0, &retrier{pub: pub, queues: queues}, nil)
			if tt.stale {
				guard.invalidate()
			}

			err := d.Retry(context.Background(), tt.attempt)
			if (tt.wantErr == nil && err != nil) || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) {
				t.Fatalf("Retry = %v, want %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(ack.calls, tt.wantAcks) {
				t.Fatalf("acknowledgements %q, want %q", ack.calls, tt.wantAcks)
			}
			if tt.wantQueue == "" {
				if pub.calls != 0 {
					t.Fatalf("published to %s", pub.key)
				}
				return
			}

			// The copy goes straight to the retry queue and keeps the delivery's identity.
			m := pub.msg
			if pub.exchange != "" || pub.key != tt.wantQueue || pub.id != d.msg.ID {
				t.Fatalf("published to exchange %q key %q as %q", pub.exchange, pub.key, pub.id)
			}
			if m.MessageId != d.msg.ID || m.Headers[worker.MessageIDHeader] != d.msg.ID || m.Headers["tenant"] != "a" {
				t.Fatalf("copy lost its identity: id %q, headers %v", m.MessageId, m.Headers)
			}
			if m.DeliveryMode != amqp.Persistent || m.Expiration != "" || m.UserId != "" || string(m.Body) != "{}" || m.ContentType != "application/json" {
				t.Fatalf("copy %+v", m)
			}
		})
	}
}
