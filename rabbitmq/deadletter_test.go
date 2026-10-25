package rabbitmq

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// routedPublisher records every publish, as "exchange/key", and fails those routed to an exchange of
// fail: "" fails the copies to the retry queues.
type routedPublisher struct {
	fail  map[string]error
	calls []string
	msgs  []amqp.Publishing
}

func (p *routedPublisher) publish(_ context.Context, exchange, key string, msg amqp.Publishing, _ string) error {
	p.calls = append(p.calls, exchange+"/"+key)
	p.msgs = append(p.msgs, msg)
	return p.fail[exchange]
}

func (p *routedPublisher) Close() error { return nil }

func TestDeliveryReject(t *testing.T) {
	errDown := errors.New("connection lost")
	tests := []struct {
		name       string
		deadLetter bool
		retry      bool
		fail       map[string]error
		stale      bool
		wantCalls  []string
		wantAcks   []string
		wantErr    error
		wantMsg    string
	}{
		{name: "without a dead letter", wantAcks: []string{"reject 7 requeue=false"}},
		{name: "dead-lettered", deadLetter: true, wantCalls: []string{"orders.dlx/orders"}, wantAcks: []string{"ack 7 multiple=false"}},
		{
			name: "copy failed without retry queues", deadLetter: true, fail: map[string]error{"orders.dlx": errUnroutable},
			wantCalls: []string{"orders.dlx/orders"}, wantAcks: []string{"nack 7 multiple=false requeue=true"},
			wantErr: errUnroutable, wantMsg: "dead-letter id to orders.dlx: rabbitmq: mandatory message is unroutable; requeuing it",
		},
		{
			name: "copy failed, parked in the last retry queue", deadLetter: true, retry: true, fail: map[string]error{"orders.dlx": errNacked},
			wantCalls: []string{"orders.dlx/orders", "/orders.retry.4s"}, wantAcks: []string{"ack 7 multiple=false"},
			wantErr: errNacked, wantMsg: "parked in orders.retry.4s until it is rejected again",
		},
		{
			name: "copy and parking failed", deadLetter: true, retry: true, fail: map[string]error{"orders.dlx": errNacked, "": errDown},
			wantCalls: []string{"orders.dlx/orders", "/orders.retry.4s"}, wantAcks: []string{"nack 7 multiple=false requeue=true"},
			wantErr: errDown, wantMsg: "parking it in orders.retry.4s: connection lost; requeuing it",
		},
		{name: "stale", deadLetter: true, stale: true, wantErr: errStaleDelivery},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack := &fakeAcknowledger{}
			guard := &ackGuard{}
			pub := &routedPublisher{fail: tt.fail}
			var dl *deadLetter
			if tt.deadLetter {
				dl = &deadLetter{pub: pub, exchange: "orders.dlx", key: "orders"}
				if tt.retry {
					dl.retry = &retrier{pub: pub, queues: RetryConfig{Initial: time.Second, Max: 4 * time.Second}.queues("orders")}
				}
			}
			d := newDelivery(amqp.Delivery{
				Acknowledger: ack, DeliveryTag: 7, Exchange: "shop", RoutingKey: "order.placed",
				ContentType: "application/json", Body: []byte(`{}`), Expiration: "60000", UserId: "producer",
				Headers: amqp.Table{"tenant": "a"}, MessageId: "id",
			}, guard, 0, nil, dl)
			if tt.stale {
				guard.invalidate()
			}

			err := d.Reject(context.Background())
			if (tt.wantErr == nil && err != nil) || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) {
				t.Fatalf("Reject = %v, want %v", err, tt.wantErr)
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Reject = %v, want one containing %q", err, tt.wantMsg)
			}
			if !reflect.DeepEqual(ack.calls, tt.wantAcks) {
				t.Fatalf("acknowledgements %q, want %q", ack.calls, tt.wantAcks)
			}
			if !reflect.DeepEqual(pub.calls, tt.wantCalls) {
				t.Fatalf("published %q, want %q", pub.calls, tt.wantCalls)
			}

			// Every copy keeps the delivery's identity.
			for _, m := range pub.msgs {
				if m.MessageId != d.msg.ID || m.Headers[worker.MessageIDHeader] != d.msg.ID || m.Headers["tenant"] != "a" {
					t.Fatalf("copy lost its identity: id %q, headers %v", m.MessageId, m.Headers)
				}
				if m.DeliveryMode != amqp.Persistent || m.Expiration != "" || m.UserId != "" || string(m.Body) != "{}" || m.ContentType != "application/json" {
					t.Fatalf("copy %+v", m)
				}
			}
		})
	}
}
