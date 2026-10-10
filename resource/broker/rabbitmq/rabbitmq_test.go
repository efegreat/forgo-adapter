package rabbitmq

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"

	amqp "github.com/rabbitmq/amqp091-go"
)

// recorder is a declarer that records every declaration as a line.
type recorder struct {
	calls  []string
	failOn string // a call starting with this prefix fails
}

func (r *recorder) record(call string) error {
	r.calls = append(r.calls, call)
	if r.failOn != "" && strings.HasPrefix(call, r.failOn) {
		return errors.New("refused")
	}
	return nil
}

func (r *recorder) ExchangeDeclare(name, kind string, durable, _, _, _ bool, args amqp.Table) error {
	return r.record(fmt.Sprintf("exchange %s %s durable=%v args=%v", name, kind, durable, args))
}

func (r *recorder) QueueDeclare(name string, durable, _, _, _ bool, args amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, r.record(fmt.Sprintf("queue %s durable=%v args=%v", name, durable, args))
}

func (r *recorder) QueueBind(name, key, exchange string, _ bool, _ amqp.Table) error {
	return r.record(fmt.Sprintf("bind %s %s %s", name, exchange, key))
}

func TestConfigSanitize(t *testing.T) {
	queues := []QueueConfig{
		{Name: "orders", DeadLetter: DeadLetterConfig{Enabled: true}},
		{Name: "billing", DeadLetter: DeadLetterConfig{Enabled: true, Exchange: "dead", Queue: "billing.dead", RoutingKey: "b"}},
		{Name: "audit"},
	}
	publishers := []PublisherConfig{{Name: "p"}, {Name: "t", DeliveryMode: amqp.Transient}}

	got := Config{Queues: queues, Publishers: publishers}.Sanitize()

	if got.Addr != "localhost" || got.Port != "5672" || got.VHost != "/" {
		t.Fatalf("connection defaults = %q %q %q", got.Addr, got.Port, got.VHost)
	}
	wantDL := []DeadLetterConfig{
		{Enabled: true, Exchange: "orders.dlx", Queue: "orders.dlq", RoutingKey: "orders"},
		{Enabled: true, Exchange: "dead", Queue: "billing.dead", RoutingKey: "b"},
		{},
	}
	for i, want := range wantDL {
		if !reflect.DeepEqual(got.Queues[i].DeadLetter, want) {
			t.Fatalf("queue %d DeadLetter = %+v, want %+v", i, got.Queues[i].DeadLetter, want)
		}
	}
	if got.Publishers[0].DeliveryMode != amqp.Persistent || got.Publishers[1].DeliveryMode != amqp.Transient {
		t.Fatalf("delivery modes = %d %d", got.Publishers[0].DeliveryMode, got.Publishers[1].DeliveryMode)
	}

	// Configs are shared values: Sanitize must not write through to the caller's slices.
	if queues[0].DeadLetter.Exchange != "" || publishers[0].DeliveryMode != 0 {
		t.Fatal("Sanitize modified the caller's slices")
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		queue   QueueConfig
		wantErr string
	}{
		{name: "disabled", queue: QueueConfig{Args: map[string]any{deadLetterExchangeArg: "x"}}},
		{name: "enabled", queue: QueueConfig{Name: "q", DeadLetter: DeadLetterConfig{Enabled: true}}},
		{
			name:    "unnamed queue",
			queue:   QueueConfig{DeadLetter: DeadLetterConfig{Enabled: true}},
			wantErr: errDeadLetterUnnamedQueue.Error(),
		},
		{
			name:    "exchange in args",
			queue:   QueueConfig{Name: "q", Args: map[string]any{deadLetterExchangeArg: "x"}, DeadLetter: DeadLetterConfig{Enabled: true}},
			wantErr: deadLetterExchangeArg,
		},
		{
			name:    "routing key in args",
			queue:   QueueConfig{Name: "q", Args: map[string]any{deadLetterRoutingKeyArg: "x"}, DeadLetter: DeadLetterConfig{Enabled: true}},
			wantErr: deadLetterRoutingKeyArg,
		},
		{name: "retry", queue: QueueConfig{Name: "q", Retry: RetryConfig{Initial: time.Second, Max: time.Minute}}},
		{name: "retry unnamed queue", queue: QueueConfig{Retry: RetryConfig{Initial: time.Second}}, wantErr: errRetryUnnamedQueue.Error()},
		{name: "retry negative", queue: QueueConfig{Name: "q", Retry: RetryConfig{Max: -1}}, wantErr: "cannot be negative"},
		{name: "retry under a millisecond", queue: QueueConfig{Name: "q", Retry: RetryConfig{Initial: time.Microsecond}}, wantErr: "at least 1ms"},
		{name: "retry max under initial", queue: QueueConfig{Name: "q", Retry: RetryConfig{Initial: time.Minute, Max: time.Second}}, wantErr: "shorter than Retry.Initial"},
		{name: "retry max too long", queue: QueueConfig{Name: "q", Retry: RetryConfig{Initial: time.Second, Max: 50 * 24 * time.Hour}}, wantErr: "cannot exceed"},
		{
			name:    "retry ttl in args",
			queue:   QueueConfig{Name: "q", Retry: RetryConfig{Initial: time.Second, Args: map[string]any{messageTTLArg: 5}}},
			wantErr: messageTTLArg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Config{Queues: []QueueConfig{tt.queue}}.Sanitize().validate()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("validate: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("validate err = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestConfigNewFailsBeforeConnecting(t *testing.T) {
	// Port 1 refuses connections; a validation error must come first.
	_, err := Config{Port: "1", Queues: []QueueConfig{{DeadLetter: DeadLetterConfig{Enabled: true}}}}.New()
	if !errors.Is(err, errDeadLetterUnnamedQueue) {
		t.Fatalf("New err = %v, want errDeadLetterUnnamedQueue", err)
	}
}

func TestDeclare(t *testing.T) {
	userArgs := map[string]any{"x-queue-type": "quorum"}
	tests := []struct {
		name  string
		queue QueueConfig
		want  []string
	}{
		{
			name: "without dead letter",
			queue: QueueConfig{
				Name: "orders", Durable: true, Args: userArgs,
				Bindings: []BindingConfig{{Exchange: "shop", RoutingKey: "order.*"}},
			},
			want: []string{
				"exchange shop topic durable=true args=map[]",
				"queue orders durable=true args=map[x-queue-type:quorum]",
				"bind orders shop order.*",
			},
		},
		{
			name: "with default dead letter",
			queue: QueueConfig{
				Name: "orders", Durable: true, Args: userArgs,
				Bindings:   []BindingConfig{{Exchange: "shop", RoutingKey: "order.*"}},
				DeadLetter: DeadLetterConfig{Enabled: true},
			},
			want: []string{
				"exchange shop topic durable=true args=map[]",
				"exchange orders.dlx direct durable=true args=map[]",
				"queue orders.dlq durable=true args=map[]",
				"bind orders.dlq orders.dlx orders",
				"queue orders durable=true args=map[x-dead-letter-exchange:orders.dlx x-dead-letter-routing-key:orders x-queue-type:quorum]",
				"bind orders shop order.*",
			},
		},
		{
			name: "with custom dead letter",
			queue: QueueConfig{
				Name: "orders",
				DeadLetter: DeadLetterConfig{
					Enabled: true, Exchange: "dead", Queue: "orders.dead", RoutingKey: "o",
					Args: map[string]any{"x-message-ttl": 86400000},
				},
			},
			want: []string{
				"exchange shop topic durable=true args=map[]",
				"exchange dead direct durable=true args=map[]",
				"queue orders.dead durable=true args=map[x-message-ttl:86400000]",
				"bind orders.dead dead o",
				"queue orders durable=false args=map[x-dead-letter-exchange:dead x-dead-letter-routing-key:o]",
			},
		},
		{
			name: "with retry",
			queue: QueueConfig{
				Name: "orders", Durable: true,
				Bindings: []BindingConfig{{Exchange: "shop", RoutingKey: "order.*"}},
				Retry:    RetryConfig{Initial: time.Second, Max: 3 * time.Second, Args: userArgs},
			},
			want: []string{
				"exchange shop topic durable=true args=map[]",
				"queue orders durable=true args=map[]",
				"bind orders shop order.*",
				"queue orders.retry.1s durable=true args=map[x-dead-letter-exchange: x-dead-letter-routing-key:orders x-message-ttl:1000 x-queue-type:quorum]",
				"queue orders.retry.2s durable=true args=map[x-dead-letter-exchange: x-dead-letter-routing-key:orders x-message-ttl:2000 x-queue-type:quorum]",
				"queue orders.retry.3s durable=true args=map[x-dead-letter-exchange: x-dead-letter-routing-key:orders x-message-ttl:3000 x-queue-type:quorum]",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Exchanges: []ExchangeConfig{{Name: "shop", Kind: amqp.ExchangeTopic, Durable: true}},
				Queues:    []QueueConfig{tt.queue},
			}.Sanitize()

			var r recorder
			if err := declare(&r, cfg); err != nil {
				t.Fatalf("declare: %v", err)
			}
			if !reflect.DeepEqual(r.calls, tt.want) {
				t.Fatalf("declarations:\n%s\nwant:\n%s", strings.Join(r.calls, "\n"), strings.Join(tt.want, "\n"))
			}
		})
	}

	if len(userArgs) != 1 {
		t.Fatalf("declare modified the queue's Args: %v", userArgs)
	}
}

func TestDeclareError(t *testing.T) {
	cfg := Config{Queues: []QueueConfig{{Name: "orders", DeadLetter: DeadLetterConfig{Enabled: true}}}}.Sanitize()
	tests := []struct {
		failOn  string
		wantErr string
	}{
		{failOn: "exchange orders.dlx", wantErr: "declare dead-letter exchange orders.dlx"},
		{failOn: "queue orders.dlq", wantErr: "declare dead-letter queue orders.dlq"},
		{failOn: "bind orders.dlq", wantErr: "bind dead-letter queue orders.dlq"},
		{failOn: "queue orders ", wantErr: "declare queue orders"},
	}

	for _, tt := range tests {
		t.Run(tt.failOn, func(t *testing.T) {
			r := recorder{failOn: tt.failOn}
			err := declare(&r, cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("declare err = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestPublishingRoundTrip(t *testing.T) {
	created := time.Unix(1_700_000_000, 123)
	msg := worker.Message{
		ID:        "0199-id",
		Name:      "order.placed",
		Payload:   []byte(`{"id":1}`),
		Headers:   types.NewJSONMapWithData(map[string]string{"tenant": "acme", worker.MessageIDHeader: "stale"}),
		CreatedAt: created,
		Key:       "order.placed",
	}

	p := publishing(msg, amqp.Persistent)

	wantHeaders := amqp.Table{
		"tenant":                      "acme",
		worker.MessageIDHeader:        "0199-id",
		worker.MessageNameHeader:      "order.placed",
		worker.MessageTimestampHeader: strconv.FormatInt(created.UnixNano(), 10),
	}
	if !reflect.DeepEqual(p.Headers, wantHeaders) {
		t.Fatalf("headers = %v, want %v", p.Headers, wantHeaders)
	}
	if p.MessageId != msg.ID || p.Type != msg.Name || p.DeliveryMode != amqp.Persistent || string(p.Body) != `{"id":1}` {
		t.Fatalf("publishing = %+v", p)
	}

	got := NewDelivery(amqp.Delivery{
		Headers:    p.Headers,
		MessageId:  p.MessageId,
		Type:       p.Type,
		Body:       p.Body,
		RoutingKey: "order.placed",
		Exchange:   "shop",
	}).Message()
	if got.ID != msg.ID || got.Name != msg.Name || !got.CreatedAt.Equal(created) || got.Key != "order.placed" || got.Destination != "shop" {
		t.Fatalf("consumed message = %+v, want the published identity", got)
	}
	if v, _ := got.Headers.Get("tenant"); v != "acme" {
		t.Fatalf("tenant header = %q", v)
	}
}

func TestPublishingWithoutHeadersOrTime(t *testing.T) {
	p := publishing(worker.Message{ID: "id", Name: "n"}, amqp.Transient)
	if _, ok := p.Headers[worker.MessageTimestampHeader]; ok {
		t.Fatal("timestamp header written for a zero CreatedAt")
	}
	if p.Headers[worker.MessageIDHeader] != "id" || p.Headers[worker.MessageNameHeader] != "n" {
		t.Fatalf("headers = %v", p.Headers)
	}
}

func TestNewDeliveryFallsBackToProperties(t *testing.T) {
	got := NewDelivery(amqp.Delivery{
		MessageId: "external-id",
		Type:      "invoice.paid",
		Headers:   amqp.Table{"retries": int32(2), "source": "erp"},
	}).Message()

	if got.ID != "external-id" || got.Name != "invoice.paid" {
		t.Fatalf("message = %+v, want ID and name from the AMQP properties", got)
	}
	if v, _ := got.Headers.Get("retries"); v != "2" {
		t.Fatalf("retries header = %q, want the integer as text", v)
	}
	if v, _ := got.Headers.Get("source"); v != "erp" {
		t.Fatalf("source header = %q", v)
	}
}

func TestNewDeliveryConvertsHeaderValues(t *testing.T) {
	created := time.Unix(1_700_000_000, 5)
	got := NewDelivery(amqp.Delivery{
		MessageId: "property-id",
		Headers: amqp.Table{
			worker.MessageIDHeader:        int64(42),
			worker.MessageTimestampHeader: created.UnixNano(),
			"x-death": []any{amqp.Table{
				"count":        int64(2),
				"queue":        "orders.retry.1s",
				"reason":       "expired",
				"routing-keys": []any{"orders"},
				"time":         time.Unix(1_700_000_000, 0),
			}},
		},
	}).Message()

	if got.ID != "42" || !got.CreatedAt.Equal(created) {
		t.Fatalf("message = %+v, want the identity from the converted headers", got)
	}
	want := `[{"count":2,"queue":"orders.retry.1s","reason":"expired","routing-keys":["orders"],"time":"2023-11-14T22:13:20Z"}]`
	if v, _ := got.Headers.Get("x-death"); v != want {
		t.Fatalf("x-death header = %s, want %s", v, want)
	}
}

func TestHeaderValue(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"string", "erp", "erp"},
		{"byte array", []byte("raw"), "raw"},
		{"void", nil, ""},
		{"bool", true, "true"},
		{"int8", int8(-8), "-8"},
		{"int16", int16(-16), "-16"},
		{"int32", int32(-32), "-32"},
		{"int64", int64(-64), "-64"},
		{"uint8", uint8(8), "8"},
		{"uint16", uint16(16), "16"},
		{"uint32", uint32(32), "32"},
		{"float32", float32(0.1), "0.1"},
		{"float64", 2.5, "2.5"},
		{"large float", 1e21, "1e+21"},
		{"NaN", math.NaN(), "NaN"},
		{"decimal", amqp.Decimal{Scale: 2, Value: 12345}, "123.45"},
		{"small decimal", amqp.Decimal{Scale: 3, Value: 5}, "0.005"},
		{"negative decimal", amqp.Decimal{Scale: 1, Value: -5}, "-0.5"},
		{"negative decimal with an integer part", amqp.Decimal{Scale: 2, Value: -12345}, "-123.45"},
		{"zero decimal", amqp.Decimal{Scale: 2}, "0.00"},
		{"unscaled decimal", amqp.Decimal{Value: 7}, "7"},
		{"timestamp", time.Date(2026, 10, 10, 13, 0, 0, 0, time.FixedZone("TRT", 3*60*60)), "2026-10-10T10:00:00Z"},
		{"empty table", amqp.Table{}, "{}"},
		{"empty array", []any{}, "[]"},
		{
			"table",
			amqp.Table{
				"b": true, "n": nil, "i": int32(1), "u": uint8(2), "go int": 3, "f": float32(0.1),
				"d": amqp.Decimal{Scale: 1, Value: 15}, "s": `say "hi"`, "x": []byte("raw"),
				"inf": math.Inf(-1), "t": time.Unix(0, 0), "nested": amqp.Table{"a": []any{int64(1), "two"}},
			},
			`{"b":true,"d":1.5,"f":0.1,"go int":3,"i":1,"inf":"-Inf","n":null,"nested":{"a":[1,"two"]},"s":"say \"hi\"","t":"1970-01-01T00:00:00Z","u":2,"x":"raw"}`,
		},
		{"unknown type", uint64(64), "64"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := headerValue(tt.value); got != tt.want {
				t.Fatalf("headerValue(%#v) = %s, want %s", tt.value, got, tt.want)
			}
		})
	}
}
