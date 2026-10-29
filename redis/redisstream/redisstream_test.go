package redisstream

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo-adapter/redis/redisdb"
	"github.com/efegreat/forgo/safemap"
	"github.com/efegreat/forgo/server/worker"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var (
	_ worker.IBroker     = (*broker)(nil)
	_ worker.IPublisher  = (*publisher)(nil)
	_ worker.ISubscriber = (*subscriber)(nil)
	_ worker.IDelivery   = (*delivery)(nil)
)

// t0 is the clock of the miniredis servers of the tests that control idle times.
var t0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

// fake starts a miniredis server and returns a client to inspect it and a config that connects to
// it.
func fake(t *testing.T) (*miniredis.Miniredis, *redis.Client, Config) {
	t.Helper()
	m := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: m.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = cl.Close() })
	return m, cl, Config{Name: "events", Redis: redisdb.Config{Addrs: []string{m.Addr()}}}
}

// open builds the broker of cfg and closes it when the test ends.
func open(t *testing.T, cfg Config) *broker {
	t.Helper()
	res, err := cfg.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Close() })
	return res.(*broker)
}

// topology is a stream orders with the group billing, read by the subscriber billing.
func topology(cfg Config, sub SubscriberConfig) Config {
	sub.Name, sub.Stream = "billing", "orders"
	cfg.Streams = []StreamConfig{{Name: "orders", Groups: []GroupConfig{{Name: "billing"}}}}
	cfg.Subscribers = []SubscriberConfig{sub}
	return cfg
}

// openSubscriber returns the subscriber name of b, timed for tests.
func openSubscriber(t *testing.T, b *broker, name string) *subscriber {
	t.Helper()
	s, err := b.NewSubscriber(name, worker.SubscriberOptions{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	sub := s.(*subscriber)
	sub.block, sub.claimEvery, sub.beat, sub.errorWait, sub.sweepEvery = 20*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond, 10*time.Millisecond
	return sub
}

// errConsumeHangs is returned by the stop function of consume when Consume does not return.
var errConsumeHangs = errors.New("Consume did not return")

// consume consumes s in the background, with at most concurrency entries in flight. stop ends it
// and returns the error of Consume. done is closed when Consume returns, also on its own.
func consume(t *testing.T, s *subscriber, concurrency int, handle func(context.Context, worker.IDelivery)) (stop func() error, done <-chan struct{}) {
	t.Helper()
	s.concurrency = concurrency
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	var err error
	go func() {
		defer close(returned)
		err = s.Consume(ctx, handle)
	}()
	stop = func() error {
		cancel()
		select {
		case <-returned:
			return err
		case <-time.After(5 * time.Second):
			return errConsumeHangs
		}
	}
	t.Cleanup(func() {
		if err := stop(); errors.Is(err, errConsumeHangs) {
			t.Error(err)
		}
	})
	return stop, returned
}

// deliveries is a handler that sends every delivery to a channel and leaves it unsettled.
func deliveries() (chan worker.IDelivery, func(context.Context, worker.IDelivery)) {
	ch := make(chan worker.IDelivery, 16)
	return ch, func(_ context.Context, d worker.IDelivery) { ch <- d }
}

func next(t *testing.T, ch <-chan worker.IDelivery) worker.IDelivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery")
		return nil
	}
}

// none fails if ch receives a delivery within a short wait.
func none(t *testing.T, ch <-chan worker.IDelivery) {
	t.Helper()
	select {
	case d := <-ch:
		t.Fatalf("unexpected delivery of %s", d.Message().ID)
	case <-time.After(150 * time.Millisecond):
	}
}

// eventually fails unless cond holds within a few seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func publish(t *testing.T, b *broker, msg worker.Message) {
	t.Helper()
	p, err := b.NewPublisher("any")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
}

// pending returns the pending entries of the group billing of orders.
func pending(t *testing.T, cl *redis.Client) []redis.XPendingExt {
	t.Helper()
	p, err := cl.XPendingExt(context.Background(), &redis.XPendingExtArgs{Stream: "orders", Group: "billing", Start: "-", End: "+", Count: 100}).Result()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// consumers returns the pending entries of every consumer of the group billing of orders.
func consumers(t *testing.T, cl *redis.Client) map[string]int64 {
	t.Helper()
	info, err := cl.XInfoConsumers(context.Background(), "orders", "billing").Result()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]int64, len(info))
	for _, c := range info {
		got[c.Name] = c.Pending
	}
	return got
}

func TestConfigSanitize(t *testing.T) {
	streams := []StreamConfig{{Name: "orders", Groups: []GroupConfig{{Name: "billing"}, {Name: "audit", StartID: "0"}}}}
	subscribers := []SubscriberConfig{
		{Name: "billing", Stream: "orders"},
		{
			Name:       "audit",
			Stream:     "orders",
			Group:      "auditors",
			ClaimIdle:  5 * time.Minute,
			Retry:      RetryConfig{Initial: time.Second},
			DeadLetter: DeadLetterConfig{Enabled: true},
		},
		{Name: "named", Stream: "orders", Retry: RetryConfig{Initial: time.Second, Max: 10 * time.Second}, DeadLetter: DeadLetterConfig{Enabled: true, Stream: "dead"}},
	}
	got := Config{Streams: streams, Subscribers: subscribers}.Sanitize()

	wantGroups := []GroupConfig{{Name: "billing", StartID: "$"}, {Name: "audit", StartID: "0"}}
	if !reflect.DeepEqual(got.Streams[0].Groups, wantGroups) {
		t.Fatalf("Groups = %+v, want %+v", got.Streams[0].Groups, wantGroups)
	}
	wantSubscribers := []SubscriberConfig{
		{Name: "billing", Stream: "orders", Group: "billing", ClaimIdle: time.Minute},
		{
			Name:       "audit",
			Stream:     "orders",
			Group:      "auditors",
			ClaimIdle:  5 * time.Minute,
			Retry:      RetryConfig{Initial: time.Second, Max: 5 * time.Minute},
			DeadLetter: DeadLetterConfig{Enabled: true, Stream: "orders:auditors:dlq"},
		},
		{Name: "named", Stream: "orders", Group: "named", ClaimIdle: time.Minute, Retry: RetryConfig{Initial: time.Second, Max: 10 * time.Second}, DeadLetter: DeadLetterConfig{Enabled: true, Stream: "dead"}},
	}
	if !reflect.DeepEqual(got.Subscribers, wantSubscribers) {
		t.Fatalf("Subscribers = %+v, want %+v", got.Subscribers, wantSubscribers)
	}

	// Configs are shared values: Sanitize must not write into the caller's slices.
	if streams[0].Groups[0].StartID != "" || subscribers[0].Group != "" || subscribers[1].DeadLetter.Stream != "" {
		t.Fatal("Sanitize modified the caller's slices")
	}
}

func TestConfigValidate(t *testing.T) {
	sub := func(s SubscriberConfig) Config {
		s.Name, s.Stream = "billing", "orders"
		return Config{Subscribers: []SubscriberConfig{s}}
	}
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"stream without name", Config{Streams: []StreamConfig{{}}}, "a stream needs a Name"},
		{"stream twice", Config{Streams: []StreamConfig{{Name: "orders"}, {Name: "orders"}}}, `stream "orders" is listed twice`},
		{"negative MaxLen", Config{Streams: []StreamConfig{{Name: "orders", MaxLen: -1}}}, "MaxLen cannot be negative"},
		{"group without name", Config{Streams: []StreamConfig{{Name: "orders", Groups: []GroupConfig{{}}}}}, "a group needs a Name"},
		{"subscriber without stream", Config{Subscribers: []SubscriberConfig{{Name: "billing"}}}, `subscriber "billing" needs a Stream`},
		{"subscriber without group", Config{Subscribers: []SubscriberConfig{{Stream: "orders"}}}, `subscriber "" needs a Group`},
		{"short ClaimIdle", sub(SubscriberConfig{ClaimIdle: 500 * time.Millisecond}), "ClaimIdle must be at least 1s"},
		{"negative ClaimIdle", sub(SubscriberConfig{ClaimIdle: -time.Second}), "ClaimIdle must be at least 1s"},
		{"negative retry", sub(SubscriberConfig{Retry: RetryConfig{Max: -time.Second}}), "cannot be negative"},
		{"short retry", sub(SubscriberConfig{Retry: RetryConfig{Initial: time.Microsecond}}), "Retry.Initial must be at least 1ms"},
		{"retry beyond ClaimIdle", sub(SubscriberConfig{Retry: RetryConfig{Initial: time.Second, Max: 2 * time.Minute}}), "cannot exceed ClaimIdle (1m0s)"},
		{"initial beyond ClaimIdle", sub(SubscriberConfig{Retry: RetryConfig{Initial: 2 * time.Minute}}), "cannot exceed ClaimIdle (1m0s)"},
		{"max under initial", sub(SubscriberConfig{Retry: RetryConfig{Initial: 10 * time.Second, Max: time.Second}}), "Retry.Max cannot be shorter than Retry.Initial"},
		{"dead letter to itself", sub(SubscriberConfig{DeadLetter: DeadLetterConfig{Enabled: true, Stream: "orders"}}), "DeadLetter.Stream cannot be the stream it reads"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The address is unreachable: validation fails before connecting.
			tt.cfg.Redis = redisdb.Config{Addrs: []string{"127.0.0.1:1"}}
			_, err := tt.cfg.New()
			if err == nil || !strings.HasPrefix(err.Error(), "redisstream: ") || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New() = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

func TestRetryDelay(t *testing.T) {
	r := RetryConfig{Initial: time.Second, Max: 10 * time.Second}
	for attempt, want := range map[int]time.Duration{0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 5: 10 * time.Second, 1000: 10 * time.Second} {
		if got := r.delay(attempt); got != want {
			t.Errorf("delay(%d) = %s, want %s", attempt, got, want)
		}
	}
	if got := (RetryConfig{}).delay(3); got != 0 {
		t.Errorf("delay without retry = %s, want 0", got)
	}
}

func TestNewDeclares(t *testing.T) {
	_, cl, cfg := fake(t)
	ctx := context.Background()
	for range 2 {
		if err := cl.XAdd(ctx, &redis.XAddArgs{Stream: "orders", Values: []string{PayloadField, "{}"}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.XGroupCreate(ctx, "orders", "billing", "$").Err(); err != nil {
		t.Fatal(err)
	}

	cfg.Streams = []StreamConfig{
		{Name: "orders", Groups: []GroupConfig{{Name: "billing", StartID: "0"}, {Name: "audit", StartID: "0"}}},
		{Name: "payments", Groups: []GroupConfig{{Name: "ledger"}}},
	}
	open(t, cfg)

	// unread returns how many entries each group of stream has not read yet.
	unread := func(stream string, groups ...string) map[string]int {
		got := make(map[string]int, len(groups))
		for _, g := range groups {
			res, err := cl.XReadGroup(ctx, &redis.XReadGroupArgs{Group: g, Consumer: "test", Streams: []string{stream, ">"}, Block: -1}).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				t.Fatal(err)
			}
			for _, r := range res {
				got[g] += len(r.Messages)
			}
		}
		return got
	}
	// The existing group keeps its position, the new one starts at its StartID.
	if got, want := unread("orders", "billing", "audit"), map[string]int{"audit": 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unread entries of orders = %v, want %v", got, want)
	}
	// The stream is created with its group.
	if got, want := unread("payments", "ledger"), map[string]int{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unread entries of payments = %v, want %v", got, want)
	}
}

func TestNewFails(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		m, _, cfg := fake(t)
		m.Close()
		_, err := cfg.New()
		if err == nil || !strings.HasPrefix(err.Error(), "redisstream: redisdb: ping") {
			t.Fatalf("New() = %v", err)
		}
	})
	t.Run("key of another type", func(t *testing.T) {
		m, _, cfg := fake(t)
		if err := m.Set("orders", "x"); err != nil {
			t.Fatal(err)
		}
		_, err := topology(cfg, SubscriberConfig{}).New()
		if err == nil || !strings.Contains(err.Error(), "redisstream: create group billing of stream orders: WRONGTYPE") {
			t.Fatalf("New() = %v", err)
		}
	})
}

func TestNewSubscriber(t *testing.T) {
	_, cl, cfg := fake(t)
	cfg = topology(cfg, SubscriberConfig{})
	cfg.Subscribers = append(cfg.Subscribers, SubscriberConfig{Name: "shipping", Stream: "orders"})
	b := open(t, cfg)

	if _, err := b.NewSubscriber("unknown", worker.SubscriberOptions{Concurrency: 1}); err == nil || !strings.Contains(err.Error(), "redisstream: subscriber config not found for name: unknown") {
		t.Fatalf("NewSubscriber(unknown) = %v", err)
	}
	// shipping has no group in Streams, and nothing created it.
	if _, err := b.NewSubscriber("shipping", worker.SubscriberOptions{Concurrency: 1}); err == nil || !strings.Contains(err.Error(), "redisstream: subscriber shipping: create consumer in group shipping of stream orders: NOGROUP") {
		t.Fatalf("NewSubscriber(shipping) = %v", err)
	}

	s := openSubscriber(t, b, "billing")
	if got := consumers(t, cl); !reflect.DeepEqual(got, map[string]int64{s.consumer: 0}) {
		t.Fatalf("consumers = %v", got)
	}
	if other := openSubscriber(t, b, "billing"); other.consumer == s.consumer {
		t.Fatal("two subscribers share a consumer name")
	}

	for _, tt := range []struct{ concurrency, want int }{{4, 4}, {0, 1}} {
		s, err := b.NewSubscriber("billing", worker.SubscriberOptions{Concurrency: tt.concurrency})
		if err != nil {
			t.Fatal(err)
		}
		if got := s.(*subscriber).concurrency; got != tt.want {
			t.Errorf("a subscriber of concurrency %d handles %d entries at once, want %d", tt.concurrency, got, tt.want)
		}
	}
}

func TestPublishConsume(t *testing.T) {
	_, cl, cfg := fake(t)
	b := open(t, topology(cfg, SubscriberConfig{}))

	msg := worker.NewMessage("order.placed", "order-1", "orders", []byte(`{"id":1}`))
	msg.Headers.Set("tenant", "acme")
	msg.Headers.Set(worker.MessageNameHeader, "spoofed")
	publish(t, b, msg)

	ch, handle := deliveries()
	stop, _ := consume(t, openSubscriber(t, b, "billing"), 1, handle)
	d := next(t, ch)
	got := d.Message()
	if got.ID != msg.ID || got.Name != msg.Name || string(got.Payload) != `{"id":1}` || got.Key != "order-1" || got.Destination != "orders" || !got.CreatedAt.Equal(msg.CreatedAt) {
		t.Fatalf("message = %+v, want %+v", got, msg)
	}
	if tenant, _ := got.Headers.Get("tenant"); tenant != "acme" {
		t.Fatalf("tenant header = %q", tenant)
	}
	if n := len(pending(t, cl)); n != 1 {
		t.Fatalf("%d pending entries before Complete, want 1", n)
	}
	if err := d.Complete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(pending(t, cl)); n != 0 {
		t.Fatalf("%d pending entries after Complete, want 0", n)
	}
	none(t, ch)
	if err := stop(); err != nil {
		t.Fatalf("Consume() = %v", err)
	}
}

func TestPublishErrors(t *testing.T) {
	_, _, cfg := fake(t)
	b := open(t, cfg)
	ctx := context.Background()
	p, err := b.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Publish(ctx, worker.NewMessage("order.placed", "", "", nil)); !errors.Is(err, errNoStream) {
		t.Fatalf("Publish() without stream = %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	msg := worker.NewMessage("order.placed", "", "orders", nil)
	if err := p.Publish(ctx, msg); err == nil || !strings.Contains(err.Error(), "redisstream: publish "+msg.ID+" to stream orders") {
		t.Fatalf("Publish() after Close = %v", err)
	}
}

func TestPublishTrims(t *testing.T) {
	_, cl, cfg := fake(t)
	cfg.Streams = []StreamConfig{{Name: "orders", MaxLen: 2}}
	b := open(t, cfg)
	for _, stream := range []string{"orders", "payments"} {
		for range 4 {
			publish(t, b, worker.NewMessage("order.placed", "", stream, nil))
		}
	}
	// miniredis trims exactly, Redis keeps a little more.
	for stream, want := range map[string]int64{"orders": 2, "payments": 4} {
		if n, err := cl.XLen(context.Background(), stream).Result(); err != nil || n != want {
			t.Fatalf("XLEN %s = %d, %v; want %d", stream, n, err, want)
		}
	}
}

func TestFields(t *testing.T) {
	created := time.Unix(0, 1_700_000_000_123_456_789)
	headers := map[string]string{
		"tenant":                      "acme",
		worker.MessageIDHeader:        "spoofed",
		worker.MessageNameHeader:      "spoofed",
		worker.MessageTimestampHeader: "1",
		PayloadField:                  "spoofed",
		KeyField:                      "spoofed",
	}
	tests := []struct {
		name string
		msg  worker.Message
		want map[string]any
	}{
		{
			name: "complete",
			msg:  worker.Message{ID: "id-1", Name: "order.placed", Payload: []byte("{}"), Headers: safemap.NewJSONMapWithData(headers), CreatedAt: created, Key: "order-1"},
			want: map[string]any{
				worker.MessageIDHeader:        "id-1",
				worker.MessageNameHeader:      "order.placed",
				worker.MessageTimestampHeader: "1700000000123456789",
				"tenant":                      "acme",
				KeyField:                      "order-1",
				PayloadField:                  []byte("{}"),
			},
		},
		{
			// Without CreatedAt, a timestamp header is kept. Without Key, there is no key field.
			name: "without CreatedAt and Key",
			msg:  worker.Message{ID: "id-1", Name: "order.placed", Headers: safemap.NewJSONMapWithData(headers)},
			want: map[string]any{
				worker.MessageIDHeader:        "id-1",
				worker.MessageNameHeader:      "order.placed",
				worker.MessageTimestampHeader: "1",
				"tenant":                      "acme",
				PayloadField:                  []byte(nil),
			},
		},
		{
			name: "without headers",
			msg:  worker.Message{ID: "id-1", Name: "order.placed"},
			want: map[string]any{worker.MessageIDHeader: "id-1", worker.MessageNameHeader: "order.placed", PayloadField: []byte(nil)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := fields(tt.msg)
			got := make(map[string]any, len(values)/2)
			for i := 0; i < len(values); i += 2 {
				k := values[i].(string)
				if _, dup := got[k]; dup {
					t.Fatalf("field %s written twice", k)
				}
				got[k] = values[i+1]
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("fields = %v, want %v", got, tt.want)
			}
			if values[len(values)-2] != PayloadField {
				t.Fatalf("last field = %v, want %s", values[len(values)-2], PayloadField)
			}
		})
	}
}

func TestMessage(t *testing.T) {
	t.Run("with identity", func(t *testing.T) {
		got := message("orders", redis.XMessage{ID: "1700000000123-4", Values: map[string]any{
			worker.MessageIDHeader:        "id-1",
			worker.MessageNameHeader:      "order.placed",
			worker.MessageTimestampHeader: "1700000000000000001",
			KeyField:                      "order-1",
			PayloadField:                  "{}",
			"tenant":                      "acme",
		}})
		if got.ID != "id-1" || got.Name != "order.placed" || string(got.Payload) != "{}" || got.Key != "order-1" || got.Destination != "orders" || got.CreatedAt.UnixNano() != 1700000000000000001 {
			t.Fatalf("message = %+v", got)
		}
		if _, ok := got.Headers.Get(PayloadField); ok {
			t.Fatal("the payload is a header too")
		}
		if v, _ := got.Headers.Get("tenant"); v != "acme" {
			t.Fatalf("tenant = %q", v)
		}
	})
	t.Run("from another producer", func(t *testing.T) {
		got := message("orders", redis.XMessage{ID: "1700000000123-4", Values: map[string]any{worker.MessageNameHeader: "order.placed"}})
		if got.ID != "1700000000123-4" || got.Name != "order.placed" || got.Payload != nil || got.Key != "" || !got.CreatedAt.Equal(time.UnixMilli(1700000000123)) {
			t.Fatalf("message = %+v", got)
		}
	})
}

func TestRetry(t *testing.T) {
	t.Run("at once", func(t *testing.T) {
		_, cl, cfg := fake(t)
		b := open(t, topology(cfg, SubscriberConfig{}))
		msg := worker.NewMessage("order.placed", "", "orders", nil)
		publish(t, b, msg)

		ch, handle := deliveries()
		s := openSubscriber(t, b, "billing")
		consume(t, s, 1, handle)
		if err := next(t, ch).Retry(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		d := next(t, ch)
		if d.Message().ID != msg.ID {
			t.Fatalf("redelivered %s, want %s", d.Message().ID, msg.ID)
		}
		if p := pending(t, cl); len(p) != 1 || p[0].Consumer != s.consumer {
			t.Fatalf("pending = %+v, want one entry of %s", p, s.consumer)
		}
		if err := d.Complete(context.Background()); err != nil {
			t.Fatal(err)
		}
		if n := len(pending(t, cl)); n != 0 {
			t.Fatalf("%d pending entries after Complete", n)
		}
	})

	t.Run("after the delay", func(t *testing.T) {
		m, cl, cfg := fake(t)
		m.SetTime(t0)
		b := open(t, topology(cfg, SubscriberConfig{Retry: RetryConfig{Initial: 10 * time.Second}}))
		msg := worker.NewMessage("order.placed", "", "orders", nil)
		publish(t, b, msg)

		ch, handle := deliveries()
		s := openSubscriber(t, b, "billing")
		consume(t, s, 1, handle)
		// Attempt 2 waits twice Initial: the entry is idle for ClaimIdle minus 20s.
		if err := next(t, ch).Retry(context.Background(), 2); err != nil {
			t.Fatal(err)
		}
		if p := pending(t, cl); len(p) != 1 || p[0].Consumer != retryConsumer || p[0].Idle != 40*time.Second {
			t.Fatalf("pending = %+v, want one entry of %s idle for 40s", p, retryConsumer)
		}
		none(t, ch)
		m.SetTime(t0.Add(19 * time.Second))
		none(t, ch)
		m.SetTime(t0.Add(20 * time.Second))
		if d := next(t, ch); d.Message().ID != msg.ID {
			t.Fatalf("redelivered %s, want %s", d.Message().ID, msg.ID)
		}
		if p := pending(t, cl); len(p) != 1 || p[0].Consumer != s.consumer {
			t.Fatalf("pending = %+v, want one entry of %s", p, s.consumer)
		}
	})
}

func TestReject(t *testing.T) {
	t.Run("without dead letter", func(t *testing.T) {
		_, cl, cfg := fake(t)
		b := open(t, topology(cfg, SubscriberConfig{}))
		publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))

		ch, handle := deliveries()
		consume(t, openSubscriber(t, b, "billing"), 1, handle)
		if err := next(t, ch).Reject(context.Background()); err != nil {
			t.Fatal(err)
		}
		if n := len(pending(t, cl)); n != 0 {
			t.Fatalf("%d pending entries after Reject", n)
		}
		none(t, ch)
	})

	t.Run("dead letter", func(t *testing.T) {
		_, cl, cfg := fake(t)
		b := open(t, topology(cfg, SubscriberConfig{DeadLetter: DeadLetterConfig{Enabled: true}}))
		ctx := context.Background()
		// An entry of another producer, without an ID of its own.
		id, err := cl.XAdd(ctx, &redis.XAddArgs{Stream: "orders", Values: []string{worker.MessageNameHeader, "order.placed", "tenant", "acme", PayloadField, "{}"}}).Result()
		if err != nil {
			t.Fatal(err)
		}

		ch, handle := deliveries()
		consume(t, openSubscriber(t, b, "billing"), 1, handle)
		if err := next(t, ch).Reject(ctx); err != nil {
			t.Fatal(err)
		}
		if n := len(pending(t, cl)); n != 0 {
			t.Fatalf("%d pending entries after Reject", n)
		}
		dead, err := cl.XRange(ctx, "orders:billing:dlq", "-", "+").Result()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{worker.MessageIDHeader: id, worker.MessageNameHeader: "order.placed", "tenant": "acme", PayloadField: "{}"}
		if len(dead) != 1 || !reflect.DeepEqual(dead[0].Values, want) {
			t.Fatalf("dead letters = %+v, want one with %v", dead, want)
		}
	})

	// A copy that cannot be added leaves the entry pending, parked for the longest retry delay.
	for _, tc := range []struct {
		name  string
		retry RetryConfig
		wait  time.Duration
	}{
		{"dead letter fails", RetryConfig{}, time.Minute},
		{"dead letter fails with retries", RetryConfig{Initial: time.Second, Max: 10 * time.Second}, 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, cl, cfg := fake(t)
			m.SetTime(t0)
			b := open(t, topology(cfg, SubscriberConfig{Retry: tc.retry, DeadLetter: DeadLetterConfig{Enabled: true}}))
			msg := worker.NewMessage("order.placed", "", "orders", nil)
			publish(t, b, msg)
			if err := m.Set("orders:billing:dlq", "x"); err != nil {
				t.Fatal(err)
			}

			ch, handle := deliveries()
			consume(t, openSubscriber(t, b, "billing"), 1, handle)
			err := next(t, ch).Reject(context.Background())
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("redisstream: dead-letter %s to orders:billing:dlq, delivering it again in %s: WRONGTYPE", msg.ID, tc.wait)) {
				t.Fatalf("Reject() = %v", err)
			}
			if p := pending(t, cl); len(p) != 1 || p[0].Consumer != retryConsumer || p[0].Idle != time.Minute-tc.wait {
				t.Fatalf("pending = %+v, want one entry of %s idle for %s", p, retryConsumer, time.Minute-tc.wait)
			}
			none(t, ch)
			m.SetTime(t0.Add(tc.wait - time.Second))
			none(t, ch)
			m.SetTime(t0.Add(tc.wait))
			if d := next(t, ch); d.Message().ID != msg.ID {
				t.Fatalf("redelivered %s, want %s", d.Message().ID, msg.ID)
			}
		})
	}
}

// replyError is an error reply of Redis, as go-redis returns them.
type replyError string

func (e replyError) Error() string { return string(e) }

func (replyError) RedisError() {}

// refuse fails the commands that match with its reply, without sending them.
type refuse struct {
	reply replyError
	match func(args []any) bool
}

func (r refuse) DialHook(next redis.DialHook) redis.DialHook { return next }

func (r refuse) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if r.match(cmd.Args()) {
			cmd.SetErr(r.reply)
			return r.reply
		}
		return next(ctx, cmd)
	}
}

func (r refuse) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// outOfMemoryReplies refuses XGROUP CREATE and CREATECONSUMER as Redis does while it is out of memory with the
// noeviction policy, even for a group or consumer that exists. miniredis has no maxmemory.
var outOfMemoryReplies = refuse{
	reply: "OOM command not allowed when used memory > 'maxmemory'.",
	match: func(args []any) bool {
		sub := fmt.Sprint(args[1:2]...)
		return strings.EqualFold(fmt.Sprint(args[0]), "xgroup") && (strings.EqualFold(sub, "create") || strings.EqualFold(sub, "createconsumer"))
	},
}

// Out of memory, New and NewSubscriber only check that the groups exist, so an instance can start
// and consume while Redis refuses new entries. Its first read creates its consumer.
func TestOutOfMemory(t *testing.T) {
	_, cl, cfg := fake(t)
	cfg = topology(cfg, SubscriberConfig{})
	cfg.Subscribers = append(cfg.Subscribers, SubscriberConfig{Name: "shipping", Stream: "orders"})
	b := open(t, cfg)
	ctx := context.Background()
	cl.AddHook(outOfMemoryReplies)
	b.client.AddHook(outOfMemoryReplies)

	if err := declare(ctx, cl, cfg.Streams); err != nil {
		t.Fatalf("declare of an existing group = %v", err)
	}
	for _, s := range []StreamConfig{
		{Name: "orders", Groups: []GroupConfig{{Name: "audit"}}},
		{Name: "payments", Groups: []GroupConfig{{Name: "ledger"}}},
	} {
		want := fmt.Sprintf("redisstream: create group %s of stream %s: OOM", s.Groups[0].Name, s.Name)
		if err := declare(ctx, cl, []StreamConfig{s}); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("declare of a missing group = %v, want %s", err, want)
		}
	}

	// shipping has no group.
	if _, err := b.NewSubscriber("shipping", worker.SubscriberOptions{Concurrency: 1}); err == nil || !strings.Contains(err.Error(), "redisstream: subscriber shipping: create consumer in group shipping of stream orders: OOM") {
		t.Fatalf("NewSubscriber(shipping) = %v", err)
	}
	s := openSubscriber(t, b, "billing")
	if got := consumers(t, cl); len(got) != 0 {
		t.Fatalf("consumers = %v, want none before the first read", got)
	}
	msg := worker.NewMessage("order.placed", "", "orders", nil)
	publish(t, b, msg)
	ch, handle := deliveries()
	consume(t, s, 1, handle)
	if d := next(t, ch); d.Message().ID != msg.ID {
		t.Fatalf("delivered %s, want %s", d.Message().ID, msg.ID)
	}
	if got := consumers(t, cl); !reflect.DeepEqual(got, map[string]int64{s.consumer: 1}) {
		t.Fatalf("consumers = %v, want %s with the entry", got, s.consumer)
	}
}

func TestConsumeConcurrency(t *testing.T) {
	_, cl, cfg := fake(t)
	b := open(t, topology(cfg, SubscriberConfig{}))
	const messages = 6
	for range messages {
		publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
	}

	var running, peak, handled atomic.Int32
	gate := make(chan struct{})
	consume(t, openSubscriber(t, b, "billing"), 2, func(ctx context.Context, d worker.IDelivery) {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		<-gate
		running.Add(-1)
		_ = d.Complete(ctx)
		handled.Add(1)
	})

	eventually(t, "two handlers", func() bool { return running.Load() == 2 })
	time.Sleep(100 * time.Millisecond) // give a third read the chance to happen
	// The subscriber reads no more than it handles: the backlog stays in the stream.
	if n := len(pending(t, cl)); n != 2 {
		t.Fatalf("%d pending entries, want 2", n)
	}
	close(gate)
	eventually(t, "every message", func() bool { return handled.Load() == messages })
	if p := peak.Load(); p != 2 {
		t.Fatalf("peak concurrency = %d, want 2", p)
	}
}

func TestConsumeWaitsForHandlers(t *testing.T) {
	_, cl, cfg := fake(t)
	b := open(t, topology(cfg, SubscriberConfig{}))
	publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))

	started, release := make(chan struct{}), make(chan struct{})
	var completeErr error
	stop, _ := consume(t, openSubscriber(t, b, "billing"), 1, func(ctx context.Context, d worker.IDelivery) {
		close(started)
		<-release
		// The worker handles a message on a context that outlives Consume's.
		completeErr = d.Complete(context.WithoutCancel(ctx))
	})
	<-started

	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	select {
	case err := <-stopped:
		t.Fatalf("Consume() returned %v while a handler was running", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Consume() = %v", err)
	}
	if completeErr != nil {
		t.Fatal(completeErr)
	}
	if n := len(pending(t, cl)); n != 0 {
		t.Fatalf("%d pending entries", n)
	}
}

func TestConsumeClaimsAbandoned(t *testing.T) {
	m, cl, cfg := fake(t)
	m.SetTime(t0)
	b := open(t, topology(cfg, SubscriberConfig{}))
	msg := worker.NewMessage("order.placed", "", "orders", nil)
	publish(t, b, msg)
	// A consumer that crashed after reading the entry.
	if err := cl.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: "billing", Consumer: "crashed", Streams: []string{"orders", ">"}, Block: -1}).Err(); err != nil {
		t.Fatal(err)
	}

	ch, handle := deliveries()
	s := openSubscriber(t, b, "billing")
	consume(t, s, 1, handle)
	none(t, ch)
	m.SetTime(t0.Add(time.Minute))
	if d := next(t, ch); d.Message().ID != msg.ID {
		t.Fatalf("claimed %s, want %s", d.Message().ID, msg.ID)
	}
	if p := pending(t, cl); len(p) != 1 || p[0].Consumer != s.consumer {
		t.Fatalf("pending = %+v, want one entry of %s", p, s.consumer)
	}
}

func TestConsumeClaimsInTurn(t *testing.T) {
	m, cl, cfg := fake(t)
	m.SetTime(t0)
	b := open(t, topology(cfg, SubscriberConfig{}))
	msgs := make([]worker.Message, 5)
	for i := range msgs {
		msgs[i] = worker.NewMessage("order.placed", "", "orders", nil)
		publish(t, b, msgs[i])
	}
	// A consumer that crashed after reading every entry, then the second and the fourth entries
	// wait for a retry, which is not due.
	res, err := cl.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: "billing", Consumer: "crashed", Streams: []string{"orders", ">"}, Block: -1}).Result()
	if err != nil {
		t.Fatal(err)
	}
	m.SetTime(t0.Add(time.Minute))
	for _, i := range []int{1, 3} {
		if err := cl.XClaimJustID(context.Background(), &redis.XClaimArgs{Stream: "orders", Group: "billing", Consumer: retryConsumer, Messages: []string{res[0].Messages[i].ID}}).Err(); err != nil {
			t.Fatal(err)
		}
	}

	// One slot: every claim takes one entry, and the next one goes on after it.
	ch, handle := deliveries()
	consume(t, openSubscriber(t, b, "billing"), 1, handle)
	for _, i := range []int{0, 2, 4} {
		if d := next(t, ch); d.Message().ID != msgs[i].ID {
			t.Fatalf("claimed %s, want entry %d, %s", d.Message().ID, i, msgs[i].ID)
		}
	}
	none(t, ch)
}

func TestClaimReply(t *testing.T) {
	entry := []any{"1-1", []any{PayloadField, "{}", KeyField, "k"}}
	entries, trimmed, err := claimed([]any{entry, nil})
	want := []redis.XMessage{{ID: "1-1", Values: map[string]any{PayloadField: "{}", KeyField: "k"}}}
	if err != nil || !trimmed || !reflect.DeepEqual(entries, want) {
		t.Fatalf("claimed() = %+v, %v, %v; want %+v, true", entries, trimmed, err, want)
	}
	for _, reply := range [][]any{
		{"1-1"},
		{[]any{"1-1"}},
		{[]any{1, []any{}}},
		{[]any{"1-1", []any{"odd"}}},
		{[]any{"1-1", []any{1, "v"}}},
	} {
		if _, _, err := claimed(reply); !errors.Is(err, errClaimReply) {
			t.Errorf("claimed(%v) = %v, want errClaimReply", reply, err)
		}
	}
}

func TestDropTrimmed(t *testing.T) {
	_, cl, cfg := fake(t)
	b := open(t, topology(cfg, SubscriberConfig{}))
	for range 3 {
		publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
	}
	res, err := cl.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: "billing", Consumer: "crashed", Streams: []string{"orders", ">"}, Block: -1}).Result()
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{res[0].Messages[0].ID, res[0].Messages[1].ID, res[0].Messages[2].ID}
	if err := cl.XDel(context.Background(), "orders", ids[0]).Err(); err != nil {
		t.Fatal(err)
	}
	due := make([]redis.XPendingExt, len(ids))
	for i, id := range ids {
		due[i] = redis.XPendingExt{ID: id}
	}

	// The claim returned the third entry and skipped the second, which another consumer claimed
	// first: only the trimmed first one leaves the group.
	s := openSubscriber(t, b, "billing")
	s.dropTrimmed(context.Background(), due, []redis.XMessage{{ID: ids[2]}})
	var left []string
	for _, p := range pending(t, cl) {
		left = append(left, p.ID)
	}
	if !reflect.DeepEqual(left, ids[1:]) {
		t.Fatalf("pending = %v, want %v", left, ids[1:])
	}
}

func TestAfter(t *testing.T) {
	for id, want := range map[string]string{
		"1700000000123-4":                    "1700000000123-5",
		"1700000000123-18446744073709551615": "1700000000124-0",
		"1700000000123":                      claimStart,
		"x-1":                                claimStart,
		"1-x":                                claimStart,
	} {
		if got := after(id); got != want {
			t.Errorf("after(%s) = %s, want %s", id, got, want)
		}
	}
}

func TestHeartbeat(t *testing.T) {
	m, cl, cfg := fake(t)
	m.SetTime(t0)
	b := open(t, topology(cfg, SubscriberConfig{}))
	publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))

	started, release := make(chan struct{}), make(chan struct{})
	consume(t, openSubscriber(t, b, "billing"), 1, func(ctx context.Context, d worker.IDelivery) {
		close(started)
		<-release
		_ = d.Complete(ctx)
	})
	<-started
	defer close(release)

	m.SetTime(t0.Add(50 * time.Second))
	eventually(t, "a refresh", func() bool {
		p := pending(t, cl)
		return len(p) == 1 && p[0].Idle == 0
	})
	// 100s after the delivery, but only 50s after the last refresh: no other consumer claims it.
	m.SetTime(t0.Add(100 * time.Second))
	claimed, _, err := cl.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{Stream: "orders", Group: "billing", MinIdle: time.Minute, Start: "0-0", Count: 10, Consumer: "other"}).Result()
	if err != nil || len(claimed) != 0 {
		t.Fatalf("XAUTOCLAIM = %v, %v; want nothing", claimed, err)
	}
}

func TestConsumeErrors(t *testing.T) {
	t.Run("deleted group", func(t *testing.T) {
		_, cl, cfg := fake(t)
		b := open(t, topology(cfg, SubscriberConfig{}))
		_, handle := deliveries()
		stop, done := consume(t, openSubscriber(t, b, "billing"), 1, handle)
		time.Sleep(50 * time.Millisecond)
		if err := cl.XGroupDestroy(context.Background(), "orders", "billing").Err(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Consume did not fail")
		}
		if err := stop(); err == nil || !strings.Contains(err.Error(), "redisstream: subscriber billing: ") || !strings.Contains(err.Error(), "NOGROUP") {
			t.Fatalf("Consume() = %v", err)
		}
	})

	// Replies that pass, which every command gets for a while. TestIntegrationBroker unblocks a
	// read on Redis; the others take the whole server, which the other packages' integration tests
	// share.
	for _, reply := range []string{
		"LOADING Redis is loading the dataset in memory",
		"UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)",
		"BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
		"MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk.",
		"NOREPLICAS Not enough good replicas to write.",
	} {
		t.Run(strings.Fields(reply)[0], func(t *testing.T) {
			m, _, cfg := fake(t)
			b := open(t, topology(cfg, SubscriberConfig{}))
			ch, handle := deliveries()
			_, done := consume(t, openSubscriber(t, b, "billing"), 1, handle)
			m.SetError(reply)
			// Longer than go-redis retries a command itself, up to about 170ms, which it does
			// for some of the replies.
			time.Sleep(300 * time.Millisecond)
			m.SetError("")
			msg := worker.NewMessage("order.placed", "", "orders", nil)
			publish(t, b, msg)
			if d := next(t, ch); d.Message().ID != msg.ID {
				t.Fatalf("delivered %s, want %s", d.Message().ID, msg.ID)
			}
			select {
			case <-done:
				t.Fatal("Consume returned during the outage")
			default:
			}
		})
	}

	t.Run("restart", func(t *testing.T) {
		m, _, cfg := fake(t)
		b := open(t, topology(cfg, SubscriberConfig{}))
		ch, handle := deliveries()
		consume(t, openSubscriber(t, b, "billing"), 1, handle)
		time.Sleep(50 * time.Millisecond)
		m.Close()
		time.Sleep(100 * time.Millisecond)
		if err := m.Restart(); err != nil {
			t.Fatal(err)
		}
		// Restart keeps the context Close canceled, and blocking reads would get no reply.
		m.Ctx, m.CtxCancel = context.WithCancel(context.Background())
		msg := worker.NewMessage("order.placed", "", "orders", nil)
		publish(t, b, msg)
		if d := next(t, ch); d.Message().ID != msg.ID {
			t.Fatalf("delivered %s, want %s", d.Message().ID, msg.ID)
		}
	})
}

func TestClose(t *testing.T) {
	t.Run("deletes an idle consumer", func(t *testing.T) {
		_, cl, cfg := fake(t)
		b := open(t, topology(cfg, SubscriberConfig{}))
		publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
		ch, handle := deliveries()
		s := openSubscriber(t, b, "billing")
		stop, _ := consume(t, s, 1, handle)
		if err := next(t, ch).Complete(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if got := consumers(t, cl); len(got) != 0 {
			t.Fatalf("consumers = %v, want none", got)
		}
	})

	t.Run("keeps a consumer with pending entries", func(t *testing.T) {
		_, cl, cfg := fake(t)
		b := open(t, topology(cfg, SubscriberConfig{}))
		publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
		ch, handle := deliveries()
		s := openSubscriber(t, b, "billing")
		stop, _ := consume(t, s, 1, handle)
		next(t, ch) // left unsettled, like a failed acknowledgement
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if got := consumers(t, cl); !reflect.DeepEqual(got, map[string]int64{s.consumer: 1}) {
			t.Fatalf("consumers = %v, want %s with one entry", got, s.consumer)
		}
	})
}

// seen makes miniredis record a consumer as active now, and creates it if needed, as Redis does
// for every read or claim. miniredis only does it for XCLAIM, and for XAUTOCLAIM when there is
// something to claim, so seen claims an entry that is not pending.
func seen(t *testing.T, cl *redis.Client, consumer string) {
	t.Helper()
	if err := cl.XClaimJustID(context.Background(), &redis.XClaimArgs{Stream: "orders", Group: "billing", Consumer: consumer, Messages: []string{"0-1"}}).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestSweep(t *testing.T) {
	m, cl, cfg := fake(t)
	m.SetTime(t0)
	b := open(t, topology(cfg, SubscriberConfig{})) // ClaimIdle 1m: stale after 10m
	publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
	s := openSubscriber(t, b, "billing")
	// A consumer that crashed while it held an entry, which nobody claimed yet.
	if err := cl.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: "billing", Consumer: "holding", Streams: []string{"orders", ">"}, Block: -1}).Err(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []string{"holding", "crashed", retryConsumer, s.consumer} {
		seen(t, cl, c)
	}
	m.SetTime(t0.Add(5 * time.Minute))
	seen(t, cl, "recent")

	m.SetTime(t0.Add(11 * time.Minute))
	s.sweep(context.Background())
	want := map[string]int64{"holding": 1, "recent": 0, retryConsumer: 0, s.consumer: 0}
	if got := consumers(t, cl); !reflect.DeepEqual(got, want) {
		t.Fatalf("consumers = %v, want %v", got, want)
	}
}

func TestConsumeSweeps(t *testing.T) {
	m, cl, cfg := fake(t)
	m.SetTime(t0)
	b := open(t, topology(cfg, SubscriberConfig{}))
	seen(t, cl, "crashed")
	if _, found := consumers(t, cl)["crashed"]; !found {
		t.Fatal("no consumer crashed to sweep")
	}

	_, handle := deliveries()
	s := openSubscriber(t, b, "billing")
	consume(t, s, 1, handle)
	m.SetTime(t0.Add(11 * time.Minute))
	eventually(t, "the sweep", func() bool {
		_, found := consumers(t, cl)["crashed"]
		return !found
	})
	if got := consumers(t, cl); !reflect.DeepEqual(got, map[string]int64{s.consumer: 0}) {
		t.Fatalf("consumers = %v, want only %s", got, s.consumer)
	}
}

func TestTransient(t *testing.T) {
	m, cl, _ := fake(t)
	// reply returns the error reply msg of a command.
	reply := func(msg string) error {
		m.SetError(msg)
		defer m.SetError("")
		return cl.Get(context.Background(), "x").Err()
	}
	tests := []struct {
		err  error
		want bool
	}{
		{errors.New("dial tcp: connection refused"), true},
		{redis.ErrPoolTimeout, true},
		{redis.ErrClosed, false},
		{reply("LOADING Redis is loading the dataset in memory"), true},
		{reply("READONLY You can't write against a read only replica."), true},
		{reply("CLUSTERDOWN The cluster is down"), true},
		{reply("UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)"), true},
		{reply("UNBLOCKED client unblocked via CLIENT UNBLOCK"), true},
		{reply("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE."), true},
		{reply("MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk."), true},
		{reply("NOREPLICAS Not enough good replicas to write."), true},
		{reply("NOGROUP No such key 'orders' or consumer group 'billing'"), false},
		{reply("NOPERM this user has no permissions"), false},
		{reply("BUSYGROUP Consumer Group name already exists"), false},
		{reply("ERR syntax error"), false},
		{redis.Nil, false},
	}
	for _, tt := range tests {
		if got := transient(tt.err); got != tt.want {
			t.Errorf("transient(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestEntryMillis(t *testing.T) {
	for id, want := range map[string]int64{"1700000000123-4": 1700000000123, "5": 5} {
		if got, ok := entryMillis(id); !ok || got != want {
			t.Errorf("entryMillis(%s) = %d, %v", id, got, ok)
		}
	}
	if _, ok := entryMillis("x-1"); ok {
		t.Error("entryMillis(x-1) succeeded")
	}
}
