package pubsub

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	gpubsub "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var (
	_ worker.IBroker     = (*broker)(nil)
	_ worker.IPublisher  = (*publisher)(nil)
	_ worker.ISubscriber = (*subscriber)(nil)
	_ worker.IDelivery   = (*delivery)(nil)
)

const project = "proj"

// fake starts an in-memory Pub/Sub server and returns a config that connects to it.
func fake(t *testing.T, opts ...pstest.ServerReactorOption) (*pstest.Server, Config) {
	t.Helper()
	srv := pstest.NewServer(opts...)
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = srv.Close()
	})
	return srv, Config{Name: "events", ProjectID: project, Options: []option.ClientOption{option.WithGRPCConn(conn)}}
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

// topology is a topic orders with the subscription billing.
func topology(cfg Config, sub SubscriptionConfig, subscribers ...SubscriberConfig) Config {
	sub.Name, sub.Topic = "billing", "orders"
	cfg.Topics = []TopicConfig{{Name: "orders"}}
	cfg.Subscriptions = []SubscriptionConfig{sub}
	cfg.Subscribers = append([]SubscriberConfig{{Name: "billing", Subscription: "billing"}}, subscribers...)
	return cfg
}

// errConsumeHangs is returned by the stop function of consume when Consume does not return.
var errConsumeHangs = errors.New("Consume did not return")

// consume consumes the subscriber name of b in the background. stop ends it and returns the
// error of Consume.
func consume(t *testing.T, b *broker, name string, concurrency int, handle func(context.Context, worker.IDelivery)) (stop func() error) {
	t.Helper()
	s, err := b.NewSubscriber(name, worker.SubscriberOptions{Concurrency: concurrency})
	if err != nil {
		t.Fatal(err)
	}
	return consumeWith(t, s, handle)
}

// consumeWith is consume for a subscriber of the test's own.
func consumeWith(t *testing.T, s worker.ISubscriber, handle func(context.Context, worker.IDelivery)) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(ctx, handle)
	}()
	var once sync.Once
	var err2 error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err2 = <-done:
			case <-time.After(5 * time.Second):
				err2 = errConsumeHangs
			}
		})
		return err2
	}
	t.Cleanup(func() {
		if err := stop(); errors.Is(err, errConsumeHangs) {
			t.Error(err)
		}
	})
	return stop
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
	case <-time.After(300 * time.Millisecond):
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
		time.Sleep(10 * time.Millisecond)
	}
}

func acks(srv *pstest.Server, id string) int {
	for _, m := range srv.Messages() {
		if m.Attributes[worker.MessageIDHeader] == id {
			return m.Acks
		}
	}
	return 0
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

func TestConfigSanitize(t *testing.T) {
	subscriptions := []SubscriptionConfig{
		{Name: "orders", DeadLetter: DeadLetterConfig{Enabled: true}, Retry: RetryConfig{Initial: time.Second}},
		{Name: "projects/other/subscriptions/billing", DeadLetter: DeadLetterConfig{Enabled: true, Topic: "projects/other/topics/dead"}},
		{Name: "audit", DeadLetter: DeadLetterConfig{Enabled: true, Topic: "dead", Subscription: "dead-letters"}, Retry: RetryConfig{Initial: time.Second, Max: time.Minute}},
		{Name: "plain"},
	}

	got := Config{Subscriptions: subscriptions}.Sanitize()

	if got.ProjectID != gpubsub.DetectProjectID || got.DeclareTimeout != 30*time.Second {
		t.Fatalf("defaults = %q %s", got.ProjectID, got.DeclareTimeout)
	}
	want := []SubscriptionConfig{
		{Name: "orders", DeadLetter: DeadLetterConfig{Enabled: true, Topic: "orders.dlq", Subscription: "orders.dlq"}, Retry: RetryConfig{Initial: time.Second, Max: 10 * time.Minute}},
		{Name: "projects/other/subscriptions/billing", DeadLetter: DeadLetterConfig{Enabled: true, Topic: "projects/other/topics/dead", Subscription: "dead"}},
		{Name: "audit", DeadLetter: DeadLetterConfig{Enabled: true, Topic: "dead", Subscription: "dead-letters"}, Retry: RetryConfig{Initial: time.Second, Max: time.Minute}},
		{Name: "plain"},
	}
	if !reflect.DeepEqual(got.Subscriptions, want) {
		t.Fatalf("subscriptions = %+v, want %+v", got.Subscriptions, want)
	}

	// Configs are shared values: Sanitize must not write through to the caller's slices.
	if subscriptions[0].DeadLetter.Topic != "" || subscriptions[0].Retry.Max != 0 {
		t.Fatal("Sanitize modified the caller's slices")
	}
}

func TestConfigValidate(t *testing.T) {
	sub := func(mod func(*SubscriptionConfig)) Config {
		s := SubscriptionConfig{Name: "billing", Topic: "orders"}
		mod(&s)
		return Config{Subscriptions: []SubscriptionConfig{s}}
	}
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"valid", sub(func(s *SubscriptionConfig) {
			s.Retry = RetryConfig{Initial: time.Second, Max: 10 * time.Minute}
			s.DeadLetter = DeadLetterConfig{Enabled: true, MaxDeliveryAttempts: 5}
		}), ""},
		{"disabled dead letter is ignored", sub(func(s *SubscriptionConfig) { s.DeadLetter.MaxDeliveryAttempts = 1 }), ""},
		{"negative timeout", Config{DeclareTimeout: -1}, "DeclareTimeout cannot be negative"},
		{"unnamed topic", Config{Topics: []TopicConfig{{}}}, "a topic needs a Name"},
		{"unnamed subscription", sub(func(s *SubscriptionConfig) { s.Name = "" }), "a subscription needs a Name"},
		{"subscription without topic", sub(func(s *SubscriptionConfig) { s.Topic = "" }), `subscription "billing" needs a Topic`},
		{"negative retry", sub(func(s *SubscriptionConfig) { s.Retry.Max = -1 }), "cannot be negative"},
		{"retry max under initial", sub(func(s *SubscriptionConfig) { s.Retry = RetryConfig{Initial: time.Minute, Max: time.Second} }), "cannot be shorter"},
		{"retry max too long", sub(func(s *SubscriptionConfig) { s.Retry = RetryConfig{Initial: time.Minute, Max: 11 * time.Minute} }), "cannot exceed 10m0s"},
		{"retry initial too long", sub(func(s *SubscriptionConfig) { s.Retry.Initial = 11 * time.Minute }), "cannot exceed 10m0s"},
		{"too few delivery attempts", sub(func(s *SubscriptionConfig) { s.DeadLetter = DeadLetterConfig{Enabled: true, MaxDeliveryAttempts: 4} }), "between 5 and 100"},
		{"too many delivery attempts", sub(func(s *SubscriptionConfig) { s.DeadLetter = DeadLetterConfig{Enabled: true, MaxDeliveryAttempts: 101} }), "between 5 and 100"},
		{"subscriber without subscription", Config{Subscribers: []SubscriberConfig{{Name: "billing"}}}, `subscriber "billing" needs a Subscription`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Sanitize().validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validate() = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.HasPrefix(err.Error(), "pubsub: ") {
				t.Fatalf("validate() = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

func TestNames(t *testing.T) {
	if got := topicName("p", "orders"); got != "projects/p/topics/orders" {
		t.Fatalf("topicName = %s", got)
	}
	if got := subscriptionName("p", "projects/o/subscriptions/billing"); got != "projects/o/subscriptions/billing" {
		t.Fatalf("subscriptionName = %s", got)
	}
	if got := resourceID("projects/o/topics/orders"); got != "orders" {
		t.Fatalf("resourceID = %s", got)
	}
	if got := resourceID("orders"); got != "orders" {
		t.Fatalf("resourceID = %s", got)
	}
}

func TestMaxOutstanding(t *testing.T) {
	tests := []struct{ configured, concurrency, want int }{
		{0, 8, 8},
		{20, 8, 20},
		{-5, 8, -1},
	}
	for _, tt := range tests {
		if got := maxOutstanding(tt.configured, tt.concurrency); got != tt.want {
			t.Fatalf("maxOutstanding(%d, %d) = %d, want %d", tt.configured, tt.concurrency, got, tt.want)
		}
	}
}

func TestNewDeclares(t *testing.T) {
	_, cfg := fake(t)
	cfg = topology(cfg, SubscriptionConfig{
		Filter:      `attributes.tenant = "acme"`,
		Ordering:    true,
		ExactlyOnce: true,
		Retry:       RetryConfig{Initial: time.Second, Max: time.Minute},
		DeadLetter:  DeadLetterConfig{Enabled: true, MaxDeliveryAttempts: 10},
	})
	b := open(t, cfg)
	ctx := context.Background()

	got, err := b.client.SubscriptionAdminClient.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: "projects/proj/subscriptions/billing"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != "projects/proj/topics/orders" || got.Filter != cfg.Subscriptions[0].Filter || !got.EnableMessageOrdering || !got.EnableExactlyOnceDelivery {
		t.Fatalf("subscription = %v", got)
	}
	if got.RetryPolicy.MinimumBackoff.AsDuration() != time.Second || got.RetryPolicy.MaximumBackoff.AsDuration() != time.Minute {
		t.Fatalf("retry policy = %v", got.RetryPolicy)
	}
	if got.DeadLetterPolicy.DeadLetterTopic != "projects/proj/topics/billing.dlq" || got.DeadLetterPolicy.MaxDeliveryAttempts != 10 {
		t.Fatalf("dead-letter policy = %v", got.DeadLetterPolicy)
	}
	if got.ExpirationPolicy == nil || got.ExpirationPolicy.Ttl != nil {
		t.Fatalf("expiration policy = %v, want never", got.ExpirationPolicy)
	}
	dlq, err := b.client.SubscriptionAdminClient.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: "projects/proj/subscriptions/billing.dlq"})
	if err != nil {
		t.Fatal(err)
	}
	if dlq.Topic != "projects/proj/topics/billing.dlq" {
		t.Fatalf("dead-letter subscription topic = %s", dlq.Topic)
	}

	// Declaring again is idempotent, also when the user changed the settings of the dead-letter
	// subscription, which New leaves to them.
	if _, err := b.client.SubscriptionAdminClient.UpdateSubscription(ctx, &pb.UpdateSubscriptionRequest{
		Subscription: &pb.Subscription{Name: dlq.Name, RetryPolicy: &pb.RetryPolicy{MinimumBackoff: durationpb.New(time.Minute)}},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"retry_policy"}},
	}); err != nil {
		t.Fatal(err)
	}
	open(t, cfg)
}

func TestNewRejectsChangedSubscription(t *testing.T) {
	base := SubscriptionConfig{Retry: RetryConfig{Initial: time.Second}, DeadLetter: DeadLetterConfig{Enabled: true, MaxDeliveryAttempts: 10}}
	tests := []struct {
		name string
		mod  func(*SubscriptionConfig)
		want string
	}{
		{"filter", func(s *SubscriptionConfig) { s.Filter = `attributes.tenant = "acme"` }, "has another filter;"},
		{"ordering", func(s *SubscriptionConfig) { s.Ordering = true }, "has another message ordering;"},
		{"exactly once", func(s *SubscriptionConfig) { s.ExactlyOnce = true }, "has another exactly-once delivery;"},
		{"retry", func(s *SubscriptionConfig) { s.Retry.Max = time.Minute }, "has another retry policy;"},
		{"no retry", func(s *SubscriptionConfig) { s.Retry = RetryConfig{} }, "has another retry policy;"},
		{"dead-letter policy", func(s *SubscriptionConfig) { s.DeadLetter.MaxDeliveryAttempts = 20 }, "has another dead-letter policy;"},
		{"several", func(s *SubscriptionConfig) {
			s.Ordering = true
			s.DeadLetter = DeadLetterConfig{}
		}, "has another message ordering, dead-letter policy;"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, cfg := fake(t)
			open(t, topology(cfg, base))

			changed := base
			tt.mod(&changed)
			_, err := topology(cfg, changed).New()
			if !errors.Is(err, errSubscriptionDiffers) || !strings.Contains(err.Error(), "projects/proj/subscriptions/billing "+tt.want) {
				t.Fatalf("New() = %v, want %q", err, tt.want)
			}
		})
	}

	t.Run("topic", func(t *testing.T) {
		_, cfg := fake(t)
		open(t, topology(cfg, SubscriptionConfig{}))
		cfg = topology(cfg, SubscriptionConfig{})
		cfg.Topics = append(cfg.Topics, TopicConfig{Name: "payments"})
		cfg.Subscriptions[0].Topic = "payments"
		if _, err := cfg.New(); !errors.Is(err, errSubscriptionDiffers) || !strings.Contains(err.Error(), "has another topic;") {
			t.Fatalf("New() = %v", err)
		}
	})
}

func TestNewFails(t *testing.T) {
	t.Run("invalid config", func(t *testing.T) {
		_, err := Config{ProjectID: project, Topics: []TopicConfig{{}}}.New()
		if err == nil || !strings.Contains(err.Error(), "a topic needs a Name") {
			t.Fatalf("New() = %v", err)
		}
	})
	t.Run("create topic", func(t *testing.T) {
		_, cfg := fake(t, pstest.WithErrorInjection("CreateTopic", codes.PermissionDenied, "denied"))
		_, err := topology(cfg, SubscriptionConfig{}).New()
		if err == nil || !strings.Contains(err.Error(), "pubsub: create topic projects/proj/topics/orders") || !strings.Contains(err.Error(), "denied") {
			t.Fatalf("New() = %v", err)
		}
	})
	t.Run("missing topic", func(t *testing.T) {
		_, cfg := fake(t)
		cfg.Subscriptions = []SubscriptionConfig{{Name: "billing", Topic: "orders"}}
		_, err := cfg.New()
		if err == nil || !strings.Contains(err.Error(), "pubsub: create subscription projects/proj/subscriptions/billing") {
			t.Fatalf("New() = %v", err)
		}
	})
	t.Run("get subscription", func(t *testing.T) {
		_, cfg := fake(t, pstest.WithErrorInjection("GetSubscription", codes.PermissionDenied, "denied"))
		open(t, topology(cfg, SubscriptionConfig{}))
		_, err := topology(cfg, SubscriptionConfig{}).New()
		if err == nil || !strings.Contains(err.Error(), "pubsub: get subscription") {
			t.Fatalf("New() = %v", err)
		}
	})
}

func TestNewSubscriberUnknown(t *testing.T) {
	_, cfg := fake(t)
	b := open(t, cfg)
	if _, err := b.NewSubscriber("billing", worker.SubscriberOptions{Concurrency: 1}); err == nil || !strings.Contains(err.Error(), "subscriber config not found") {
		t.Fatalf("NewSubscriber() = %v", err)
	}
}

func TestResolveSubscriber(t *testing.T) {
	cfg := Config{Subscriptions: []SubscriptionConfig{
		{Name: "billing", DeadLetter: DeadLetterConfig{Enabled: true}},
		{Name: "audit"},
	}}.Sanitize()
	tests := []struct {
		in   SubscriberConfig
		want SubscriberConfig
	}{
		{SubscriberConfig{Subscription: "billing"}, SubscriberConfig{Subscription: "projects/p/subscriptions/billing", DeadLetterTopic: "projects/p/topics/billing.dlq"}},
		{SubscriberConfig{Subscription: "projects/p/subscriptions/billing"}, SubscriberConfig{Subscription: "projects/p/subscriptions/billing", DeadLetterTopic: "projects/p/topics/billing.dlq"}},
		{SubscriberConfig{Subscription: "billing", DeadLetterTopic: "dead"}, SubscriberConfig{Subscription: "projects/p/subscriptions/billing", DeadLetterTopic: "projects/p/topics/dead"}},
		{SubscriberConfig{Subscription: "audit"}, SubscriberConfig{Subscription: "projects/p/subscriptions/audit"}},
		{SubscriberConfig{Subscription: "managed", DeadLetterTopic: "projects/o/topics/dead"}, SubscriberConfig{Subscription: "projects/p/subscriptions/managed", DeadLetterTopic: "projects/o/topics/dead"}},
	}
	for _, tt := range tests {
		if got := resolveSubscriber(cfg, "p", tt.in); got != tt.want {
			t.Fatalf("resolveSubscriber(%+v) = %+v, want %+v", tt.in, got, tt.want)
		}
	}
}

func TestPublishConsume(t *testing.T) {
	for _, exactlyOnce := range []bool{false, true} {
		t.Run(map[bool]string{false: "at least once", true: "exactly once"}[exactlyOnce], func(t *testing.T) {
			srv, cfg := fake(t)
			b := open(t, topology(cfg, SubscriptionConfig{ExactlyOnce: exactlyOnce}))

			msg := worker.NewMessage("order.placed", "order-1", "orders", []byte(`{"id":1}`))
			msg.Headers.Set("tenant", "acme")
			msg.Headers.Set(worker.MessageNameHeader, "spoofed")
			publish(t, b, msg)

			ch, handle := deliveries()
			stop := consume(t, b, "billing", 1, handle)
			d := next(t, ch)
			got := d.Message()
			if got.ID != msg.ID || got.Name != msg.Name || string(got.Payload) != `{"id":1}` || got.Key != "order-1" || !got.CreatedAt.Equal(msg.CreatedAt) {
				t.Fatalf("message = %+v, want %+v", got, msg)
			}
			if tenant, _ := got.Headers.Get("tenant"); tenant != "acme" {
				t.Fatalf("tenant header = %q", tenant)
			}
			if err := d.Complete(context.Background()); err != nil {
				t.Fatal(err)
			}
			eventually(t, "the ack", func() bool { return acks(srv, msg.ID) == 1 })
			none(t, ch)
			if err := stop(); err != nil {
				t.Fatalf("Consume() = %v", err)
			}
		})
	}
}

func TestPublishErrors(t *testing.T) {
	_, cfg := fake(t)
	b := open(t, cfg)
	ctx := context.Background()
	p, err := b.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Publish(ctx, worker.NewMessage("order.placed", "", "", nil)); !errors.Is(err, errNoTopic) {
		t.Fatalf("Publish() without topic = %v", err)
	}

	// A failed publish pauses its ordering key in the client. Publish resumes it, so the outbox
	// can publish the message again.
	msg := worker.NewMessage("order.placed", "order-1", "orders", nil)
	if err := p.Publish(ctx, msg); err == nil || !strings.Contains(err.Error(), "pubsub: publish "+msg.ID+" to topic orders") {
		t.Fatalf("Publish() to a missing topic = %v", err)
	}
	if _, err := b.client.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: "projects/proj/topics/orders"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish() after the topic was created = %v", err)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(ctx, msg); !errors.Is(err, errClosed) {
		t.Fatalf("Publish() after Close = %v", err)
	}
}

func TestRetry(t *testing.T) {
	srv, cfg := fake(t)
	b := open(t, topology(cfg, SubscriptionConfig{}))
	msg := worker.NewMessage("order.placed", "", "orders", nil)
	publish(t, b, msg)

	ch, handle := deliveries()
	consume(t, b, "billing", 1, handle)
	if err := next(t, ch).Retry(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	d := next(t, ch)
	if d.Message().ID != msg.ID {
		t.Fatalf("redelivered %s, want %s", d.Message().ID, msg.ID)
	}
	if err := d.Complete(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the ack", func() bool { return acks(srv, msg.ID) == 1 })
}

func TestReject(t *testing.T) {
	t.Run("without dead letter", func(t *testing.T) {
		srv, cfg := fake(t)
		b := open(t, topology(cfg, SubscriptionConfig{}))
		msg := worker.NewMessage("order.placed", "", "orders", nil)
		publish(t, b, msg)

		ch, handle := deliveries()
		consume(t, b, "billing", 1, handle)
		if err := next(t, ch).Reject(context.Background()); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the ack", func() bool { return acks(srv, msg.ID) == 1 })
		none(t, ch)
	})

	t.Run("dead letter", func(t *testing.T) {
		srv, cfg := fake(t)
		cfg = topology(cfg, SubscriptionConfig{DeadLetter: DeadLetterConfig{Enabled: true}}, SubscriberConfig{Name: "dlq", Subscription: "billing.dlq"})
		b := open(t, cfg)
		msg := worker.NewMessage("order.placed", "order-1", "orders", []byte(`{"id":1}`))
		msg.Headers.Set("tenant", "acme")
		publish(t, b, msg)

		ch, handle := deliveries()
		consume(t, b, "billing", 1, handle)
		if err := next(t, ch).Reject(context.Background()); err != nil {
			t.Fatal(err)
		}
		none(t, ch)

		dead, handleDead := deliveries()
		consume(t, b, "dlq", 1, handleDead)
		dl := next(t, dead)
		// Receive returns only once every message it delivered is settled.
		defer func() { _ = dl.Complete(context.Background()) }()
		got := dl.Message()
		tenant, _ := got.Headers.Get("tenant")
		if got.ID != msg.ID || got.Name != msg.Name || string(got.Payload) != `{"id":1}` || got.Key != "order-1" || tenant != "acme" {
			t.Fatalf("dead letter = %+v, want %+v", got, msg)
		}
		eventually(t, "the ack", func() bool {
			for _, m := range srv.Messages() {
				if m.Topic == "projects/proj/topics/orders" {
					return m.Acks == 1
				}
			}
			return false
		})
	})

	t.Run("dead letter fails", func(t *testing.T) {
		_, cfg := fake(t)
		b := open(t, topology(cfg, SubscriptionConfig{}, SubscriberConfig{Name: "missing", Subscription: "billing", DeadLetterTopic: "missing"}))
		msg := worker.NewMessage("order.placed", "", "orders", nil)
		publish(t, b, msg)

		ch, handle := deliveries()
		consume(t, b, "missing", 1, handle)
		err := next(t, ch).Reject(context.Background())
		if err == nil || !strings.Contains(err.Error(), "pubsub: dead-letter "+msg.ID+" to projects/proj/topics/missing, delivering it again") {
			t.Fatalf("Reject() = %v", err)
		}
		d := next(t, ch)
		defer func() { _ = d.Complete(context.Background()) }()
		if d.Message().ID != msg.ID {
			t.Fatalf("redelivered %s, want %s", d.Message().ID, msg.ID)
		}
	})
}

func TestConsumeConcurrency(t *testing.T) {
	_, cfg := fake(t)
	cfg = topology(cfg, SubscriptionConfig{})
	cfg.Subscribers[0].MaxOutstandingMessages = 10
	b := open(t, cfg)
	const messages = 6
	for range messages {
		publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
	}

	var running, peak, handled atomic.Int32
	gate := make(chan struct{})
	s, err := b.NewSubscriber("billing", worker.SubscriberOptions{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	consumeWith(t, s, func(ctx context.Context, d worker.IDelivery) {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		<-gate
		running.Add(-1)
		_ = d.Complete(ctx)
		handled.Add(1)
	})

	eventually(t, "two handlers", func() bool { return running.Load() == 2 })
	// Set before receiving started, which happened before the handlers ran.
	if got := s.(*subscriber).sub.ReceiveSettings.MaxOutstandingMessages; got != 10 {
		t.Fatalf("MaxOutstandingMessages = %d, want 10", got)
	}
	time.Sleep(100 * time.Millisecond) // give a third handler the chance to start
	close(gate)
	eventually(t, "every message", func() bool { return handled.Load() == messages })
	if p := peak.Load(); p != 2 {
		t.Fatalf("peak concurrency = %d, want 2", p)
	}
}

func TestConsumeWaitsForHandlers(t *testing.T) {
	srv, cfg := fake(t)
	b := open(t, topology(cfg, SubscriptionConfig{}))
	msg := worker.NewMessage("order.placed", "", "orders", nil)
	publish(t, b, msg)

	started, release := make(chan struct{}), make(chan struct{})
	var completeErr error
	stop := consume(t, b, "billing", 1, func(ctx context.Context, d worker.IDelivery) {
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
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Consume() = %v", err)
	}
	if completeErr != nil {
		t.Fatal(completeErr)
	}
	// The client sent the acknowledgement before Consume returned.
	if n := acks(srv, msg.ID); n != 1 {
		t.Fatalf("%d acks when Consume returned, want 1", n)
	}
}

func TestConsumeMissingSubscription(t *testing.T) {
	_, cfg := fake(t)
	cfg.Subscribers = []SubscriberConfig{{Name: "billing", Subscription: "billing"}}
	b := open(t, cfg)
	s, err := b.NewSubscriber("billing", worker.SubscriberOptions{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(context.Background(), func(context.Context, worker.IDelivery) {})
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "pubsub: subscriber billing: receive from projects/proj/subscriptions/billing") {
			t.Fatalf("Consume() = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not fail")
	}
}

// endedStream is a StreamingPull stream that the server ended with err as soon as it opened.
type endedStream struct {
	ctx context.Context
	err error
}

func (s *endedStream) Header() (metadata.MD, error) { return nil, nil }
func (s *endedStream) Trailer() metadata.MD         { return nil }
func (s *endedStream) CloseSend() error             { return nil }
func (s *endedStream) Context() context.Context     { return s.ctx }
func (s *endedStream) SendMsg(any) error            { return nil }
func (s *endedStream) RecvMsg(any) error            { return s.err }

// endingStreams is fake for a server that ends the first n StreamingPull streams with err once
// they opened, as Pub/Sub ends a stream with a status. It returns the number of streams opened.
// pstest cannot inject errors into StreamingPull, so the client's connection does.
func endingStreams(t *testing.T, n int32, err error) (*pstest.Server, Config, *atomic.Int32) {
	t.Helper()
	srv := pstest.NewServer()
	streams := new(atomic.Int32)
	conn, cerr := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			if strings.HasSuffix(method, "/StreamingPull") && streams.Add(1) <= n {
				return &endedStream{ctx: ctx, err: err}, nil
			}
			return streamer(ctx, desc, cc, method, opts...)
		}))
	if cerr != nil {
		t.Fatal(cerr)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = srv.Close()
	})
	return srv, Config{Name: "events", ProjectID: project, Options: []option.ClientOption{option.WithGRPCConn(conn)}}, streams
}

// quickRetries returns the subscriber name of b, which handles a message at a time and pauses
// briefly before receiving again.
func quickRetries(t *testing.T, b *broker, name string) *subscriber {
	t.Helper()
	s, err := b.NewSubscriber(name, worker.SubscriberOptions{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	sub := s.(*subscriber)
	sub.retryInitial, sub.retryMax = 10*time.Millisecond, 40*time.Millisecond
	return sub
}

func TestConsumeReceivesAgain(t *testing.T) {
	// Statuses the client gives up on when a stream ends with them, and that pass.
	for _, err := range []error{
		status.Error(codes.ResourceExhausted, "Quota exceeded for quota metric 'StreamingPull subscriber throughput' of service 'pubsub.googleapis.com'"),
		status.Error(codes.Unknown, "unexpected HTTP status code received from server: 520"),
		status.Error(codes.Unavailable, "Server shutdownNow invoked"),
	} {
		t.Run(status.Code(err).String(), func(t *testing.T) {
			srv, cfg, streams := endingStreams(t, 3, err)
			b := open(t, topology(cfg, SubscriptionConfig{}))
			ch, handle := deliveries()
			stop := consumeWith(t, quickRetries(t, b, "billing"), handle)
			eventually(t, "a stream that stays open", func() bool { return streams.Load() > 3 })

			msg := worker.NewMessage("order.placed", "", "orders", nil)
			publish(t, b, msg)
			d := next(t, ch)
			if d.Message().ID != msg.ID {
				t.Fatalf("delivered %s, want %s", d.Message().ID, msg.ID)
			}
			if err := d.Complete(context.Background()); err != nil {
				t.Fatal(err)
			}
			eventually(t, "the ack", func() bool { return acks(srv, msg.ID) == 1 })
			if err := stop(); err != nil {
				t.Fatalf("Consume() = %v", err)
			}
		})
	}

	t.Run("lasting", func(t *testing.T) {
		_, cfg, streams := endingStreams(t, 100, status.Error(codes.PermissionDenied, "User not authorized to perform this action."))
		b := open(t, topology(cfg, SubscriptionConfig{}))
		_, handle := deliveries()
		stop := consumeWith(t, quickRetries(t, b, "billing"), handle)
		eventually(t, "the stream", func() bool { return streams.Load() == 1 })
		err := stop()
		if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "pubsub: subscriber billing: receive from projects/proj/subscriptions/billing") {
			t.Fatalf("Consume() = %v", err)
		}
		if n := streams.Load(); n != 1 {
			t.Fatalf("%d streams, want 1", n)
		}
	})

	t.Run("stopped while pausing", func(t *testing.T) {
		_, cfg, streams := endingStreams(t, 100, status.Error(codes.ResourceExhausted, "Quota exceeded"))
		b := open(t, topology(cfg, SubscriptionConfig{}))
		s := quickRetries(t, b, "billing")
		s.retryInitial, s.retryMax = time.Minute, time.Minute
		_, handle := deliveries()
		stop := consumeWith(t, s, handle)
		eventually(t, "the stream", func() bool { return streams.Load() == 1 })
		if err := stop(); err != nil {
			t.Fatalf("Consume() = %v", err)
		}
	})
}

// While Pub/Sub does not answer, the client retries the last acknowledgements for about a minute
// before Receive returns. Consume waits for its handlers, then only for flushWait.
func TestConsumeStopsWhileAcksHang(t *testing.T) {
	srv := pstest.NewServer()
	answer := make(chan struct{})
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if strings.HasSuffix(method, "/Acknowledge") {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-answer:
				}
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = srv.Close()
	})
	b := open(t, topology(Config{Name: "events", ProjectID: project, Options: []option.ClientOption{option.WithGRPCConn(conn)}}, SubscriptionConfig{}))
	t.Cleanup(func() { close(answer) }) // before the broker closes, which waits for nothing hung
	msg := worker.NewMessage("order.placed", "", "orders", nil)
	publish(t, b, msg)

	s := quickRetries(t, b, "billing")
	s.flushWait = 200 * time.Millisecond
	started, finish := make(chan struct{}), make(chan struct{})
	stop := consumeWith(t, s, func(ctx context.Context, d worker.IDelivery) {
		close(started)
		<-finish
		_ = d.Complete(context.WithoutCancel(ctx))
	})
	<-started

	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	select {
	case err := <-stopped:
		t.Fatalf("Consume() returned %v while a handler was running", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(finish)
	finished := time.Now()
	if err := <-stopped; err != nil {
		t.Fatalf("Consume() = %v", err)
	}
	if waited := time.Since(finished); waited > 2*time.Second {
		t.Fatalf("Consume returned %s after the handler, want about flushWait", waited)
	}
	if n := acks(srv, msg.ID); n != 0 {
		t.Fatalf("%d acks reached the server, which did not answer", n)
	}
}

// While Pub/Sub does not answer, the client retries a publish its caller gave up on for a minute.
// Close waits for it for stopWait only.
func TestCloseWhilePublishesHang(t *testing.T) {
	srv := pstest.NewServer()
	answer := make(chan struct{})
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if strings.HasSuffix(method, "/Publish") {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-answer:
				}
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(answer)
		_ = conn.Close()
		_ = srv.Close()
	})
	cfg := Config{Name: "events", ProjectID: project, Options: []option.ClientOption{option.WithGRPCConn(conn)}, Topics: []TopicConfig{{Name: "orders"}}}
	res, err := cfg.New()
	if err != nil {
		t.Fatal(err)
	}
	b := res.(*broker)
	b.topics.stopWait = 200 * time.Millisecond

	p, err := b.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := p.Publish(ctx, worker.NewMessage("order.placed", "", "orders", nil)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish() = %v, want a deadline exceeded", err)
	}

	closed := make(chan error, 1)
	start := time.Now()
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for the publish its caller gave up on")
	}
	if waited := time.Since(start); waited < 150*time.Millisecond {
		t.Fatalf("Close returned after %s, want it to wait about stopWait for the pending publish", waited)
	}
}

func TestRetryable(t *testing.T) {
	for code, want := range map[codes.Code]bool{
		codes.ResourceExhausted:  true,
		codes.Unknown:            true,
		codes.Unavailable:        true,
		codes.Internal:           true,
		codes.DeadlineExceeded:   true,
		codes.Aborted:            true,
		codes.NotFound:           false,
		codes.PermissionDenied:   false,
		codes.InvalidArgument:    false,
		codes.FailedPrecondition: false,
		codes.Unauthenticated:    false,
		codes.Canceled:           false,
	} {
		if got := retryable(fmt.Errorf("receive: %w", status.Error(code, "x"))); got != want {
			t.Errorf("retryable(%s) = %v, want %v", code, got, want)
		}
	}
	if retryable(errors.New("not a status")) {
		t.Error("retryable(non-status error) = true")
	}
}

func TestDeliveryMessage(t *testing.T) {
	published := time.Unix(1700000000, 123)
	m := &gpubsub.Message{
		ID:          "server-id",
		Data:        []byte("{}"),
		Attributes:  map[string]string{"tenant": "acme"},
		PublishTime: published,
		OrderingKey: "order-1",
	}

	got := newDelivery(m, nil).Message()

	if got.ID != "server-id" || got.Name != "unknown" || !got.CreatedAt.Equal(published) || got.Key != "order-1" || got.Destination != "" {
		t.Fatalf("message = %+v", got)
	}
	if tenant, _ := got.Headers.Get("tenant"); tenant != "acme" {
		t.Fatalf("tenant header = %q", tenant)
	}
	if len(m.Attributes) != 1 {
		t.Fatalf("newDelivery modified the attributes: %v", m.Attributes)
	}

	// The identity attributes win over the fallbacks.
	m.Attributes = map[string]string{worker.MessageIDHeader: "id", worker.MessageNameHeader: "order.placed", worker.MessageTimestampHeader: "5"}
	got = newDelivery(m, nil).Message()
	if got.ID != "id" || got.Name != "order.placed" || got.CreatedAt.UnixNano() != 5 {
		t.Fatalf("message = %+v", got)
	}
}

func TestMessage(t *testing.T) {
	msg := worker.NewMessage("order.placed", "order-1", "orders", []byte("{}"))
	msg.Headers.Set(worker.MessageIDHeader, "spoofed")
	msg.Headers.Set("tenant", "acme")

	got := message(msg)

	want := map[string]string{
		worker.MessageIDHeader:        msg.ID,
		worker.MessageNameHeader:      "order.placed",
		worker.MessageTimestampHeader: strconv.FormatInt(msg.CreatedAt.UnixNano(), 10),
		"tenant":                      "acme",
	}
	if !reflect.DeepEqual(got.Attributes, want) || got.OrderingKey != "order-1" || string(got.Data) != "{}" {
		t.Fatalf("message = %+v, want attributes %v", got, want)
	}

	msg.CreatedAt = time.Time{}
	msg.Headers = nil
	if _, ok := message(msg).Attributes[worker.MessageTimestampHeader]; ok {
		t.Fatal("a message without CreatedAt has a timestamp attribute")
	}
}

func TestDeadLetterMessage(t *testing.T) {
	m := &gpubsub.Message{ID: "server-id", Data: []byte("{}"), Attributes: map[string]string{"tenant": "acme"}, OrderingKey: "order-1"}

	got := deadLetterMessage(newDelivery(m, nil))

	want := map[string]string{"tenant": "acme", worker.MessageIDHeader: "server-id"}
	if !reflect.DeepEqual(got.Attributes, want) || got.OrderingKey != "order-1" || string(got.Data) != "{}" {
		t.Fatalf("dead letter = %+v", got)
	}
	if len(m.Attributes) != 1 {
		t.Fatalf("deadLetterMessage modified the attributes: %v", m.Attributes)
	}
}
