package rabbitmq

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// integrationEnv names the URL of a disposable RabbitMQ, such as amqp://forgo:forgo@127.0.0.1:5672/.
// Without it, the integration tests skip.
const integrationEnv = "ADAPTER_TEST_RABBITMQ_URL"

// TestIntegrationBroker runs a broker against a real RabbitMQ: the topology New declares, confirmed
// publishes, consuming, and the three settlements, a delayed retry and a dead letter included.
func TestIntegrationBroker(t *testing.T) {
	url := os.Getenv(integrationEnv)
	if url == "" {
		t.Skip("set " + integrationEnv + " to run against a disposable RabbitMQ")
	}
	uri, err := amqp.ParseURI(url)
	if err != nil {
		t.Fatal(err)
	}

	name := "forgo-test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	retry := RetryConfig{Initial: 50 * time.Millisecond, Max: 100 * time.Millisecond}
	cfg := Config{
		Name:      "events",
		Addr:      uri.Host,
		Port:      strconv.Itoa(uri.Port),
		Username:  uri.Username,
		Password:  uri.Password,
		VHost:     uri.Vhost,
		Exchanges: []ExchangeConfig{{Name: name, Kind: amqp.ExchangeTopic, Durable: true}},
		Queues: []QueueConfig{{
			Name:       name,
			Durable:    true, // RabbitMQ 4 refuses transient queues that are not exclusive
			Bindings:   []BindingConfig{{Exchange: name, RoutingKey: "order.*"}},
			DeadLetter: DeadLetterConfig{Enabled: true},
			Retry:      retry,
		}},
		Publishers:  []PublisherConfig{{Name: "strict", Mandatory: true}},
		Subscribers: []SubscriberConfig{{Name: "billing", Queue: name}},
	}
	t.Cleanup(func() { deleteTopology(t, url, cfg.Sanitize()) })

	res, err := cfg.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := res.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	b := res.Value().(worker.IBroker)

	pub, err := b.NewPublisher("strict")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	sub, err := b.NewSubscriber("billing")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, err := b.NewSubscriber("unknown"); err == nil {
		t.Fatal("NewSubscriber of an unknown name succeeded")
	}

	deliveries := make(chan worker.IDelivery, 10)
	ctx, stop := context.WithCancel(context.Background())
	consumed := make(chan error, 1)
	go func() {
		consumed <- sub.Consume(ctx, worker.ConsumerState{
			Concurrency: 2,
			Handler:     func(_ context.Context, d worker.IDelivery) { deliveries <- d },
		})
	}()
	// next waits for the delivery of want. A retried message comes back with the routing key of its
	// queue, so the key is not compared.
	next := func(want worker.Message) worker.IDelivery {
		t.Helper()
		select {
		case d := <-deliveries:
			got := d.Message()
			if got.ID != want.ID || got.Name != want.Name || string(got.Payload) != string(want.Payload) {
				t.Fatalf("delivered %+v, want %+v", got, want)
			}
			return d
		case <-time.After(10 * time.Second):
			t.Fatalf("message %s was not delivered", want.ID)
			return nil
		}
	}
	publish := func(p worker.IPublisher, msg worker.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return p.Publish(ctx, msg)
	}

	// Complete.
	msg := worker.NewMessage("order.placed", "order.placed", name, []byte(`{"id":1}`))
	msg.Headers.Set("tenant", "acme")
	if err := publish(pub, msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := next(msg)
	if got := d.Message(); got.Key != msg.Key || got.Destination != name || !got.CreatedAt.Equal(msg.CreatedAt) {
		t.Fatalf("delivered %+v, want the routing key, the exchange and the creation time", got)
	}
	if tenant, _ := d.Message().Headers.Get("tenant"); tenant != "acme" {
		t.Fatalf("tenant header = %q", tenant)
	}
	if err := d.Complete(context.Background()); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Retry through the retry queue, then Reject into the dead-letter queue.
	msg = worker.NewMessage("order.placed", "order.paid", name, []byte(`{"id":2}`))
	if err := publish(pub, msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	retried := time.Now()
	if err := next(msg).Retry(context.Background(), 1); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	d = next(msg)
	if waited := time.Since(retried); waited < retry.Initial {
		t.Fatalf("retried after %s, want at least %s", waited, retry.Initial)
	}
	if got := d.Message(); got.Key != name {
		t.Fatalf("retried message has routing key %q, want its queue %q", got.Key, name)
	}
	if death, _ := d.Message().Headers.Get("x-death"); death == "" {
		t.Fatal("the retried message has no x-death header")
	}
	if err := d.Reject(context.Background()); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	deadLetter := getMessage(t, url, name+".dlq")
	if deadLetter.MessageId != msg.ID {
		t.Fatalf("dead letter = %q, want %q", deadLetter.MessageId, msg.ID)
	}

	// The default publisher needs no config.
	def, err := b.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}
	defer def.Close()
	msg = worker.NewMessage("order.shipped", "order.shipped", name, []byte(`{"id":3}`))
	if err := publish(def, msg); err != nil {
		t.Fatalf("Publish with the default publisher: %v", err)
	}
	if err := next(msg).Complete(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A mandatory message that no queue receives fails.
	unroutable := worker.NewMessage("invoice.paid", "invoice.paid", name, []byte(`{}`))
	if err := publish(pub, unroutable); !errors.Is(err, errUnroutable) {
		t.Fatalf("Publish of an unroutable message = %v, want errUnroutable", err)
	}

	stop()
	select {
	case err := <-consumed:
		if err != nil {
			t.Fatalf("Consume = %v, want nil once stopped", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Consume did not return")
	}
}

// getMessage waits for a message in queue and takes it.
func getMessage(t *testing.T, url, queue string) amqp.Delivery {
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
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get from %s: %v", queue, err)
		}
		if ok {
			return msg
		}
	}
	t.Fatalf("no message in %s", queue)
	return amqp.Delivery{}
}

// deleteTopology deletes what New declared for cfg.
func deleteTopology(t *testing.T, url string, cfg Config) {
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Errorf("connect to clean up: %v", err)
		return
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Errorf("open a channel to clean up: %v", err)
		return
	}
	for _, q := range cfg.Queues {
		queues := []string{q.Name}
		if q.DeadLetter.Enabled {
			queues = append(queues, q.DeadLetter.Queue)
		}
		for _, rq := range q.Retry.queues(q.Name) {
			queues = append(queues, rq.name)
		}
		for _, name := range queues {
			if _, err := ch.QueueDelete(name, false, false, false); err != nil {
				t.Errorf("delete queue %s: %v", name, err)
				return
			}
		}
		if q.DeadLetter.Enabled {
			if err := ch.ExchangeDelete(q.DeadLetter.Exchange, false, false); err != nil {
				t.Errorf("delete exchange %s: %v", q.DeadLetter.Exchange, err)
				return
			}
		}
	}
	for _, e := range cfg.Exchanges {
		if err := ch.ExchangeDelete(e.Name, false, false); err != nil {
			t.Errorf("delete exchange %s: %v", e.Name, err)
			return
		}
	}
}
