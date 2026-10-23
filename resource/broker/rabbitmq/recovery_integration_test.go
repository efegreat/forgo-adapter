package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// faultyNetwork dials the broker for the recovery tests, and fails like a network: cut closes every
// connection it made, and while it refuses, dials fail.
type faultyNetwork struct {
	mu     sync.Mutex
	conns  []net.Conn
	refuse bool
}

func (n *faultyNetwork) dial(network, addr string) (net.Conn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.refuse {
		return nil, errors.New("network unreachable")
	}
	conn, err := net.DialTimeout(network, addr, 5*time.Second)
	if err == nil {
		n.conns = append(n.conns, conn)
	}
	return conn, err
}

// cut closes the connections, as a lost network would.
func (n *faultyNetwork) cut() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, conn := range n.conns {
		_ = conn.Close()
	}
	n.conns = nil
}

func (n *faultyNetwork) setRefuse(refuse bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.refuse = refuse
}

// recoveryTest is a broker on the RabbitMQ of integrationEnv, which recovers at once, with a queue
// of its own bound to a fanout exchange of the same name.
type recoveryTest struct {
	url     string
	name    string
	network *faultyNetwork
	broker  *broker
}

// newRecoveryTest opens a broker for cfg, which it completes with the address of the RabbitMQ, the
// exchange and the queue of the test and a recovery that retries at once. queue configures the
// queue and sub the subscriber named "sub". It skips without integrationEnv.
func newRecoveryTest(t *testing.T, cfg Config, queue QueueConfig, sub SubscriberConfig) *recoveryTest {
	t.Helper()
	url := os.Getenv(integrationEnv)
	if url == "" {
		t.Skip("set " + integrationEnv + " to run against a disposable RabbitMQ")
	}
	uri, err := amqp.ParseURI(url)
	if err != nil {
		t.Fatal(err)
	}

	name := "forgo-recovery-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cfg.Name = name
	cfg.Addr, cfg.Port = uri.Host, strconv.Itoa(uri.Port)
	cfg.Username, cfg.Password, cfg.VHost = uri.Username, uri.Password, uri.Vhost
	if cfg.Recovery.RetryInterval == 0 {
		cfg.Recovery.RetryInterval = 10 * time.Millisecond
	}
	cfg.Exchanges = []ExchangeConfig{{Name: name, Kind: amqp.ExchangeFanout, Durable: true}}
	queue.Name = name
	queue.Bindings = []BindingConfig{{Exchange: name}}
	cfg.Queues = []QueueConfig{queue}
	sub.Name, sub.Queue = "sub", name
	cfg.Subscribers = []SubscriberConfig{sub}

	n := &faultyNetwork{}
	b, err := cfg.open(n.dial)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = b.Close()
		deleteTopology(t, url, cfg.Sanitize())
	})
	return &recoveryTest{url: url, name: name, network: n, broker: b}
}

// publish publishes n messages to the test's exchange through the broker's default publisher, with
// the IDs prefix-0 to prefix-n-1, and returns the IDs.
func (r *recoveryTest) publish(t *testing.T, prefix string, n int) []string {
	t.Helper()
	pub, err := r.broker.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-%d", prefix, i)
		msg := worker.NewMessage("m", "k", r.name, []byte(`{}`))
		msg.ID = ids[i]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pub.Publish(ctx, msg)
		cancel()
		if err != nil {
			t.Fatalf("Publish %s: %v", msg.ID, err)
		}
	}
	return ids
}

// queued returns the number of messages ready in the test's queue.
func (r *recoveryTest) queued(t *testing.T) int {
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
	q, err := ch.QueueDeclarePassive(r.name, true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return q.Messages
}

// consumption is a subscriber's consumption, which records what its handlers did per message ID.
type consumption struct {
	sub     *subscriber
	stop    context.CancelFunc
	done    chan error
	mu      sync.Mutex
	handled map[string]int
	acked   map[string]bool
	stale   int
	// lost counts the acknowledgements that a cut connection failed to send. The broker
	// redelivers their messages.
	lost     int
	handlers int
	// closes counts the channel's closes by reply code: those of a lost connection are not codes
	// of the broker's.
	closes map[int]int
}

// consume starts consuming the test's queue with concurrency handlers that each take handle, then
// complete their delivery.
func (r *recoveryTest) consume(t *testing.T, concurrency int, handle time.Duration) *consumption {
	t.Helper()
	s, err := r.broker.NewSubscriber("sub")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	c := &consumption{
		sub:     s.(*subscriber),
		stop:    stop,
		done:    make(chan error, 1),
		handled: map[string]int{},
		acked:   map[string]bool{},
		closes:  map[int]int{},
	}
	closes := c.sub.ch.(*amqp.Channel).NotifyClose(make(chan *amqp.Error, 16))
	go func() {
		for err := range closes {
			c.mu.Lock()
			c.closes[err.Code]++
			c.mu.Unlock()
		}
	}()
	go func() {
		c.done <- s.Consume(ctx, worker.ConsumerState{Concurrency: concurrency, Handler: func(ctx context.Context, d worker.IDelivery) {
			time.Sleep(handle)
			err := d.Complete(ctx)
			id := d.Message().ID
			c.mu.Lock()
			defer c.mu.Unlock()
			c.handlers++
			c.handled[id]++
			switch {
			case err == nil:
				c.acked[id] = true
			case errors.Is(err, errStaleDelivery):
				c.stale++
			default:
				c.lost++
			}
		}})
	}()
	t.Cleanup(func() {
		stop()
		<-c.done
		_ = s.Close()
	})
	return c
}

func (c *consumption) ackedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.acked)
}

// waitAcked waits until n messages were acknowledged.
func (c *consumption) waitAcked(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for c.ackedCount() < n {
		select {
		case err := <-c.done:
			t.Fatalf("Consume returned %v with %d of %d messages acknowledged", err, c.ackedCount(), n)
		case <-time.After(5 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d messages acknowledged", c.ackedCount(), n)
		}
	}
}

// checkAll checks that every message of ids was acknowledged by a handler of its own, and that the
// broker closed the channel only closes times with PRECONDITION_FAILED: never for an acknowledgement
// of a delivery tag of a lost channel.
func (c *consumption) checkAll(t *testing.T, ids []string, closes int) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		if !c.acked[id] {
			t.Errorf("%s was not acknowledged by its handler (handled %d times)", id, c.handled[id])
		}
	}
	if c.closes[amqp.PreconditionFailed] != closes {
		t.Errorf("the broker closed the channel %d times with PRECONDITION_FAILED, want %d: %v", c.closes[amqp.PreconditionFailed], closes, c.closes)
	}
}

// A publish to an exchange that does not exist makes the broker close the publisher's channel. The
// publish fails with the broker's reason, and the client reopens the channel: the next publishes
// are confirmed, however soon they follow.
func TestIntegrationPublisherChannelRecovery(t *testing.T) {
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{})
	pub, err := r.broker.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	const rounds, perRound = 5, 20
	for round := range rounds {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pub.Publish(ctx, worker.NewMessage("m", "k", r.name+"-missing", []byte(`{}`)))
		cancel()
		if !errors.Is(err, errChannelLost) || !errors.Is(err, &amqp.Error{Code: amqp.NotFound}) && !containsCode(err, amqp.NotFound) {
			t.Fatalf("round %d: Publish to a missing exchange = %v, want errChannelLost with NOT_FOUND", round, err)
		}

		for i := range perRound {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := pub.Publish(ctx, worker.NewMessage("m", "k", r.name, []byte(`{}`)))
			cancel()
			if err != nil {
				t.Fatalf("round %d: publish %d after the channel was recovered: %v", round, i, err)
			}
		}
	}

	if got := r.queued(t); got != rounds*perRound {
		t.Fatalf("the queue holds %d messages, want %d", got, rounds*perRound)
	}
}

// containsCode reports whether err wraps an *amqp.Error with code.
func containsCode(err error, code int) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == code
}

// Publishes keep going while the connection is lost and recovered again and again: every publish
// that the client sends once it reopened the channel is confirmed.
func TestIntegrationConnectionRecoveryPublishes(t *testing.T) {
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{})
	pub, err := r.broker.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	var confirmed, failed atomic.Int64
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			pctx, cancel := context.WithTimeout(ctx, time.Second)
			err := pub.Publish(pctx, worker.NewMessage("m", "k", r.name, []byte(`{}`)))
			cancel()
			if err == nil {
				confirmed.Add(1)
			} else {
				failed.Add(1)
			}
		}
	}()
	defer func() {
		stop()
		<-done
	}()

	waitConfirmed := func(n int64, what string) {
		t.Helper()
		target := confirmed.Load() + n
		for deadline := time.Now().Add(15 * time.Second); confirmed.Load() < target; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: %d publishes confirmed of %d, %d failed", what, confirmed.Load(), target, failed.Load())
			}
		}
	}

	waitConfirmed(100, "before the first cut")
	for round := range 5 {
		r.network.cut()
		waitConfirmed(200, fmt.Sprintf("after cut %d", round+1))
	}

	stop()
	<-done
	// A publish whose confirm a cut lost may have been stored: the queue holds at least every
	// confirmed message.
	if queued := r.queued(t); int64(queued) < confirmed.Load() {
		t.Fatalf("the queue holds %d messages, but %d publishes were confirmed", queued, confirmed.Load())
	}
}

// Deliveries that the client buffers when the connection is lost, here because the prefetch exceeds
// the concurrency, never reach a handler, and their delivery tags never reach the reopened channel:
// every message is acknowledged by its own handler, and the broker never closes the channel.
func TestIntegrationConnectionRecoveryConsumes(t *testing.T) {
	const total, concurrency, prefetch = 600, 2, 100
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{Prefetch: prefetch})
	ids := r.publish(t, "msg", total)
	c := r.consume(t, concurrency, 2*time.Millisecond)

	c.waitAcked(t, 150, 15*time.Second)
	r.network.cut()
	c.waitAcked(t, 350, 15*time.Second)
	r.network.cut()
	c.waitAcked(t, total, 30*time.Second)

	c.checkAll(t, ids, 0)
	c.mu.Lock()
	handlers, stale, lost := c.handlers, c.stale, c.lost
	c.mu.Unlock()
	// Each cut redelivers the messages whose handlers were running, the one the loop may hold for a
	// free handler, and the few it may hand out before the client notices the cut. The deliveries
	// of the lost channel that the client buffers, up to the prefetch, are dropped unhandled.
	if limit := total + 2*(concurrency+1+10); handlers > limit {
		t.Errorf("%d handlers ran for %d messages, want at most %d: deliveries of a lost channel were handled", handlers, total, limit)
	}
	t.Logf("%d handlers for %d messages, %d acknowledgements dropped as stale, %d lost with the connection", handlers, total, stale, lost)
	if queued := r.queued(t); queued != 0 {
		t.Errorf("the queue still holds %d messages", queued)
	}
}

// The broker closes a subscriber's channel, here for an acknowledgement of an unknown delivery tag,
// while the client buffers deliveries of it. The client reopens the channel, and the deliveries of
// the lost one are dropped instead of closing the reopened one again.
func TestIntegrationSubscriberChannelRecovery(t *testing.T) {
	const total, concurrency, prefetch = 200, 1, 50
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{Prefetch: prefetch})
	ids := r.publish(t, "msg", total)
	c := r.consume(t, concurrency, 2*time.Millisecond)

	c.waitAcked(t, 40, 15*time.Second)
	if err := c.sub.ch.(*amqp.Channel).Ack(1_000_000, false); err != nil {
		t.Fatal(err)
	}
	c.waitAcked(t, total, 30*time.Second)

	c.checkAll(t, ids, 1)
	c.mu.Lock()
	handlers := c.handlers
	c.mu.Unlock()
	if limit := total + concurrency + 2; handlers > limit {
		t.Errorf("%d handlers ran for %d messages, want at most %d: deliveries of the lost channel were handled", handlers, total, limit)
	}
	if queued := r.queued(t); queued != 0 {
		t.Errorf("the queue still holds %d messages", queued)
	}
}

// The broker deletes an auto-delete queue with its last consumer, when its channel or connection
// is lost. The recovery declares it again with its binding before it subscribes again.
func TestIntegrationAutoDeleteQueueRecovery(t *testing.T) {
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true, AutoDelete: true}, SubscriberConfig{})
	c := r.consume(t, 1, 0)
	c.waitAcked(t, 0, time.Second)

	var ids []string
	ids = append(ids, r.publish(t, "before", 1)...)
	c.waitAcked(t, len(ids), 10*time.Second)

	gen := c.sub.guard.generation()
	if err := c.sub.ch.(*amqp.Channel).Ack(1_000_000, false); err != nil {
		t.Fatal(err)
	}
	waitResubscribed(t, r, c, gen)
	ids = append(ids, r.publishConfirmed(t, "after-channel"))
	c.waitAcked(t, len(ids), 10*time.Second)

	gen = c.sub.guard.generation()
	r.network.cut()
	waitResubscribed(t, r, c, gen)
	ids = append(ids, r.publishConfirmed(t, "after-connection"))
	c.waitAcked(t, len(ids), 10*time.Second)

	c.checkAll(t, ids, 1)
}

// publishConfirmed publishes the message prefix-0 like publish, and again while the broker nacks
// it, as the outbox would. Right after a recovery, the publisher's channel can still learn that the
// deleted auto-delete queue of the same name went away, and nack a message routed to the new one.
func (r *recoveryTest) publishConfirmed(t *testing.T, prefix string) string {
	t.Helper()
	pub, err := r.broker.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	msg := worker.NewMessage("m", "k", r.name, []byte(`{}`))
	msg.ID = prefix + "-0"
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pub.Publish(ctx, msg)
		cancel()
		if err == nil {
			return msg.ID
		}
		if !errors.Is(err, errNacked) || attempt == 10 {
			t.Fatalf("Publish %s: %v", msg.ID, err)
		}
		t.Logf("Publish %s: %v; publishing it again", msg.ID, err)
		time.Sleep(100 * time.Millisecond)
	}
}

// waitResubscribed waits until the subscriber of c subscribed again, on a later incarnation of its
// channel than the one of guard generation gen, then until the test's queue exists with a
// consumer. Ack returns before the broker closes the channel, and the broker notices a cut
// connection a moment later, so until then the old queue is still there with its old consumer.
func waitResubscribed(t *testing.T, r *recoveryTest, c *consumption, gen uint64) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		c.sub.mu.Lock()
		st := c.sub.stream
		c.sub.mu.Unlock()
		if st != nil && st.gen > gen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the subscriber did not subscribe again")
		}
	}
	conn, err := amqp.Dial(r.url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		// A missing queue closes the channel.
		q, err := ch.QueueDeclarePassive(r.name, true, true, false, false, nil)
		if err == nil {
			_ = ch.Close()
			if q.Consumers > 0 {
				return
			}
		}
	}
	t.Fatalf("queue %s was not recovered with its consumer", r.name)
}

// Another connection takes the queue with an exclusive consumer while the connection is lost. The
// recovery cannot subscribe again, and Consume fails instead of waiting for deliveries forever.
func TestIntegrationRecoveryConsumerLost(t *testing.T) {
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{})
	c := r.consume(t, 1, 0)
	ids := r.publish(t, "before", 1)
	c.waitAcked(t, len(ids), 10*time.Second)

	r.network.setRefuse(true)
	r.network.cut()

	conn, err := amqp.Dial(r.url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The broker drops the lost connection's consumer once it notices the connection closed.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ch.Consume(r.name, "intruder", false, true, false, false, nil); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("exclusive consume: %v", err)
		}
	}
	r.network.setRefuse(false)

	select {
	case err := <-c.done:
		if !errors.Is(err, errConsumerLost) || !containsCode(err, amqp.AccessRefused) {
			t.Fatalf("Consume = %v, want errConsumerLost with ACCESS_REFUSED", err)
		}
		c.done <- err // for the cleanup
	case <-time.After(15 * time.Second):
		t.Fatal("Consume kept waiting on a consumer the recovery could not re-subscribe")
	}
}

// The recovery gives up after MaxRetries. Consume and Publish fail instead of waiting.
func TestIntegrationRecoveryGivesUp(t *testing.T) {
	r := newRecoveryTest(t, Config{Recovery: RecoveryConfig{MaxRetries: 2}}, QueueConfig{Durable: true}, SubscriberConfig{})
	c := r.consume(t, 1, 0)
	ids := r.publish(t, "before", 1)
	c.waitAcked(t, len(ids), 10*time.Second)
	pub, err := r.broker.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}

	r.network.setRefuse(true)
	r.network.cut()

	select {
	case err := <-c.done:
		if err == nil {
			t.Fatal("Consume returned nil once the recovery gave up")
		}
		c.done <- err // for the cleanup
	case <-time.After(15 * time.Second):
		t.Fatal("Consume kept waiting once the recovery gave up")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if err := pub.Publish(ctx, worker.NewMessage("m", "k", r.name, []byte(`{}`))); err == nil || ctx.Err() != nil {
		t.Fatalf("Publish once the recovery gave up = %v after %s, want an error at once", err, time.Since(start))
	}
}

// While the connection is down, a publish fails at once, so the outbox moves on, and publishes are
// confirmed again once the connection is back.
func TestIntegrationPublishFailsFastWhileDown(t *testing.T) {
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{})
	pub, err := r.broker.NewDefaultPublisher()
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	publish := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return pub.Publish(ctx, worker.NewMessage("m", "k", r.name, []byte(`{}`)))
	}

	r.network.setRefuse(true)
	r.network.cut()
	time.Sleep(100 * time.Millisecond) // the client notices the cut
	start := time.Now()
	if err := publish(); !errors.Is(err, amqp.ErrClosed) || time.Since(start) > time.Second {
		t.Fatalf("Publish while the connection is down = %v after %s, want amqp.ErrClosed at once", err, time.Since(start))
	}

	r.network.setRefuse(false)
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		err := publish()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Publish once the network is back: %v", err)
		}
	}
}
