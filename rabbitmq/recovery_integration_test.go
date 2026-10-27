package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// faultyNetwork dials the broker for the recovery tests, and fails like a network: cut closes every
// connection it made and cutPublishing those of the publishing connection, while it refuses, dials
// fail, while it hangs up, dials of the subscribers' connection succeed and the other end closes
// the connection at once, and once it stalls, nothing gets through either way, as with a broker
// that hangs or a network that drops every packet. While it slows the publishing connection, each
// read of that connection waits first, so a round trip takes at least as long, and the confirms
// of the publishes in flight then arrive together.
type faultyNetwork struct {
	mu     sync.Mutex
	conns  []dialedConn
	refuse bool
	// hangUps counts the dials still to hang up, and hungUp those hung up so far.
	hangUps, hungUp int
	// dialed counts the connections made.
	dialed  int
	stalled atomic.Bool
	// publishDelay is how long a read of the publishing connection waits, in nanoseconds.
	publishDelay atomic.Int64
}

// dialedConn is a connection the network made, for the publishing connection or the other one.
type dialedConn struct {
	net.Conn
	publishing bool
}

func (n *faultyNetwork) dial(publishing bool, network, addr string) (net.Conn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.refuse {
		return nil, errors.New("network unreachable")
	}
	if n.hangUps > 0 && !publishing {
		n.hangUps--
		n.hungUp++
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	}
	tcp, err := net.DialTimeout(network, addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	conn := &stallableConn{Conn: tcp, stalled: &n.stalled}
	if publishing {
		conn.delay = &n.publishDelay
	}
	n.conns = append(n.conns, dialedConn{Conn: conn, publishing: publishing})
	n.dialed++
	return conn, nil
}

// waitDialed waits until the network made n connections in all.
func (n *faultyNetwork) waitDialed(t *testing.T, dials int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		n.mu.Lock()
		dialed := n.dialed
		n.mu.Unlock()
		if dialed >= dials {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections made, want %d", dialed, dials)
		}
	}
}

// stall lets nothing through anymore, on the connections made so far and the next ones.
func (n *faultyNetwork) stall() {
	n.stalled.Store(true)
}

// slowPublishing makes every read of the publishing connections, made so far and later, wait for
// delay first.
func (n *faultyNetwork) slowPublishing(delay time.Duration) {
	n.publishDelay.Store(int64(delay))
}

// stallableConn drops what is written to it and what it reads while its network stalls. A read
// still ends with the connection's deadline or close. A read waits for delay first, when set.
type stallableConn struct {
	net.Conn
	stalled *atomic.Bool
	delay   *atomic.Int64
}

func (c *stallableConn) Read(b []byte) (int, error) {
	if c.delay != nil {
		time.Sleep(time.Duration(c.delay.Load()))
	}
	for {
		n, err := c.Conn.Read(b)
		if err != nil || !c.stalled.Load() {
			return n, err
		}
	}
}

func (c *stallableConn) Write(b []byte) (int, error) {
	if c.stalled.Load() {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

// cut closes the connections, as a lost network would.
func (n *faultyNetwork) cut() {
	n.cutWhere(func(dialedConn) bool { return true })
}

// cutPublishing closes the connections of the publishing connection only.
func (n *faultyNetwork) cutPublishing() {
	n.cutWhere(func(c dialedConn) bool { return c.publishing })
}

func (n *faultyNetwork) cutWhere(cut func(dialedConn) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	kept := n.conns[:0]
	for _, c := range n.conns {
		if cut(c) {
			_ = c.Close()
		} else {
			kept = append(kept, c)
		}
	}
	n.conns = kept
}

func (n *faultyNetwork) setRefuse(refuse bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.refuse = refuse
}

// hangUp hangs up the next dials of the subscribers' connection, as a load balancer does while the
// broker behind it is down.
func (n *faultyNetwork) hangUp(dials int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.hangUps = dials
}

// hungUpCount returns the number of dials hung up so far.
func (n *faultyNetwork) hungUpCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.hungUp
}

// recoveryTest is a broker on the RabbitMQ of integrationEnv, which recovers at once, with a queue
// of its own bound to a fanout exchange of the same name.
type recoveryTest struct {
	url     string
	name    string
	network *faultyNetwork
	broker  *broker
	// pub is the broker's default publisher, opened with the broker, as a producer's are.
	pub worker.IPublisher
}

// recoveryTests numbers the recovery tests, so that their names differ: the clock of some systems,
// such as macOS, gives the parallel ones the same time, and two tests on one queue take each
// other's messages.
var recoveryTests atomic.Uint64

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

	name := fmt.Sprintf("forgo-recovery-%s-%d", strconv.FormatInt(time.Now().UnixNano(), 36), recoveryTests.Add(1))
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
	pub, err := b.NewDefaultPublisher()
	if err != nil {
		_ = b.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = pub.Close()
		_ = b.Close()
		deleteTopology(t, url, cfg.Sanitize())
	})
	return &recoveryTest{url: url, name: name, network: n, broker: b, pub: pub}
}

// publish publishes n messages to the test's exchange through the test's publisher, with the IDs
// prefix-0 to prefix-n-1, and returns the IDs. As the outbox would, it publishes a message again
// until the broker confirms it: while the publishing connection is down, which outlasts the
// recovery of the subscribers' connection, or while the broker nacks it, as the publisher's channel
// can do right after a recovery once it learns that the deleted auto-delete queue of the same name
// went away.
func (r *recoveryTest) publish(t *testing.T, prefix string, n int) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-%d", prefix, i)
		msg := worker.NewMessage("m", "k", r.name, []byte(`{}`))
		msg.ID = ids[i]
		for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := r.pub.Publish(ctx, msg)
			cancel()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Publish %s: %v", msg.ID, err)
			}
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

// The publishers have a connection of their own, so the subscribers keep consuming, and their
// deliveries stay current, while it is lost and cannot reconnect.
func TestIntegrationPublishingConnectionLost(t *testing.T) {
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{})
	ids := r.publish(t, "before", 200)
	c := r.consume(t, 1, 2*time.Millisecond)
	c.waitAcked(t, 20, 10*time.Second)

	gen := c.sub.guard.generation()
	r.network.setRefuse(true)
	r.network.cutPublishing()
	c.waitAcked(t, len(ids), 15*time.Second)
	if !r.broker.publishConn.IsClosed() || r.broker.conn.IsClosed() {
		t.Fatalf("publishing connection closed %v, subscribers' connection closed %v, want only the first", r.broker.publishConn.IsClosed(), r.broker.conn.IsClosed())
	}
	if c.sub.guard.generation() != gen {
		t.Fatal("the loss of the publishing connection marked the deliveries stale")
	}

	r.network.setRefuse(false)
	ids = append(ids, r.publish(t, "after", 20)...)
	c.waitAcked(t, len(ids), 10*time.Second)
	c.checkAll(t, ids, 0)
}

// A retry waits for the publishing connection to come back, within its context, instead of
// requeueing the delivery at once, so its handler does not run again until the retry queue returns
// the message.
func TestIntegrationRetryWaitsForPublishingConnection(t *testing.T) {
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true, Retry: RetryConfig{Initial: 100 * time.Millisecond, Max: 100 * time.Millisecond}}, SubscriberConfig{})
	r.publish(t, "msg", 1)
	s, err := r.broker.NewSubscriber("sub")
	if err != nil {
		t.Fatal(err)
	}
	type handled struct {
		retried bool // the message came back from a retry queue, which adds x-death
		err     error
	}
	results := make(chan handled, 4)
	var runs atomic.Int32
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(ctx, worker.ConsumerState{Concurrency: 1, Handler: func(ctx context.Context, d worker.IDelivery) {
			_, retried := d.Message().Headers.Get("x-death")
			if runs.Add(1) > 1 {
				results <- handled{retried: retried, err: d.Complete(ctx)}
				return
			}
			r.network.setRefuse(true)
			r.network.cutPublishing()
			for r.broker.publishConn != nil && !r.broker.publishConn.IsClosed() {
				time.Sleep(time.Millisecond)
			}
			time.AfterFunc(500*time.Millisecond, func() { r.network.setRefuse(false) })
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			results <- handled{err: d.Retry(rctx, 1)}
		}})
	}()
	t.Cleanup(func() {
		stop()
		<-done
		_ = s.Close()
	})

	for i, want := range []string{"the retry", "the delivery from the retry queue"} {
		select {
		case h := <-results:
			if h.err != nil {
				t.Fatalf("%s failed: %v", want, h.err)
			}
			if i == 1 && !h.retried {
				t.Fatal("the message came back without going through the retry queue: it was requeued")
			}
		case err := <-done:
			t.Fatalf("Consume returned %v before %s", err, want)
		case <-time.After(15 * time.Second):
			t.Fatalf("no %s", want)
		}
	}
	if n := runs.Load(); n != 2 {
		t.Fatalf("the handler ran %d times, want 2", n)
	}
}

// RabbitMQ's own dead-lettering drops a message whose dead-letter queue is deleted or full. The
// subscriber publishes the copy itself instead, and while the copy cannot be stored, it requeues the
// delivery, or parks it in the last retry queue when the queue has retry queues, so that it does not
// keep the subscriber's handlers from the other messages: the message comes back, is rejected again,
// and is acknowledged once its copy is stored.
func TestIntegrationDeadLetterNotStored(t *testing.T) {
	deleted := func(ch *amqp.Channel, dlq, _, _ string) error {
		_, err := ch.QueueDelete(dlq, false, false, false)
		return err
	}
	redeclared := func(ch *amqp.Channel, dlq, dlx, key string) error {
		if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
			return err
		}
		return ch.QueueBind(dlq, key, dlx, false, nil)
	}
	tests := []struct {
		name string
		// args are the dead-letter queue's, and retry the queue's.
		args  map[string]any
		retry RetryConfig
		// rejects is the number of messages the handler rejects, and others the number it
		// completes, published after them.
		rejects, others int
		// breakIt makes the dead-letter queue refuse copies, and repair lets it store them again.
		breakIt, repair func(ch *amqp.Channel, dlq, dlx, key string) error
	}{
		{name: "dead-letter queue deleted", rejects: 1, breakIt: deleted, repair: redeclared},
		{
			name:    "dead-letter queue full",
			args:    map[string]any{"x-max-length": 1, "x-overflow": "reject-publish"},
			rejects: 2,
			breakIt: func(*amqp.Channel, string, string, string) error { return nil }, // the first copy fills it
			repair: func(ch *amqp.Channel, dlq, _, _ string) error {
				_, err := ch.QueuePurge(dlq, false)
				return err
			},
		},
		{
			name:    "dead-letter queue deleted, with retry queues",
			retry:   RetryConfig{Initial: 50 * time.Millisecond, Max: 100 * time.Millisecond},
			rejects: 3, others: 200,
			breakIt: deleted, repair: redeclared,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true, DeadLetter: DeadLetterConfig{Enabled: true, Args: tt.args}, Retry: tt.retry}, SubscriberConfig{})
			dlq, dlx := r.name+".dlq", r.name+".dlx"
			conn, err := amqp.Dial(r.url)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ch, err := conn.Channel()
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.breakIt(ch, dlq, dlx, r.name); err != nil {
				t.Fatal(err)
			}

			var failed, rejected, completed, parked atomic.Int32
			s, err := r.broker.NewSubscriber("sub")
			if err != nil {
				t.Fatal(err)
			}
			ctx, stop := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- s.Consume(ctx, worker.ConsumerState{Concurrency: 1, Handler: func(ctx context.Context, d worker.IDelivery) {
					if strings.HasPrefix(d.Message().ID, "other-") {
						if d.Complete(ctx) == nil {
							completed.Add(1)
						}
						return
					}
					if _, ok := d.Message().Headers.Get("x-death"); ok {
						parked.Add(1) // it came back through a retry queue
					}
					if err := d.Reject(ctx); err != nil {
						failed.Add(1)
						return
					}
					rejected.Add(1)
				}})
			}()
			t.Cleanup(func() {
				stop()
				<-done
				_ = s.Close()
			})
			ids := r.publish(t, "msg", tt.rejects)
			r.publish(t, "other", tt.others)

			// A copy that cannot be stored brings its message back, again and again, and the
			// other messages are handled meanwhile.
			for deadline := time.Now().Add(15 * time.Second); failed.Load() < int32(3*tt.rejects) || completed.Load() < int32(tt.others); time.Sleep(10 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("%d rejections failed, %d went through, and %d of %d other messages were completed while the dead-letter queue refused copies", failed.Load(), rejected.Load(), completed.Load(), tt.others)
				}
			}
			if tt.retry.Initial > 0 && parked.Load() == 0 {
				t.Fatal("the rejected messages came back without going through a retry queue: they were requeued")
			}
			if err := tt.repair(ch, dlq, dlx, r.name); err != nil {
				t.Fatal(err)
			}
			for deadline := time.Now().Add(15 * time.Second); rejected.Load() < int32(tt.rejects); time.Sleep(10 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("%d of %d messages dead-lettered once the dead-letter queue was repaired", rejected.Load(), tt.rejects)
				}
			}
			last := getMessage(t, r.url, dlq)
			if !slices.Contains(ids, last.MessageId) {
				t.Fatalf("the dead-letter queue holds %q, want one of %v", last.MessageId, ids)
			}
			if queued := r.queued(t); queued != 0 {
				t.Fatalf("the queue still holds %d messages", queued)
			}
			t.Logf("%d rejections failed before the copies were stored, %d of them parked", failed.Load(), parked.Load())
		})
	}
}

// A SIGTERM can come while the broker is unreachable. The subscribers, the publishers and the broker
// then close without failing, within closeTimeout each, whether the connections are down and being
// recovered, the broker does not answer, or the client is reconnecting to a broker that does not
// answer.
func TestIntegrationCloseWhileUnreachable(t *testing.T) {
	tests := []struct {
		name  string
		fault func(*testing.T, *faultyNetwork)
	}{
		{name: "connections down", fault: func(_ *testing.T, n *faultyNetwork) { n.setRefuse(true); n.cut() }},
		{name: "broker not answering", fault: func(_ *testing.T, n *faultyNetwork) { n.stall() }},
		{
			// Both connections reconnect into a broker that does not answer, and wait for its
			// handshake, which the dialer of the test bounds by no deadline.
			name: "reconnecting to a broker not answering",
			fault: func(t *testing.T, n *faultyNetwork) {
				n.mu.Lock()
				dialed := n.dialed
				n.mu.Unlock()
				n.stall()
				n.cut()
				n.waitDialed(t, dialed+2)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{})
			s, err := r.broker.NewSubscriber("sub")
			if err != nil {
				t.Fatal(err)
			}
			handled := make(chan struct{}, 1)
			ctx, stop := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- s.Consume(ctx, worker.ConsumerState{Concurrency: 1, Handler: func(ctx context.Context, d worker.IDelivery) {
					_ = d.Complete(ctx)
					handled <- struct{}{}
				}})
			}()
			r.publish(t, "msg", 1)
			<-handled

			tt.fault(t, r.network)
			time.Sleep(100 * time.Millisecond)
			stop()
			if err := <-done; err != nil {
				t.Fatalf("Consume = %v", err)
			}
			for _, c := range []struct {
				what  string
				close func() error
			}{
				{"subscriber", s.Close},
				{"publisher", r.pub.Close},
				{"broker", r.broker.Close},
			} {
				start := time.Now()
				err := c.close()
				if limit := closeTimeout + time.Second; err != nil || time.Since(start) > limit {
					t.Errorf("closing the %s = %v after %s, want nil within %s", c.what, err, time.Since(start).Round(time.Millisecond), limit)
				}
			}
		})
	}
}

// Reconnection attempts fail after they dialed, as through a load balancer while the broker behind
// it is down. The client hands the close of each one to the recovery hooks once a later attempt
// reconnected, and the subscriber keeps consuming on the stream it subscribed on.
func TestIntegrationRecoveryAfterHangUps(t *testing.T) {
	const hangUps = 3
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{})
	c := r.consume(t, 1, 0)
	ids := r.publish(t, "before", 1)
	c.waitAcked(t, len(ids), 10*time.Second)

	gen := c.sub.guard.generation()
	r.network.hangUp(hangUps)
	r.network.cut()
	waitResubscribed(t, r, c, gen)
	if n := r.network.hungUpCount(); n != hangUps {
		t.Fatalf("%d dials hung up, want %d", n, hangUps)
	}

	ids = append(ids, r.publish(t, "after", 20)...)
	c.waitAcked(t, len(ids), 10*time.Second)
	c.checkAll(t, ids, 0)
}

// serverAtLeast reports whether the test's RabbitMQ is at least version major.minor, and returns
// its version.
func (r *recoveryTest) serverAtLeast(major, minor int) (string, bool) {
	version, _ := r.broker.conn.Properties["version"].(string)
	var gotMajor, gotMinor int
	if _, err := fmt.Sscanf(version, "%d.%d", &gotMajor, &gotMinor); err != nil {
		return version, false
	}
	return version, gotMajor > major || gotMajor == major && gotMinor >= minor
}

// From RabbitMQ 4.3 on, a quorum queue cancels a consumer whose delivery outlives the consumer
// timeout and returns that message to the queue, while the consumer's other deliveries stay with
// it. The subscriber subscribes again: the message is delivered again while its first handler still
// runs, a delivery that had not timed out is acknowledged by its handler after the cancellation, and
// Consume goes on.
func TestIntegrationConsumerTimeout(t *testing.T) {
	const timeout = 2 * time.Second
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true, Args: map[string]any{
		"x-queue-type":       "quorum",
		"x-consumer-timeout": timeout.Milliseconds(),
	}}, SubscriberConfig{})
	if version, ok := r.serverAtLeast(4, 3); !ok {
		t.Skipf("RabbitMQ %s closes the channel of a consumer that timed out, which the channel's recovery covers", version)
	}

	s, err := r.broker.NewSubscriber("sub")
	if err != nil {
		t.Fatal(err)
	}
	ch := s.(*subscriber).ch.(*amqp.Channel)
	cancels := ch.NotifyCancel(make(chan string, 4))
	closes := ch.NotifyClose(make(chan *amqp.Error, 4))

	type handled struct {
		id  string
		err error
	}
	results := make(chan handled, 32)
	var slowRuns atomic.Int32
	slowStarted, redelivered, cancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Consume(ctx, worker.ConsumerState{Concurrency: 3, Handler: func(ctx context.Context, d worker.IDelivery) {
			id := d.Message().ID
			switch {
			case id == "slow-0" && slowRuns.Add(1) == 1:
				close(slowStarted)
				<-redelivered
			case id == "held-0":
				<-cancelled
			}
			results <- handled{id: id, err: d.Complete(ctx)}
		}})
	}()
	// release lets the handlers waiting for c go on. Only the test closes these channels.
	release := func(c chan struct{}) {
		select {
		case <-c:
		default:
			close(c)
		}
	}
	t.Cleanup(func() {
		release(cancelled)
		release(redelivered)
		stop()
		<-done
		_ = s.Close()
	})

	// wait waits for the handlers of want to complete their deliveries, in any order.
	count := map[string]int{}
	wait := func(want ...string) {
		t.Helper()
		pending := map[string]int{}
		for _, id := range want {
			pending[id]++
		}
		for range want {
			select {
			case h := <-results:
				if pending[h.id] == 0 || h.err != nil {
					t.Fatalf("%s completed with %v, want one of %v", h.id, h.err, pending)
				}
				pending[h.id]--
				count[h.id]++
			case err := <-done:
				done <- err // for the cleanup
				t.Fatalf("Consume returned %v while %v were due", err, pending)
			case <-time.After(3 * timeout):
				t.Fatalf("%v were not handled", pending)
			}
		}
	}

	r.publish(t, "slow", 1)
	<-slowStarted
	// Delivered later than slow-0, it times out later too.
	time.Sleep(timeout / 2)
	r.publish(t, "held", 1)
	heldDue := time.Now().Add(timeout)

	var cancelledTag string
	select {
	case cancelledTag = <-cancels:
	case <-time.After(3 * timeout):
		t.Fatal("the broker did not cancel the consumer")
	}
	release(cancelled)
	wait("held-0", "slow-0")
	release(redelivered)
	wait("slow-0")
	ids := r.publish(t, "after", 5)
	wait(ids...)

	// The broker would deliver held-0 again once it timed out, had the acknowledgement after the
	// cancellation not settled it.
	time.Sleep(time.Until(heldDue) + timeout/2)
	select {
	case h := <-results:
		t.Fatalf("%s was handled again", h.id)
	case err := <-closes:
		t.Fatalf("the broker closed the channel: %v", err)
	default:
	}
	if count["slow-0"] != 2 || count["held-0"] != 1 {
		t.Fatalf("handled %v, want slow-0 twice and held-0 once", count)
	}
	if queued := r.queued(t); queued != 0 {
		t.Fatalf("the queue still holds %d messages", queued)
	}
	// A quorum queue keeps a consumer that timed out, which could be delivered to again into the
	// stream the client dropped, until the subscriber cancels it: its tag stays in use meanwhile, and
	// consuming under it would close the channel with NOT_ALLOWED.
	if _, err := ch.Consume(r.name, cancelledTag, false, false, false, false, nil); err != nil {
		t.Fatalf("the broker still holds the consumer %s it cancelled: %v", cancelledTag, err)
	}
	if err := ch.Cancel(cancelledTag, false); err != nil {
		t.Fatal(err)
	}
}

// The broker cancels the consumer of a queue that is deleted. The subscriber cannot consume it again,
// and Consume fails with the broker's NOT_FOUND instead of waiting for deliveries forever.
func TestIntegrationQueueDeleted(t *testing.T) {
	r := newRecoveryTest(t, Config{}, QueueConfig{Durable: true}, SubscriberConfig{})
	c := r.consume(t, 1, 0)
	ids := r.publish(t, "before", 1)
	c.waitAcked(t, len(ids), 10*time.Second)

	conn, err := amqp.Dial(r.url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueueDelete(r.name, false, false, false); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-c.done:
		if !errors.Is(err, errConsumerLost) || !containsCode(err, amqp.NotFound) {
			t.Fatalf("Consume = %v, want errConsumerLost with NOT_FOUND", err)
		}
		c.done <- err // for the cleanup
	case <-time.After(15 * time.Second):
		t.Fatal("Consume kept waiting on a deleted queue")
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
	ids = append(ids, r.publish(t, "after-channel", 1)...)
	c.waitAcked(t, len(ids), 10*time.Second)

	gen = c.sub.guard.generation()
	r.network.cut()
	waitResubscribed(t, r, c, gen)
	ids = append(ids, r.publish(t, "after-connection", 1)...)
	c.waitAcked(t, len(ids), 10*time.Second)

	c.checkAll(t, ids, 1)
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
