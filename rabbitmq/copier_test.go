package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// pendingConfirmation is a confirmation that settles once settle is called.
type pendingConfirmation struct {
	done chan struct{}
	ack  bool
}

func newPendingConfirmation() *pendingConfirmation {
	return &pendingConfirmation{done: make(chan struct{})}
}

func (c *pendingConfirmation) settle(ack bool) {
	c.ack = ack
	close(c.done)
}

func (c *pendingConfirmation) WaitContext(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.done:
		return c.ack, nil
	}
}

// testRoom is the room of the copiers and their return listeners in tests.
const testRoom = 64

// testCopier returns a copier on a publisher of ch, whose returns come from returns, and which lets
// as many copies in flight as returns has room for.
func testCopier(ch publishChannel, returns chan amqp.Return) *copier {
	return newCopier(&publisher{ch: ch, returns: returns, gate: &gate{}})
}

// awaitCopy waits for the confirm of the copy k as publish does, holding its room.
func awaitCopy(c *copier, conf confirmation, k returnKey) error {
	c.room <- struct{}{}
	return c.await(context.Background(), conf, k, 0)
}

// A copier lets as many copies in flight as its return listener has room for, so that the listener
// never fills up.
func TestCopierRoom(t *testing.T) {
	for _, n := range []int{1, 7, testRoom} {
		if c := testCopier(nil, make(chan amqp.Return, n)); cap(c.room) != n {
			t.Errorf("a copier whose listener has room for %d returns lets %d copies in flight", n, cap(c.room))
		}
	}
}

func TestCopierAwait(t *testing.T) {
	k := returnKey{id: "id", exchange: "", key: "orders.retry.1s"}
	notFound := &amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no queue"}
	tests := []struct {
		name    string
		conf    settledConfirmation
		returns []amqp.Return
		closed  bool
		lost    error
		wantErr error
		wantMsg string
		// kept counts the copies whose returns the copy keeps for them.
		kept int
	}{
		{name: "confirmed", conf: settledConfirmation{ack: true}},
		{name: "nacked", conf: settledConfirmation{ack: false}, wantErr: errNacked},
		{name: "channel lost", conf: settledConfirmation{ack: false}, lost: notFound, wantErr: errChannelLost, wantMsg: "id: Exception (404)"},
		{
			name:    "returned unroutable",
			conf:    settledConfirmation{ack: true},
			returns: []amqp.Return{{MessageId: "id", Exchange: "", RoutingKey: "orders.retry.1s", ReplyText: "NO_ROUTE"}},
			wantErr: errUnroutable,
			wantMsg: `id to exchange "" with routing key "orders.retry.1s": NO_ROUTE`,
		},
		{
			name: "returns of other copies",
			conf: settledConfirmation{ack: true},
			returns: []amqp.Return{
				{MessageId: "other", RoutingKey: "orders.retry.1s"},
				{MessageId: "id", Exchange: "orders.dlx", RoutingKey: "orders.retry.1s"},
				{MessageId: "id", RoutingKey: "orders.retry.2s"},
			},
			kept: 3,
		},
		{name: "returns closed", conf: settledConfirmation{ack: true}, closed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returns := make(chan amqp.Return, testRoom)
			for _, r := range tt.returns {
				returns <- r
			}
			if tt.closed {
				close(returns)
			}
			c := testCopier(nil, returns)
			if tt.lost != nil {
				c.p.gate.close(tt.lost)
				c.p.gate.open()
			}

			err := awaitCopy(c, tt.conf, k)
			if tt.wantErr == nil && err != nil {
				t.Fatalf("await = %v", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("await = %v, want %v", err, tt.wantErr)
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("await = %v, want one containing %q", err, tt.wantMsg)
			}
			if len(c.room) != 0 {
				t.Fatal("the copy kept its room")
			}
			if len(returns) != 0 || len(c.returned) != tt.kept {
				t.Fatalf("%d returns left in the listener, %d copies' kept, want %d: %v", len(returns), len(c.returned), tt.kept, c.returned)
			}
		})
	}
}

// A copy takes the returns of the other copies with its own, and leaves them for them. Copies alike
// in message ID, exchange and routing key share theirs.
func TestCopierLeavesReturnsForTheirCopies(t *testing.T) {
	returns := make(chan amqp.Return, testRoom)
	c := testCopier(nil, returns)
	a := returnKey{id: "a", key: "q.retry.1s"}
	b := returnKey{id: "b", key: "q.retry.1s"}
	for _, k := range []returnKey{a, b, b} {
		returns <- amqp.Return{MessageId: k.id, Exchange: k.exchange, RoutingKey: k.key, ReplyText: "NO_ROUTE"}
	}

	confirmed := settledConfirmation{ack: true}
	if err := awaitCopy(c, confirmed, a); !errors.Is(err, errUnroutable) {
		t.Fatalf("a = %v, want errUnroutable", err)
	}
	if len(returns) != 0 || len(c.returned[b]) != 2 {
		t.Fatalf("a left %d returns in the listener and %v for b", len(returns), c.returned)
	}
	for i := range 2 {
		if err := awaitCopy(c, confirmed, b); !errors.Is(err, errUnroutable) {
			t.Fatalf("b, copy %d = %v, want errUnroutable", i, err)
		}
	}
	if err := awaitCopy(c, confirmed, b); err != nil {
		t.Fatalf("a third b, which nothing returned = %v", err)
	}
	if len(c.returned) != 0 {
		t.Fatalf("returns left: %v", c.returned)
	}
}

// A copy that stops waiting for its confirm keeps its room until the confirm comes, and then takes
// its return, so that it holds no room in the listener and no later copy alike takes it.
func TestCopierStoppedWaiting(t *testing.T) {
	returns := make(chan amqp.Return, testRoom)
	c := testCopier(nil, returns)
	k := returnKey{id: "id", key: "q.retry.1s"}
	conf := newPendingConfirmation()

	c.room <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.await(ctx, conf, k, 0); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wait for the confirm of id") {
		t.Fatalf("await = %v, want the context's error", err)
	}
	time.Sleep(10 * time.Millisecond)
	if len(c.room) != 1 {
		t.Fatal("a copy freed its room before its confirm")
	}

	returns <- amqp.Return{MessageId: k.id, RoutingKey: k.key}
	conf.settle(true)
	for deadline := time.Now().Add(5 * time.Second); len(c.room) != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the copy kept its room once confirmed")
		}
	}
	if err := awaitCopy(c, settledConfirmation{ack: true}, k); err != nil {
		t.Fatalf("the next copy alike = %v: it took the return of the copy that stopped waiting", err)
	}
}

// A confirmed copy frees its room only once it took the returns: a copy that took its room earlier
// could have its return handed to a listener that still holds the earlier copy's, and the client
// holds the connection back while the listener is full.
func TestCopierFreesRoomAfterTakingReturns(t *testing.T) {
	c := testCopier(nil, make(chan amqp.Return, testRoom))
	first := newPendingConfirmation()
	c.room <- struct{}{}
	done := make(chan error, 1)
	go func() { done <- c.await(context.Background(), first, returnKey{id: "first"}, 0) }()
	for range testRoom - 1 {
		c.room <- struct{}{}
	}

	// The confirmed copy cannot take the returns while the test holds the copier's lock.
	c.mu.Lock()
	first.settle(true)
	entered := make(chan struct{})
	go func() {
		c.room <- struct{}{}
		close(entered)
	}()
	select {
	case <-entered:
		c.mu.Unlock()
		t.Fatal("a copy took the room of a copy that had not taken its returns yet")
	case <-time.After(20 * time.Millisecond):
	}
	c.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-entered
}

// publish waits for room among the copies in flight until its context ends, and frees its room on
// every failure.
func TestCopierPublishFailures(t *testing.T) {
	t.Run("no room", func(t *testing.T) {
		ch := &recordingPublishChannel{}
		c := testCopier(ch, make(chan amqp.Return, testRoom))
		for range testRoom {
			c.room <- struct{}{}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		err := c.publish(ctx, "", "q", amqp.Publishing{}, "id")
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wait for the copies in flight") || ch.publishes.Load() != 0 {
			t.Fatalf("publish without room = %v after %d sends", err, ch.publishes.Load())
		}
	})

	for _, tt := range []struct {
		name    string
		ch      *recordingPublishChannel
		blocked bool
		wantErr error
	}{
		{name: "send fails", ch: &recordingPublishChannel{err: amqp.ErrClosed}, wantErr: amqp.ErrClosed},
		{name: "not confirming", ch: &recordingPublishChannel{notConfirming: true}, wantErr: errNotConfirming},
		{name: "blocked", ch: &recordingPublishChannel{}, blocked: true, wantErr: errBlocked},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testCopier(tt.ch, make(chan amqp.Return, testRoom))
			if tt.blocked {
				c.p.flow = newFlow()
				c.p.flow.block("low on memory")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if err := c.publish(ctx, "dlx", "orders", amqp.Publishing{MessageId: "id"}, "id"); !errors.Is(err, tt.wantErr) {
				t.Fatalf("publish = %v, want %v", err, tt.wantErr)
			}
			if len(c.room) != 0 {
				t.Fatal("a failed copy kept its room")
			}
			if !tt.blocked && (!tt.ch.mandatory || tt.ch.exchange != "dlx" || tt.ch.key != "orders") {
				t.Fatalf("sent %+v, want a mandatory copy to dlx with routing key orders", tt.ch)
			}
		})
	}
}

// Many copies at once each learn whether they were returned, from a broker that returns copies in
// batches before it confirms them. The listener never holds more returns than the copier has room
// for, so it never fills up.
func TestCopierConcurrentCopies(t *testing.T) {
	const copies = 4 * testRoom
	returns := make(chan amqp.Return, testRoom)
	c := testCopier(nil, returns)

	type sent struct {
		k        returnKey
		conf     *pendingConfirmation
		returned bool
	}
	sends := make(chan sent)
	brokerDone := make(chan struct{})
	go func() {
		defer close(brokerDone)
		var batch []sent
		flush := func() {
			for _, s := range batch {
				if s.returned {
					select {
					case returns <- amqp.Return{MessageId: s.k.id, RoutingKey: s.k.key}:
					default:
						t.Error("the return listener is full")
					}
				}
			}
			for _, s := range batch {
				s.conf.settle(true)
			}
			batch = batch[:0]
		}
		for {
			select {
			case s, ok := <-sends:
				if !ok {
					flush()
					return
				}
				batch = append(batch, s)
				if rand.IntN(16) == 0 {
					flush()
				}
			case <-time.After(2 * time.Millisecond):
				flush()
			}
		}
	}()

	errs := make([]error, copies)
	var wg sync.WaitGroup
	for i := range copies {
		wg.Go(func() {
			// Copies i and i+copies/2 are alike, and go the same way.
			s := sent{k: returnKey{id: fmt.Sprintf("copy-%d", i%(copies/2)), key: "q.retry.1s"}, conf: newPendingConfirmation(), returned: i%2 == 1}
			c.room <- struct{}{}
			sends <- s
			errs[i] = c.await(context.Background(), s.conf, s.k, 0)
		})
	}
	wg.Wait()
	close(sends)
	<-brokerDone

	for i, err := range errs {
		switch {
		case i%2 == 0 && err != nil:
			t.Errorf("copy %d = %v", i, err)
		case i%2 == 1 && !errors.Is(err, errUnroutable):
			t.Errorf("copy %d = %v, want errUnroutable", i, err)
		}
	}
	if len(c.room) != 0 || len(c.returned) != 0 || len(returns) != 0 {
		t.Fatalf("%d copies in flight, returns kept %v, %d in the listener", len(c.room), c.returned, len(returns))
	}
}
