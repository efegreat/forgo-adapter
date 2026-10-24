package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// recordingRecovery is the client recovery the hooks delegate to. during, when set, runs in its
// place, while the client would reopen the connection or channel.
type recordingRecovery struct {
	connections int
	channels    []*amqp.Channel
	during      func()
}

func (r *recordingRecovery) OnConnectionClose(*amqp.Connection, *amqp.Error) {
	r.connections++
	if r.during != nil {
		r.during()
	}
}

func (r *recordingRecovery) OnChannelClose(ch *amqp.Channel, _ *amqp.Error) {
	r.channels = append(r.channels, ch)
	if r.during != nil {
		r.during()
	}
}

// recordingTopology is the client's topology recovery, which records its calls in log and fails
// with err.
type recordingTopology struct {
	log *callLog
	err error
}

func (r *recordingTopology) RecoverTopology(_ *amqp.Connection, channels []*amqp.Channel) ([]amqp.TopologyRecoveryEntity, error) {
	r.log.add(fmt.Sprintf("recover %d channels", len(channels)))
	return nil, r.err
}

// fakeAcknowledger records the acknowledgements sent for a delivery.
type fakeAcknowledger struct {
	mu    sync.Mutex
	calls []string
}

func (a *fakeAcknowledger) record(call string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, call)
}

func (a *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	a.record(fmt.Sprintf("ack %d multiple=%v", tag, multiple))
	return nil
}

func (a *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	a.record(fmt.Sprintf("nack %d multiple=%v requeue=%v", tag, multiple, requeue))
	return nil
}

func (a *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	a.record(fmt.Sprintf("reject %d requeue=%v", tag, requeue))
	return nil
}

func TestAckGuard(t *testing.T) {
	var g ackGuard
	gen := g.generation()

	called := 0
	if err := g.settle(gen, func() error { called++; return nil }); err != nil || called != 1 {
		t.Fatalf("settle current = %v, calls %d", err, called)
	}

	g.invalidate()
	if err := g.settle(gen, func() error { called++; return nil }); !errors.Is(err, errStaleDelivery) || called != 1 {
		t.Fatalf("settle stale = %v, calls %d; want errStaleDelivery and no acknowledgement", err, called)
	}
	if err := g.settle(g.generation(), func() error { called++; return nil }); err != nil || called != 2 {
		t.Fatalf("settle after recovery = %v, calls %d", err, called)
	}
}

func TestAckGuardInvalidateWaitsForInFlightAck(t *testing.T) {
	var g ackGuard
	gen := g.generation()

	acking := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = g.settle(gen, func() error {
			close(acking)
			<-release
			return nil
		})
	}()
	<-acking

	invalidated := make(chan struct{})
	go func() {
		g.invalidate()
		close(invalidated)
	}()

	select {
	case <-invalidated:
		t.Fatal("invalidate returned while an acknowledgement was in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-invalidated:
	case <-time.After(time.Second):
		t.Fatal("invalidate did not return after the acknowledgement finished")
	}
}

func TestDeliveryAcknowledgements(t *testing.T) {
	tests := []struct {
		name   string
		settle func(*delivery) error
		want   string
	}{
		{name: "complete", settle: func(d *delivery) error { return d.Complete(context.Background()) }, want: "ack 7 multiple=false"},
		{name: "retry", settle: func(d *delivery) error { return d.Retry(context.Background(), 1) }, want: "nack 7 multiple=false requeue=true"},
		{name: "reject", settle: func(d *delivery) error { return d.Reject(context.Background()) }, want: "reject 7 requeue=false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack := &fakeAcknowledger{}
			guard := &ackGuard{}

			d := newDelivery(amqp.Delivery{Acknowledger: ack, DeliveryTag: 7}, guard, guard.generation(), nil)
			if err := tt.settle(d); err != nil {
				t.Fatal(err)
			}
			if len(ack.calls) != 1 || ack.calls[0] != tt.want {
				t.Fatalf("acknowledgements = %q, want %q", ack.calls, tt.want)
			}

			stale := newDelivery(amqp.Delivery{Acknowledger: ack, DeliveryTag: 7}, guard, guard.generation(), nil)
			guard.invalidate()
			if err := tt.settle(stale); !errors.Is(err, errStaleDelivery) {
				t.Fatalf("settle after recovery = %v, want errStaleDelivery", err)
			}
			if len(ack.calls) != 1 {
				t.Fatalf("a stale delivery reached the channel: %q", ack.calls)
			}
		})
	}
}

// gateState reports whether g holds publishes back, without waiting.
func gateState(g *gate) string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.closed > 0 {
		return "closed"
	}
	return "open"
}

func TestRecoveryHooks(t *testing.T) {
	subCh, pubCh, topologyCh, unowned := new(amqp.Channel), new(amqp.Channel), new(amqp.Channel), new(amqp.Channel)
	fake := &fakeConsumeChannel{}
	sub := newSubscription(SubscriberConfig{Name: "s", Queue: "q"}, fake, tagsOf("s"))
	pubGate := &gate{}
	next := &recordingRecovery{}
	b := &broker{owned: map[*amqp.Channel]ownedChannel{
		subCh:      {sub: sub},
		pubCh:      {gate: pubGate},
		topologyCh: {},
	}}
	hooks := &recoveryHooks{broker: b, next: next}

	hooks.OnChannelClose(unowned, &amqp.Error{Code: 404})
	if len(next.channels) != 0 {
		t.Fatal("an unowned channel was recovered")
	}

	var during string
	next.during = func() { during = gateState(pubGate) }
	hooks.OnChannelClose(pubCh, &amqp.Error{Code: 404, Reason: "NOT_FOUND"})
	if len(next.channels) != 1 || next.channels[0] != pubCh || during != "closed" || gateState(pubGate) != "open" {
		t.Fatalf("publisher channel close: recovered %v, gate %s while reopened and %s after", next.channels, during, gateState(pubGate))
	}
	if lost := pubGate.lostSince(0); lost == nil || !strings.Contains(lost.Error(), "NOT_FOUND") {
		t.Fatalf("the gate recorded the loss as %v", lost)
	}
	next.during = nil

	fake.setClosed(true)
	hooks.OnChannelClose(subCh, &amqp.Error{Code: 406})
	if len(next.channels) != 2 || sub.guard.generation() != 1 {
		t.Fatalf("subscriber channel close: recovered %d channels, guard %d", len(next.channels), sub.guard.generation())
	}

	// The hook of a channel that its connection's recovery already reopened leaves it alone.
	fake.setClosed(false)
	hooks.OnChannelClose(subCh, &amqp.Error{Code: 320})
	if sub.guard.generation() != 1 {
		t.Fatal("a late hook marked the reopened channel's deliveries stale")
	}

	hooks.OnChannelClose(topologyCh, &amqp.Error{Code: 406})
	if len(next.channels) != 4 {
		t.Fatal("the topology channel was not recovered")
	}

	next.during = func() { during = gateState(pubGate) }
	hooks.OnConnectionClose(nil, &amqp.Error{Code: 320})
	if next.connections != 1 || sub.guard.generation() != 2 || during != "closed" || gateState(pubGate) != "open" {
		t.Fatalf("connection close: recoveries %d, guard %d, gate %s while reconnecting and %s after", next.connections, sub.guard.generation(), during, gateState(pubGate))
	}

	// The close of a reconnection attempt that failed once it dialed comes after a later attempt
	// reconnected, while the connection is open.
	hooks.OnConnectionClose(new(amqp.Connection), &amqp.Error{Code: amqp.FrameError})
	if next.connections != 1 || sub.guard.generation() != 2 || pubGate.lostSince(2) != nil {
		t.Fatalf("close of an open connection: recoveries %d, guard %d, gate loss %v", next.connections, sub.guard.generation(), pubGate.lostSince(2))
	}

	b.disown(subCh)
	hooks.OnChannelClose(subCh, &amqp.Error{Code: 406})
	if len(next.channels) != 4 {
		t.Fatal("a closed subscriber's channel was recovered")
	}
}

// consumingSubscription returns a subscription that Consume reads, on a fake channel recording its
// calls in log.
func consumingSubscription(t *testing.T, log *callLog) (*subscription, *fakeConsumeChannel) {
	t.Helper()
	ch := &fakeConsumeChannel{log: log}
	s := newSubscription(SubscriberConfig{Name: "s", Queue: "q"}, ch, tagsOf("s"))
	if _, err := s.start(); err != nil {
		t.Fatal(err)
	}
	return s, ch
}

func TestRecoverTopology(t *testing.T) {
	log := &callLog{}
	subCh, pubCh := new(amqp.Channel), new(amqp.Channel)
	sub, fake := consumingSubscription(t, log)
	b := &broker{owned: map[*amqp.Channel]ownedChannel{subCh: {sub: sub}, pubCh: {gate: &gate{}}}}
	topology := &recordingTopology{log: log}
	hooks := &recoveryHooks{broker: b, topology: topology}

	if _, err := hooks.RecoverTopology(nil, []*amqp.Channel{subCh, pubCh}); err != nil {
		t.Fatal(err)
	}
	// The client recovers no consumer of its own: the subscriber is subscribed again once the rest of
	// the topology is back, on a new stream.
	want := "consume forgo-s-1, cancel forgo-s-1, recover 2 channels, consume forgo-s-1"
	if got := log.String(); got != want {
		t.Fatalf("calls %q, want %q", got, want)
	}
	if len(fake.streams) != 2 || sub.stream == nil || sub.replacing || sub.lost.reported() {
		t.Fatalf("after the recovery: %d streams, replacing %v, lost %v", len(fake.streams), sub.replacing, sub.lost.reported())
	}
}

func TestRecoverTopologyFailures(t *testing.T) {
	errDown := errors.New("connection lost")

	t.Run("cancel failed", func(t *testing.T) {
		subCh := new(amqp.Channel)
		sub, fake := consumingSubscription(t, nil)
		fake.cancelErr = errDown
		topology := &recordingTopology{log: &callLog{}}
		hooks := &recoveryHooks{broker: &broker{owned: map[*amqp.Channel]ownedChannel{subCh: {sub: sub}}}, topology: topology}

		if _, err := hooks.RecoverTopology(nil, []*amqp.Channel{subCh}); !errors.Is(err, errDown) {
			t.Fatalf("RecoverTopology = %v, want the cancellation's error", err)
		}
		if topology.log.String() != "" {
			t.Fatal("the topology was recovered with the client's consumer still in place")
		}
	})

	t.Run("topology failed, then recovered", func(t *testing.T) {
		log := &callLog{}
		subCh := new(amqp.Channel)
		sub, _ := consumingSubscription(t, log)
		topology := &recordingTopology{log: log, err: errDown}
		hooks := &recoveryHooks{broker: &broker{owned: map[*amqp.Channel]ownedChannel{subCh: {sub: sub}}}, topology: topology}

		if _, err := hooks.RecoverTopology(nil, []*amqp.Channel{subCh}); !errors.Is(err, errDown) {
			t.Fatalf("RecoverTopology = %v", err)
		}
		if !sub.replacing {
			t.Fatal("the subscription does not wait for the next attempt")
		}
		topology.err = nil
		if _, err := hooks.RecoverTopology(nil, []*amqp.Channel{subCh}); err != nil {
			t.Fatal(err)
		}
		want := "consume forgo-s-1, cancel forgo-s-1, recover 1 channels, recover 1 channels, consume forgo-s-1"
		if got := log.String(); got != want {
			t.Fatalf("calls %q, want %q", got, want)
		}
	})

	t.Run("gave up", func(t *testing.T) {
		subCh := new(amqp.Channel)
		sub, _ := consumingSubscription(t, nil)
		b := &broker{owned: map[*amqp.Channel]ownedChannel{subCh: {sub: sub}}}
		hooks := &recoveryHooks{broker: b, next: &recordingRecovery{}, topology: &recordingTopology{err: errDown}}
		if _, err := hooks.RecoverTopology(nil, []*amqp.Channel{subCh}); err == nil {
			t.Fatal("RecoverTopology succeeded")
		}

		// While its connection is being recovered, the hook of the channel leaves the subscription
		// waiting for the connection's recovery.
		hooks.reconnecting.Add(1)
		hooks.OnChannelClose(subCh, &amqp.Error{Code: 320})
		if sub.lost.reported() {
			t.Fatal("the channel's hook gave up during its connection's recovery")
		}
		hooks.reconnecting.Add(-1)

		// The connection could not be recovered.
		hooks.OnConnectionClose(nil, &amqp.Error{Code: 320})
		if !sub.lost.reported() || !errors.Is(sub.lost.err, errRecoveryFailed) {
			t.Fatalf("a subscription that the recovery could not subscribe again was not told: %v", sub.lost.err)
		}
	})
}

func TestRedeclareAutoDeleteQueue(t *testing.T) {
	cfg := Config{Queues: []QueueConfig{
		{Name: "durable", Durable: true},
		{Name: "temporary", Durable: true, AutoDelete: true, Bindings: []BindingConfig{{Exchange: "shop", RoutingKey: "order.*"}}},
	}}.Sanitize()
	var r recorder
	b := &broker{conf: cfg, topology: &r}

	for _, queue := range []string{"durable", "declared elsewhere"} {
		if err := b.redeclare(queue); err != nil || len(r.calls) != 0 {
			t.Fatalf("redeclare(%s) = %v, declared %v", queue, err, r.calls)
		}
	}
	if err := b.redeclare("temporary"); err != nil {
		t.Fatal(err)
	}
	want := []string{"queue temporary durable=true args=map[]", "bind temporary shop order.*"}
	if strings.Join(r.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("redeclared %q, want %q", r.calls, want)
	}
}

// reopening reports a channel that the client is reopening, not closed.
func reopening() bool { return false }

func TestGate(t *testing.T) {
	var g gate
	sent := 0
	send := func() error { sent++; return nil }

	if losses, err := g.send(context.Background(), reopening, send); err != nil || losses != 0 || sent != 1 {
		t.Fatalf("send through an open gate = %d, %v, %d sends", losses, err, sent)
	}

	// While the channel is closed, a publish fails at once, as the client's would.
	g.close(amqp.ErrClosed)
	if _, err := g.send(context.Background(), func() bool { return true }, send); !errors.Is(err, amqp.ErrClosed) || sent != 1 {
		t.Fatalf("send while the channel is closed = %v, %d sends", err, sent)
	}
	g.open()

	// The hooks of a channel and of its connection overlap.
	g.close(amqp.ErrClosed)
	g.close(&amqp.Error{Code: 320, Reason: "CONNECTION_FORCED"})
	g.open()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := g.send(ctx, reopening, send); !errors.Is(err, context.DeadlineExceeded) || sent != 1 {
		t.Fatalf("send through a gate closed twice and opened once = %v, %d sends", err, sent)
	}

	done := make(chan uint64, 1)
	go func() {
		losses, _ := g.send(context.Background(), reopening, send)
		done <- losses
	}()
	g.open()
	select {
	case losses := <-done:
		if losses != 3 || sent != 2 {
			t.Fatalf("send once the gate opened: losses %d, %d sends", losses, sent)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("send kept waiting once the gate opened")
	}

	if g.lostSince(3) != nil {
		t.Fatal("a loss was reported without one")
	}
	if lost := g.lostSince(2); lost == nil || !strings.Contains(lost.Error(), "CONNECTION_FORCED") {
		t.Fatalf("lostSince = %v, want the last loss", lost)
	}

	var nilGate *gate
	if _, err := nilGate.send(context.Background(), reopening, send); err != nil || sent != 3 || nilGate.lostSince(0) != nil {
		t.Fatal("a nil gate is not open")
	}
}

// A publish in flight holds the gate open: the client cannot reset the channel under it.
func TestGateCloseWaitsForSend(t *testing.T) {
	var g gate
	sending, release := make(chan struct{}), make(chan struct{})
	go func() {
		_, _ = g.send(context.Background(), reopening, func() error {
			close(sending)
			<-release
			return nil
		})
	}()
	<-sending

	closed := make(chan struct{})
	go func() {
		g.close(amqp.ErrClosed)
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("the gate closed while a publish was being sent")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the gate did not close once the publish was sent")
	}
}

func TestConsumerTag(t *testing.T) {
	if got := consumerTag("billing", 3); got != "forgo-billing-3" {
		t.Fatalf("consumerTag = %q", got)
	}
	if got := consumerTag(strings.Repeat("n", 250), 3); got != "forgo-3" {
		t.Fatalf("consumerTag of a long name = %q, want forgo-3", got)
	}
	if got := consumerTag(strings.Repeat("n", 247), 3); len(got) != maxConsumerTag {
		t.Fatalf("consumerTag at the limit = %d bytes, want %d", len(got), maxConsumerTag)
	}
}

func TestRecoveryConfig(t *testing.T) {
	got := Config{}.Sanitize().Recovery
	if got.MaxRetries != math.MaxInt || got.RetryInterval != 5*time.Second || got.Disabled {
		t.Fatalf("default Recovery = %+v", got)
	}

	hooks := &recoveryHooks{}
	r := RecoveryConfig{MaxRetries: 3, RetryInterval: time.Second}.Sanitize().amqpRecovery(hooks)
	if r == nil || r.ReconnectionConfig.MaxRetryCount != 3 || r.ReconnectionConfig.RetryInterval != time.Second ||
		r.ConnectionRecovery != hooks || r.TopologyRecovery != hooks || r.TopologyRecoveryMode != amqp.TopologyRecoveryAllEnabled {
		t.Fatalf("amqpRecovery = %+v", r)
	}

	if r := (RecoveryConfig{Disabled: true}).Sanitize().amqpRecovery(hooks); r != nil {
		t.Fatalf("disabled amqpRecovery = %+v, want nil", r)
	}
}

func TestConfigValidateLimits(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{name: "negative retries", cfg: Config{Recovery: RecoveryConfig{MaxRetries: -1}}, wantErr: "cannot be negative"},
		{name: "negative interval", cfg: Config{Recovery: RecoveryConfig{RetryInterval: -time.Second}}, wantErr: "cannot be negative"},
		{name: "prefetch too large", cfg: Config{Subscribers: []SubscriberConfig{{Name: "s", Prefetch: 70000}}}, wantErr: "Prefetch cannot exceed 65535"},
		{name: "prefetch at limit", cfg: Config{Subscribers: []SubscriberConfig{{Name: "s", Prefetch: 65535}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Sanitize().validate()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("validate: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("validate err = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}
