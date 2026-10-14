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

// recordingRecovery is the client recovery the hooks delegate to.
type recordingRecovery struct {
	connections int
	channels    []*amqp.Channel
}

func (r *recordingRecovery) OnConnectionClose(*amqp.Connection, *amqp.Error) { r.connections++ }

func (r *recordingRecovery) OnChannelClose(ch *amqp.Channel, _ *amqp.Error) {
	r.channels = append(r.channels, ch)
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

			d := newDelivery(amqp.Delivery{Acknowledger: ack, DeliveryTag: 7}, guard, nil)
			if err := tt.settle(d); err != nil {
				t.Fatal(err)
			}
			if len(ack.calls) != 1 || ack.calls[0] != tt.want {
				t.Fatalf("acknowledgements = %q, want %q", ack.calls, tt.want)
			}

			stale := newDelivery(amqp.Delivery{Acknowledger: ack, DeliveryTag: 7}, guard, nil)
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

func TestRecoveryHooks(t *testing.T) {
	sub, pub, unowned := new(amqp.Channel), new(amqp.Channel), new(amqp.Channel)
	guard := &ackGuard{}
	next := &recordingRecovery{}
	b := &broker{owned: map[*amqp.Channel]*ackGuard{sub: guard, pub: nil}}
	hooks := &recoveryHooks{broker: b, next: next}

	hooks.OnChannelClose(unowned, &amqp.Error{Code: 404})
	if len(next.channels) != 0 || guard.generation() != 0 {
		t.Fatal("an unowned channel was recovered")
	}

	hooks.OnChannelClose(pub, &amqp.Error{Code: 406})
	if len(next.channels) != 1 || next.channels[0] != pub || guard.generation() != 0 {
		t.Fatalf("publisher channel close: recovered %v, guard %d", next.channels, guard.generation())
	}

	hooks.OnChannelClose(sub, &amqp.Error{Code: 406})
	if len(next.channels) != 2 || guard.generation() != 1 {
		t.Fatalf("subscriber channel close: recovered %d channels, guard %d", len(next.channels), guard.generation())
	}

	hooks.OnConnectionClose(nil, &amqp.Error{Code: 320})
	if next.connections != 1 || guard.generation() != 2 {
		t.Fatalf("connection close: recoveries %d, guard %d", next.connections, guard.generation())
	}

	b.disown(sub)
	hooks.OnChannelClose(sub, &amqp.Error{Code: 406})
	if len(next.channels) != 2 {
		t.Fatal("a closed subscriber's channel was recovered")
	}
}

// reported tells whether a loss was reported, without waiting.
func reported(l *consumerLoss) bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

func TestRecoveryReportsLostConsumer(t *testing.T) {
	b := &broker{consumers: make(map[string]*consumerLoss)}
	hooks := &recoveryHooks{broker: b}
	tag, lost := b.addConsumer("billing")
	otherTag, other := b.addConsumer("billing")
	if tag == otherTag {
		t.Fatalf("two subscribers share the consumer tag %q", tag)
	}

	refused := &amqp.Error{Code: amqp.AccessRefused, Reason: "queue 'billing' in exclusive use"}
	skipped := []amqp.TopologyRecoveryEntity{
		// Only consumers are matched by tag: a queue may have any name.
		{EntityType: amqp.TopologyEntityQueue, EntityName: tag, Err: refused},
		{EntityType: amqp.TopologyEntityConsumer, EntityName: "ctag-elsewhere-1", Err: refused},
	}
	for _, e := range skipped {
		if !hooks.OnTopologyEntityError(nil, e) {
			t.Fatalf("%v aborted the recovery", e)
		}
	}
	if reported(lost) || reported(other) {
		t.Fatal("a subscriber was told about another entity")
	}

	if !hooks.OnTopologyEntityError(nil, amqp.TopologyRecoveryEntity{EntityType: amqp.TopologyEntityConsumer, EntityName: tag, Err: refused}) {
		t.Fatal("a lost consumer aborted the recovery of the rest of the topology")
	}
	if !reported(lost) || !errors.Is(lost.err, errConsumerLost) || !errors.Is(lost.err, refused) || !strings.Contains(lost.err.Error(), tag) {
		t.Fatalf("lost consumer: reported %v, err %v", reported(lost), lost.err)
	}
	if reported(other) {
		t.Fatal("another subscriber was told its consumer was lost")
	}

	// A later recovery that fails again keeps the first error.
	hooks.OnTopologyEntityError(nil, amqp.TopologyRecoveryEntity{EntityType: amqp.TopologyEntityConsumer, EntityName: tag, Err: amqp.ErrClosed})
	if errors.Is(lost.err, amqp.ErrClosed) {
		t.Fatalf("second report replaced the first: %v", lost.err)
	}

	b.removeConsumer(otherTag)
	hooks.OnTopologyEntityError(nil, amqp.TopologyRecoveryEntity{EntityType: amqp.TopologyEntityConsumer, EntityName: otherTag, Err: refused})
	if reported(other) {
		t.Fatal("a closed subscriber was told its consumer was lost")
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
		r.ConnectionRecovery != hooks || r.TopologyRecovery == nil || r.TopologyRecoveryMode != amqp.TopologyRecoveryAllEnabled ||
		r.OnTopologyEntityError == nil {
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
