package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// errStaleDelivery is returned when a delivery is settled after the channel that delivered it
	// was lost. The broker requeued the message when the channel closed and delivers it again.
	errStaleDelivery = errors.New("rabbitmq: delivery belongs to a lost channel; the broker redelivers it")
	// errConsumerLost is returned by Consume when the recovery could not re-subscribe the
	// subscriber's consumer. Its channel is open again but delivers nothing.
	errConsumerLost = errors.New("rabbitmq: recovery could not re-subscribe the consumer")
	// errRecoveryFailed is the loss of a consumer whose connection or channel the client could not
	// recover once the recovery had canceled the consumer.
	errRecoveryFailed = errors.New("rabbitmq: the connection or channel could not be recovered")
)

// ackGuard keeps acknowledgements of deliveries from a lost channel off its recovered successor.
//
// A recovered channel reuses the same *amqp.Channel, but delivery tags start over at 1. A late
// acknowledgement of an old delivery would therefore settle whichever new message got its tag.
// Every delivery records the generation of the channel that delivered it, and the recovery hooks
// advance the generation before the channel is reopened. The read lock spans the check and the
// acknowledgement, so no acknowledgement can pass the check and then reach the reopened channel.
type ackGuard struct {
	mu  sync.RWMutex
	gen uint64
}

func (g *ackGuard) generation() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.gen
}

// invalidate marks every delivery received so far as stale.
func (g *ackGuard) invalidate() {
	g.mu.Lock()
	g.gen++
	g.mu.Unlock()
}

// settle runs ack unless the delivery of generation gen is stale.
func (g *ackGuard) settle(gen uint64, ack func() error) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if gen != g.gen {
		return errStaleDelivery
	}
	return ack()
}

// gate holds the publishes of a publisher channel back while the client reopens it.
//
// The client resets a channel before it opens it again and puts it back in confirm mode, and lets
// frames through in between. A message published then is never confirmed, and it shifts the
// client's count of confirms by one for good: every later publish would wait for a confirm in
// vain, while the broker routes it. The recovery hooks close the gate before the client reopens
// the channel and open it once the client returns, and publish sends under the gate's read lock,
// so no publish slips into the reopening. While the channel is still closed, such as while the
// connection is down, publishes fail at once instead of waiting.
type gate struct {
	mu sync.RWMutex
	// closed counts the recoveries in progress: the hooks of a channel and of its connection can
	// overlap.
	closed int
	// opened is closed once closed is back at zero.
	opened chan struct{}
	// losses counts the losses of the channel, and reason is why it was last lost, for the
	// publishes whose confirm a loss cut short.
	losses uint64
	reason error
}

// close holds publishes back until open, for a loss of the channel for reason.
func (g *gate) close(reason error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed == 0 {
		g.opened = make(chan struct{})
	}
	g.closed++
	g.losses++
	g.reason = reason
}

// open lets publishes through again once every close is matched.
func (g *gate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed--
	if g.closed == 0 {
		close(g.opened)
	}
}

// send runs publish once the gate is open, holding it open until publish returns, and returns the
// losses counted so far, for lostSince. While the gate is closed, it fails with amqp.ErrClosed as
// long as closed reports the channel closed, as publishing would while the connection is down, and
// otherwise waits for the client to finish reopening the channel, until ctx ends. A nil gate is
// always open.
func (g *gate) send(ctx context.Context, closed func() bool, publish func() error) (uint64, error) {
	if g == nil {
		return 0, publish()
	}
	for {
		g.mu.RLock()
		if g.closed == 0 {
			defer g.mu.RUnlock()
			return g.losses, publish()
		}
		opened := g.opened
		g.mu.RUnlock()
		if closed() {
			return 0, amqp.ErrClosed
		}

		select {
		case <-opened:
		case <-ctx.Done():
			return 0, fmt.Errorf("wait for the recovery of the channel: %w", ctx.Err())
		}
	}
}

// lostSince returns why the channel was last lost, if it was lost after send returned losses.
func (g *gate) lostSince(losses uint64) error {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.losses == losses {
		return nil
	}
	return g.reason
}

// consumerLoss hands the first failure to re-subscribe a subscriber's consumer to its Consume.
type consumerLoss struct {
	once sync.Once
	done chan struct{}
	err  error
}

func newConsumerLoss() *consumerLoss {
	return &consumerLoss{done: make(chan struct{})}
}

// report records err and closes done. Later reports are ignored.
func (l *consumerLoss) report(err error) {
	l.once.Do(func() {
		l.err = fmt.Errorf("%w: %w", errConsumerLost, err)
		close(l.done)
	})
}

// reported tells whether a loss was reported, without waiting.
func (l *consumerLoss) reported() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// recoveryHooks wraps the client's connection and topology recovery. The close hooks run on the
// client's close watchers, after the lost connection or channel is closed. The client's recovery,
// which they call, reopens it and recovers the topology through RecoverTopology before it returns.
type recoveryHooks struct {
	broker   *broker
	next     amqp.ConnectionRecovery
	topology amqp.TopologyRecovery
	// reconnecting counts the connection recoveries in progress. The channels of a lost connection
	// are closed too, and their hooks leave them to the connection's.
	reconnecting atomic.Int32
}

func (h *recoveryHooks) OnConnectionClose(conn *amqp.Connection, err *amqp.Error) {
	h.reconnecting.Add(1)
	gates := h.broker.loseConnection(lossReason(err))
	h.next.OnConnectionClose(conn, err)
	h.reconnecting.Add(-1)

	for _, g := range gates {
		g.open()
	}
	// A recovered connection re-subscribed every subscriber it detached. One that gave up, or that
	// Close stopped, leaves them waiting.
	if conn == nil || conn.IsClosed() {
		h.broker.giveUp()
	}
}

// OnChannelClose recovers only the channels the broker owns. A channel closed by a failed check
// before it was handed out, such as a subscriber whose queue does not exist, stays closed.
func (h *recoveryHooks) OnChannelClose(ch *amqp.Channel, err *amqp.Error) {
	o, owned := h.broker.owner(ch)
	if !owned {
		return
	}
	if o.sub != nil {
		o.sub.lose(false)
	}
	if o.gate != nil {
		o.gate.close(lossReason(err))
	}

	h.next.OnChannelClose(ch, err)

	if o.gate != nil {
		o.gate.open()
	}
	if o.sub != nil {
		o.sub.giveUp(func() bool { return h.reconnecting.Load() > 0 })
	}
}

// RecoverTopology recovers the exchanges, queues and bindings through the client, and re-subscribes
// the subscribers of the reopened channels itself, each on a new delivery stream (see subscription).
// The client runs it once it reopened the channels.
//
// An exchange, queue or binding that fails to recover is skipped, as the client does by default,
// so the rest of the topology still recovers. Aborting instead would make the client reconnect
// again and again while every publisher and subscriber waits, without end when the failure is
// permanent, such as a deleted queue. A subscriber that cannot consume again is told, and its
// Consume fails instead of waiting forever.
func (h *recoveryHooks) RecoverTopology(conn *amqp.Connection, channels []*amqp.Channel) ([]amqp.TopologyRecoveryEntity, error) {
	subs, err := h.broker.detach(channels)
	if err != nil {
		return nil, err
	}
	skipped, err := h.topology.RecoverTopology(conn, channels)
	if err != nil {
		// The client starts over, or gives up and closes the channels. The detached subscribers
		// wait for the next attempt, or for giveUp.
		return skipped, err
	}
	h.broker.resubscribe(subs)
	return skipped, nil
}

// lossReason returns err as an error. The client passes nil for a connection closed on purpose.
func lossReason(err *amqp.Error) error {
	if err == nil {
		return amqp.ErrClosed
	}
	return err
}

// amqpRecovery returns the client recovery configuration, or nil when recovery is disabled.
func (r RecoveryConfig) amqpRecovery(hooks *recoveryHooks) *amqp.Recovery {
	if r.Disabled {
		return nil
	}
	return &amqp.Recovery{
		ReconnectionConfig: &amqp.ReconnectionConfig{
			MaxRetryCount: r.MaxRetries,
			RetryInterval: r.RetryInterval,
		},
		ConnectionRecovery: hooks,
		TopologyRecovery:   hooks,
	}
}
