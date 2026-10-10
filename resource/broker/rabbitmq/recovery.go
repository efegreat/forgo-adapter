package rabbitmq

import (
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// errStaleDelivery is returned when a delivery is settled after the channel that delivered it
	// was lost. The broker requeued the message when the channel closed and delivers it again.
	errStaleDelivery = errors.New("rabbitmq: delivery belongs to a lost channel; the broker redelivers it")
	// errConsumerLost is returned by Consume when the recovery could not re-subscribe the
	// subscriber's consumer. Its channel is open again but delivers nothing.
	errConsumerLost = errors.New("rabbitmq: recovery could not re-subscribe the consumer")
)

// ackGuard keeps acknowledgements of deliveries from a lost channel off its recovered successor.
//
// A recovered channel reuses the same *amqp.Channel, but delivery tags start over at 1. A late
// acknowledgement of an old delivery would therefore settle whichever new message got its tag.
// Every delivery records the guard's generation, and the recovery hooks advance it before the
// channel is reopened. The read lock spans the check and the acknowledgement, so no
// acknowledgement can pass the check and then reach the reopened channel.
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

// recoveryHooks wraps the client's connection recovery. It runs on the client's close watchers,
// after the lost channel is closed and before next reopens it.
type recoveryHooks struct {
	broker *Broker
	next   amqp.ConnectionRecovery
}

func (h *recoveryHooks) OnConnectionClose(conn *amqp.Connection, err *amqp.Error) {
	h.broker.invalidateAll()
	h.next.OnConnectionClose(conn, err)
}

// OnChannelClose recovers only the channels the broker owns. A channel closed by a failed check
// before it was handed out, such as a subscriber whose queue does not exist, stays closed.
func (h *recoveryHooks) OnChannelClose(ch *amqp.Channel, err *amqp.Error) {
	guard, owned := h.broker.owner(ch)
	if !owned {
		return
	}
	if guard != nil {
		guard.invalidate()
	}
	h.next.OnChannelClose(ch, err)
}

// OnTopologyEntityError skips an entity the recovery fails to restore, as the client does by
// default, so the rest of the topology still recovers. Aborting instead would make the client
// reconnect again and again while every publisher and subscriber waits, without end when the
// failure is permanent, such as a deleted queue.
//
// A skipped consumer leaves its delivery channel open with nothing behind it, so its subscriber
// is told, and its Consume fails instead of waiting forever.
func (h *recoveryHooks) OnTopologyEntityError(_ *amqp.Connection, e amqp.TopologyRecoveryEntity) bool {
	if e.EntityType == amqp.TopologyEntityConsumer {
		h.broker.consumerLost(e.EntityName, e)
	}
	return true
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
		ConnectionRecovery:    hooks,
		TopologyRecovery:      &amqp.DefaultTopologyRecovery{},
		OnTopologyEntityError: hooks.OnTopologyEntityError,
	}
}
