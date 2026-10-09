package rabbitmq

import (
	"errors"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// errStaleDelivery is returned when a delivery is settled after the channel that delivered it was
// lost. The broker requeued the message when the channel closed and delivers it again.
var errStaleDelivery = errors.New("rabbitmq: delivery belongs to a lost channel; the broker redelivers it")

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

// amqpRecovery returns the client recovery configuration, or nil when recovery is disabled.
func (r RecoveryConfig) amqpRecovery(hooks amqp.ConnectionRecovery) *amqp.Recovery {
	if r.Disabled {
		return nil
	}
	return &amqp.Recovery{
		ReconnectionConfig: &amqp.ReconnectionConfig{
			MaxRetryCount: r.MaxRetries,
			RetryInterval: r.RetryInterval,
		},
		ConnectionRecovery: hooks,
		TopologyRecovery:   &amqp.DefaultTopologyRecovery{},
	}
}
