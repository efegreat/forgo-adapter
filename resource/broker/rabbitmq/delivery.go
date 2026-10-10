package rabbitmq

import (
	"context"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Delivery struct {
	delivery amqp.Delivery
	msg      worker.Message
	// guard and gen drop acknowledgements once the channel that delivered the message was lost.
	guard *ackGuard
	gen   uint64
	// retry delays retries through the retry queues of the delivery's queue. Nil requeues at once.
	retry *retrier
}

// NewDelivery wraps an AMQP delivery. Only string headers are kept. Messages published without
// the identity headers, for example by another producer, take their ID from the message_id
// property and their name from the type property.
func NewDelivery(delivery amqp.Delivery) *Delivery {
	headers := types.NewJSONMap[string]()
	for k, v := range delivery.Headers {
		if s, ok := v.(string); ok {
			headers.Set(k, s)
		}
	}
	if _, ok := headers.Get(worker.MessageIDHeader); !ok && delivery.MessageId != "" {
		headers.Set(worker.MessageIDHeader, delivery.MessageId)
	}
	if _, ok := headers.Get(worker.MessageNameHeader); !ok && delivery.Type != "" {
		headers.Set(worker.MessageNameHeader, delivery.Type)
	}

	return &Delivery{
		delivery: delivery,
		msg:      worker.NewMessageFromDelivery(delivery.RoutingKey, delivery.Exchange, delivery.Body, headers),
	}
}

// newDelivery is NewDelivery for a subscriber channel guarded against recovery, whose queue has the
// retry queues of retry, if any.
func newDelivery(delivery amqp.Delivery, guard *ackGuard, retry *retrier) *Delivery {
	d := NewDelivery(delivery)
	d.guard = guard
	d.gen = guard.generation()
	d.retry = retry
	return d
}

func (d *Delivery) Message() worker.Message {
	return d.msg
}

func (d *Delivery) Complete(context.Context) error {
	return d.settle(func() error { return d.delivery.Ack(false) })
}

// Retry redelivers the message after the delay of the retry queue for attempt when its queue has
// Retry, and at once otherwise.
func (d *Delivery) Retry(ctx context.Context, attempt int) error {
	if d.retry == nil {
		return d.requeue()
	}
	return d.retry.retry(ctx, d, attempt)
}

// requeue returns the message to its queue, which redelivers it at once.
func (d *Delivery) requeue() error {
	return d.settle(func() error { return d.delivery.Nack(false, true) })
}

// Reject discards the message, or dead-letters it when its queue has DeadLetter enabled.
func (d *Delivery) Reject(context.Context) error {
	return d.settle(func() error { return d.delivery.Reject(false) })
}

// stale reports whether the channel that delivered the message was lost since it arrived.
func (d *Delivery) stale() bool {
	return d.guard != nil && d.guard.generation() != d.gen
}

// settle sends an acknowledgement unless the delivery's channel was lost since it arrived.
func (d *Delivery) settle(ack func() error) error {
	if d.guard == nil {
		return ack()
	}
	return d.guard.settle(d.gen, ack)
}
