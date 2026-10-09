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

func (d *Delivery) Message() worker.Message {
	return d.msg
}

func (d *Delivery) Complete(context.Context) error {
	return d.delivery.Ack(false)
}

// Retry requeues the message.
func (d *Delivery) Retry(context.Context) error {
	return d.delivery.Nack(false, true)
}

// Reject discards the message, or dead-letters it when its queue has DeadLetter enabled.
func (d *Delivery) Reject(context.Context) error {
	return d.delivery.Reject(false)
}
