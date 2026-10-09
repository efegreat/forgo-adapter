package rabbitmq

import (
	"context"
	"strconv"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	conf PublisherConfig
	ch   *amqp.Channel
}

// Publish sends msg to the exchange msg.Destination with the routing key msg.Key.
func (p *Publisher) Publish(ctx context.Context, msg worker.Message) error {
	return p.ch.PublishWithContext(
		ctx,
		msg.Destination,
		msg.Key,
		p.conf.Mandatory,
		p.conf.Immediate,
		publishing(msg, p.conf.DeliveryMode),
	)
}

func (p *Publisher) Close() error {
	return p.ch.Close()
}

// publishing converts msg to an AMQP message. The identity headers are written from the message
// fields, so the consuming side rebuilds the same ID, name and creation time.
func publishing(msg worker.Message, deliveryMode uint8) amqp.Publishing {
	headers := amqp.Table{}
	if msg.Headers != nil {
		msg.Headers.Range(func(k, v string) bool {
			headers[k] = v
			return true
		})
	}
	headers[worker.MessageIDHeader] = msg.ID
	headers[worker.MessageNameHeader] = msg.Name
	if !msg.CreatedAt.IsZero() {
		headers[worker.MessageTimestampHeader] = strconv.FormatInt(msg.CreatedAt.UnixNano(), 10)
	}

	return amqp.Publishing{
		MessageId:    msg.ID,
		Type:         msg.Name,
		Timestamp:    msg.CreatedAt,
		Headers:      headers,
		ContentType:  "application/json",
		Body:         msg.Payload,
		DeliveryMode: deliveryMode,
	}
}
