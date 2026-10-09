package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// confirmedPublisher publishes a message and waits for the broker's confirm. *Publisher is one.
type confirmedPublisher interface {
	publish(ctx context.Context, exchange, key string, msg amqp.Publishing, id string) error
	Close() error
}

// retrier delays the retries of a queue's deliveries through its retry queues.
type retrier struct {
	pub    confirmedPublisher
	queues []retryQueue // by increasing delay
}

// queue returns the retry queue for a failed attempt: the first one for attempt 1, and for 0, a
// failure before the attempt was counted, then the next one for every later attempt, up to the last.
func (r *retrier) queue(attempt int) retryQueue {
	return r.queues[min(max(attempt-1, 0), len(r.queues)-1)]
}

// retry publishes a copy of d to the retry queue for attempt and acknowledges d once the broker
// confirmed the copy. If the copy cannot be published, d is requeued at once instead, so the
// message is neither lost nor held. A crash between the confirm and the acknowledgement delivers
// the message twice, which the inbox absorbs.
func (r *retrier) retry(ctx context.Context, d *Delivery, attempt int) error {
	// The broker redelivers a stale delivery itself: a copy would duplicate it.
	if d.stale() {
		return errStaleDelivery
	}
	q := r.queue(attempt)
	if err := r.pub.publish(ctx, "", q.name, retryPublishing(d), d.msg.ID); err != nil {
		err = fmt.Errorf("rabbitmq: delay the retry of %s through %s, requeuing it at once: %w", d.msg.ID, q.name, err)
		return errors.Join(err, d.requeue())
	}
	return d.settle(func() error { return d.delivery.Ack(false) })
}

// retryPublishing copies a delivery for its retry queue. The copy is persistent and carries the
// message ID in its identity header and property, so a message that arrived without an ID keeps the
// one it was given, and a return of the copy is matched to its publish. Expiration and UserId are
// dropped: the retry queue decides when the copy expires, and RabbitMQ checks UserId against the
// user of the connection.
func retryPublishing(d *Delivery) amqp.Publishing {
	src := d.delivery
	headers := make(amqp.Table, len(src.Headers)+1)
	maps.Copy(headers, src.Headers)
	headers[worker.MessageIDHeader] = d.msg.ID

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     src.ContentType,
		ContentEncoding: src.ContentEncoding,
		DeliveryMode:    amqp.Persistent,
		Priority:        src.Priority,
		CorrelationId:   src.CorrelationId,
		ReplyTo:         src.ReplyTo,
		MessageId:       d.msg.ID,
		Timestamp:       src.Timestamp,
		Type:            src.Type,
		AppId:           src.AppId,
		Body:            src.Body,
	}
}
