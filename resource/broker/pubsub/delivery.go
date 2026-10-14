package pubsub

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"

	gpubsub "cloud.google.com/go/pubsub/v2"
)

// Delivery is the worker.IDelivery of a Pub/Sub message.
type Delivery struct {
	msg *gpubsub.Message
	wm  worker.Message
	// deadLetter publishes the message on Reject. Nil acknowledges it instead.
	deadLetter *deadLetter
}

// NewDelivery wraps a Pub/Sub message. Messages published without the identity attributes, for
// example by another producer, take their ID from the message ID Pub/Sub assigned and their
// creation time from the time Pub/Sub received them.
func NewDelivery(m *gpubsub.Message) *Delivery {
	headers := types.NewJSONMapWithData(m.Attributes)
	if _, ok := headers.Get(worker.MessageIDHeader); !ok && m.ID != "" {
		headers.Set(worker.MessageIDHeader, m.ID)
	}
	if _, ok := headers.Get(worker.MessageTimestampHeader); !ok && !m.PublishTime.IsZero() {
		headers.Set(worker.MessageTimestampHeader, strconv.FormatInt(m.PublishTime.UnixNano(), 10))
	}

	return &Delivery{
		msg: m,
		// Pub/Sub does not tell a subscriber which topic a message was published to.
		wm: worker.NewMessageFromDelivery(m.OrderingKey, "", m.Data, headers),
	}
}

// newDelivery is NewDelivery for a subscriber whose rejected messages go to deadLetter, if any.
func newDelivery(m *gpubsub.Message, deadLetter *deadLetter) *Delivery {
	d := NewDelivery(m)
	d.deadLetter = deadLetter
	return d
}

// Message returns the message, with the identity its attributes carry.
func (d *Delivery) Message() worker.Message {
	return d.wm
}

// Complete acknowledges the message. With exactly-once delivery, it waits until Pub/Sub confirmed
// the acknowledgement or ctx ends.
func (d *Delivery) Complete(ctx context.Context) error {
	return d.settle(ctx, "acknowledge", d.msg.AckWithResult())
}

// Retry makes Pub/Sub deliver the message again, after the delay of the subscription's retry
// policy, if it has one. The attempt is not used: Pub/Sub derives the delay from its own count of
// deliveries.
func (d *Delivery) Retry(ctx context.Context, _ int) error {
	return d.nack(ctx)
}

// Reject publishes the message to the dead-letter topic and acknowledges it, or only acknowledges
// it when there is no dead-letter topic. If the copy cannot be published, the message is delivered
// again instead of being acknowledged.
func (d *Delivery) Reject(ctx context.Context) error {
	if d.deadLetter == nil {
		return d.Complete(ctx)
	}
	if err := d.deadLetter.topics.publish(ctx, d.deadLetter.topic, deadLetterMessage(d)); err != nil {
		err = fmt.Errorf("pubsub: dead-letter %s to %s, delivering it again: %w", d.wm.ID, d.deadLetter.topic, err)
		return errors.Join(err, d.nack(ctx))
	}
	return d.Complete(ctx)
}

func (d *Delivery) nack(ctx context.Context) error {
	return d.settle(ctx, "nack", d.msg.NackWithResult())
}

// settle waits for the result of an acknowledgement, which is ready at once without exactly-once
// delivery.
func (d *Delivery) settle(ctx context.Context, action string, r *gpubsub.AckResult) error {
	st, err := r.Get(ctx)
	if err == nil && st != gpubsub.AcknowledgeStatusSuccess {
		err = fmt.Errorf("status %d", st)
	}
	if err != nil {
		return fmt.Errorf("pubsub: %s %s: %w", action, d.wm.ID, err)
	}
	return nil
}

// deadLetter is the topic the rejected messages of a subscriber are published to.
type deadLetter struct {
	topics *topics
	topic  string
}

// deadLetterMessage copies a delivery for the dead-letter topic. The copy keeps the data, the
// attributes and the ordering key, and carries the message ID in its identity attribute, so a
// message that arrived without one keeps the ID it was given.
func deadLetterMessage(d *Delivery) *gpubsub.Message {
	attributes := make(map[string]string, len(d.msg.Attributes)+1)
	maps.Copy(attributes, d.msg.Attributes)
	attributes[worker.MessageIDHeader] = d.wm.ID

	return &gpubsub.Message{
		Data:        d.msg.Data,
		Attributes:  attributes,
		OrderingKey: d.msg.OrderingKey,
	}
}
