package rabbitmq

import (
	"context"
	"errors"
	"fmt"
)

// deadLetter publishes the copies of a queue's rejected deliveries to its dead-letter exchange.
//
// RabbitMQ's own dead-lettering, which basic.reject relies on, is at most once: it drops a message
// whose dead-letter queue is deleted, full or on a node that is down, and nobody learns of it,
// while the inbox already marked the message failed or rejected. The subscriber publishes the copy
// itself instead, with a confirm, and acknowledges the delivery only once the broker has the copy.
type deadLetter struct {
	pub      confirmedPublisher
	exchange string
	key      string
	// retry parks the deliveries whose copy failed, when the queue has retry queues.
	retry *retrier
}

// reject publishes a copy of d to the dead-letter exchange, under the dead-letter routing key, and
// acknowledges d once the broker confirmed the copy. If the copy cannot be published, d is parked
// to be rejected again later. A crash between the confirm and the acknowledgement stores the dead
// letter twice.
func (l *deadLetter) reject(ctx context.Context, d *delivery) error {
	// The broker redelivers a stale delivery itself: a copy would duplicate it.
	if d.stale() {
		return errStaleDelivery
	}
	err := l.pub.publish(ctx, l.exchange, l.key, copyPublishing(d), d.msg.ID)
	if err == nil {
		return d.settle(func() error { return d.delivery.Ack(false) })
	}
	return l.park(ctx, d, fmt.Errorf("rabbitmq: dead-letter %s to %s: %w", d.msg.ID, l.exchange, err))
}

// park returns d, whose copy failed with failed, to be rejected again from its inbox status. A
// dead-letter queue can refuse copies for as long as nobody repairs it, and a delivery requeued at
// once comes back at once, ahead of the queue's other messages: a few such messages would take
// every handler of the subscriber, again and again. So when the queue has retry queues, d waits in
// the last one, and only without them, or when that copy fails too, is it requeued.
func (l *deadLetter) park(ctx context.Context, d *delivery, failed error) error {
	if l.retry == nil {
		return errors.Join(fmt.Errorf("%w; requeuing it", failed), d.requeue())
	}
	q := l.retry.queues[len(l.retry.queues)-1]
	if err := l.retry.pub.publish(ctx, "", q.name, copyPublishing(d), d.msg.ID); err != nil {
		return errors.Join(fmt.Errorf("%w; parking it in %s: %w; requeuing it", failed, q.name, err), d.requeue())
	}
	return errors.Join(fmt.Errorf("%w; parked in %s until it is rejected again", failed, q.name), d.settle(func() error { return d.delivery.Ack(false) }))
}
