package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// errDeliveriesClosed reports that the delivery channel closed while consuming: the broker
// cancelled the consumer, or the connection was lost and recovery is disabled or gave up.
var errDeliveriesClosed = errors.New("rabbitmq: delivery channel closed by the broker")

// consumeChannel is the part of *amqp.Channel a Subscriber uses.
type consumeChannel interface {
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Close() error
}

// subscriber is the worker.ISubscriber of a SubscriberConfig. It consumes on a channel of its own.
type subscriber struct {
	conf SubscriberConfig
	ch   consumeChannel
	// tag is the consumer tag, under which the recovery reports a failed re-subscription to lost.
	tag     string
	guard   *ackGuard
	lost    *consumerLoss
	retry   *retrier
	release func()
}

// Consume sets the prefetch, subscribes to the queue and hands every delivery to state.Handler,
// with at most state.Concurrency in flight. It returns once ctx ends and every handler finished,
// or with an error once the consumer is gone: the delivery channel closed, or the recovery could
// not re-subscribe the consumer.
func (s *subscriber) Consume(ctx context.Context, state worker.ConsumerState) error {
	if err := s.ch.Qos(prefetch(s.conf.Prefetch, state.Concurrency), 0, false); err != nil {
		return fmt.Errorf("rabbitmq: subscriber %s: set prefetch: %w", s.conf.Name, err)
	}
	msgs, err := s.ch.Consume(
		s.conf.Queue,
		s.tag,
		false, // the worker settles every message
		s.conf.Exclusive,
		s.conf.NoLocal,
		s.conf.NoWait,
		s.conf.Args,
	)
	if err != nil {
		return fmt.Errorf("rabbitmq: subscriber %s: consume queue %s: %w", s.conf.Name, s.conf.Queue, err)
	}

	runner := process.NewConcurrentRunner(process.NewChannelSemaphore(state.Concurrency))
	defer runner.Wait()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.lost.done:
			return s.lost.err
		case msg, ok := <-msgs:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return errDeliveriesClosed
			}
			d := newDelivery(msg, s.guard, s.retry)
			err := runner.Run(ctx, func() {
				state.Handler(ctx, d)
			})
			if err != nil {
				// The message was not handled or acknowledged: the broker redelivers it once the
				// channel closes.
				if errors.Is(err, process.ErrNoSlot) {
					return nil
				}
				return err
			}
		}
	}
}

// Close closes the subscriber's channel, and the publisher of its queue's retry queues.
func (s *subscriber) Close() error {
	if s.release != nil {
		s.release()
	}
	err := s.ch.Close()
	if s.retry != nil {
		err = errors.Join(err, s.retry.pub.Close())
	}
	return err
}

// prefetch returns the prefetch count for a subscriber: configured when positive, the worker's
// concurrency when zero, and no limit (0 for RabbitMQ) when negative.
func prefetch(configured, concurrency int) int {
	switch {
	case configured > 0:
		return configured
	case configured < 0:
		return 0
	default:
		return min(max(concurrency, 1), maxPrefetch)
	}
}
