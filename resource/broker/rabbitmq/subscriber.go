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

type Subscriber struct {
	conf    SubscriberConfig
	ch      consumeChannel
	guard   *ackGuard
	release func()
}

// Consume sets the prefetch, subscribes to the queue and hands every delivery to state.Handler,
// with at most state.Concurrency in flight. It returns once ctx ends and every handler finished.
func (s *Subscriber) Consume(ctx context.Context, state worker.ConsumerState) error {
	if err := s.ch.Qos(prefetch(s.conf.Prefetch, state.Concurrency), 0, false); err != nil {
		return fmt.Errorf("rabbitmq: subscriber %s: set prefetch: %w", s.conf.Name, err)
	}
	msgs, err := s.ch.Consume(
		s.conf.Queue,
		"",
		s.conf.AutoAck,
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
		case msg, ok := <-msgs:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return errDeliveriesClosed
			}
			d := newDelivery(msg, s.guard)
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

func (s *Subscriber) Close() error {
	if s.release != nil {
		s.release()
	}
	return s.ch.Close()
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
