package rabbitmq

import (
	"context"
	"errors"

	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// errDeliveriesClosed reports that the broker closed the consumer, for example because the
// connection was lost. The subscriber stops; it does not reconnect.
var errDeliveriesClosed = errors.New("rabbitmq: delivery channel closed by the broker")

type Subscriber struct {
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
}

func (s *Subscriber) Consume(ctx context.Context, state worker.ConsumerState) error {
	runner := process.NewConcurrentRunner(process.NewChannelSemaphore(state.Concurrency))
	defer runner.Wait()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-s.msgs:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return errDeliveriesClosed
			}
			err := runner.Run(ctx, func() {
				state.Handler(ctx, NewDelivery(msg))
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
	return s.ch.Close()
}
