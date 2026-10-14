package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// errNacked is returned when the broker refuses responsibility for a message, for example
	// because a queue it routes to is full and rejects publishes.
	errNacked = errors.New("rabbitmq: broker nacked the message")
	// errUnroutable is returned when a mandatory message matches no queue.
	errUnroutable = errors.New("rabbitmq: mandatory message is unroutable")
	// errNotConfirming is returned when the channel gives no confirmation to wait for, which it
	// only does outside confirm mode.
	errNotConfirming = errors.New("rabbitmq: publisher channel is not in confirm mode")
)

// publishChannel is the part of *amqp.Channel a Publisher uses.
type publishChannel interface {
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
	Close() error
}

// confirmation is the part of *amqp.DeferredConfirmation Publish waits on.
type confirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

// publisher publishes on a channel in confirm mode. Publish returns once the broker has taken
// responsibility for the message: written it to disk for a persistent message on a durable queue,
// or routed it. The outbox only marks a message processed after that.
type publisher struct {
	conf PublisherConfig
	ch   publishChannel
	// returns receives unroutable mandatory messages. It is nil unless conf.Mandatory is set.
	returns <-chan amqp.Return
	release func()

	// mu keeps one publish in flight per channel, so a return can be matched to its publish.
	mu sync.Mutex
}

// Publish sends msg to the exchange msg.Destination with the routing key msg.Key and waits for
// the broker's confirm until ctx ends.
func (p *publisher) Publish(ctx context.Context, msg worker.Message) error {
	return p.publish(ctx, msg.Destination, msg.Key, publishing(msg, p.conf.DeliveryMode), msg.ID)
}

// publish sends msg, whose message ID is id, and waits for the broker's confirm until ctx ends.
func (p *publisher) publish(ctx context.Context, exchange, key string, msg amqp.Publishing, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dropReturns()
	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, p.conf.Mandatory, false, msg)
	if err != nil {
		return fmt.Errorf("rabbitmq: publish %s: %w", id, err)
	}
	if dc == nil {
		return fmt.Errorf("%w: %s", errNotConfirming, id)
	}
	return p.await(ctx, dc, id)
}

// await waits for the confirm of the message with the given ID.
func (p *publisher) await(ctx context.Context, c confirmation, id string) error {
	acked, err := c.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("rabbitmq: wait for the confirm of %s: %w", id, err)
	}
	if !acked {
		return fmt.Errorf("%w: %s", errNacked, id)
	}

	// The broker sends the return of an unroutable mandatory message before its confirm, and the
	// client hands the return to the listener before it processes the confirm. So the return, if
	// any, is already buffered.
	for {
		select {
		case ret, ok := <-p.returns:
			if !ok {
				return nil
			}
			if ret.MessageId == id {
				return fmt.Errorf("%w: %s to exchange %q with routing key %q: %s", errUnroutable, id, ret.Exchange, ret.RoutingKey, ret.ReplyText)
			}
		default:
			return nil
		}
	}
}

// dropReturns discards returns of earlier publishes that stopped waiting before their confirm.
func (p *publisher) dropReturns() {
	for {
		select {
		case _, ok := <-p.returns:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// Close closes the publisher's channel.
func (p *publisher) Close() error {
	if p.release != nil {
		p.release()
	}
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
