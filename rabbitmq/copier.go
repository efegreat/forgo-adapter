package rabbitmq

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// maxCopiesInFlight bounds the copies a subscriber's copier waits for the confirms of at once, and
// is the room of its return listener.
const maxCopiesInFlight = 64

// returnKey tells the copies of a copier apart, as the broker's returns tell them.
type returnKey struct {
	id, exchange, key string
}

// copier publishes the copies of a subscriber's deliveries, for its retry queues and its
// dead-letter exchange, on one publisher channel, and waits for the confirms of up to
// maxCopiesInFlight copies at once.
//
// A publisher holds one message in flight, from publish to confirm, so that a return can be matched
// to it. The handlers of a subscriber whose messages fail copy at once, and would wait in line for
// one confirm after the other, each holding its slot. The copier takes turns only to send a copy,
// since the client allows no two sends on a channel at once, and then waits for its confirm
// alongside the others'. It opens no channel of its own: the client recovers the channels of a
// connection one after the other, and must not be asked for a channel while it does.
//
// The copies are mandatory, and the broker returns a copy that no queue takes before it confirms
// it. A copy takes, once confirmed, every return the client handed to the listener so far, keeps
// those of the other copies for them, and claims its own by message ID, exchange and routing key:
// copies alike in all three go the same way. The client holds the connection back while the
// listener is full, and a broker can return every copy in flight before it confirms any, so a copy
// keeps its room until it took the returns, also when it stopped waiting for its confirm. At most
// maxCopiesInFlight returns are then ever held.
type copier struct {
	p *publisher
	// returns is p's return listener, with room for maxCopiesInFlight returns.
	returns <-chan amqp.Return
	// room holds a token for each copy in flight.
	room chan struct{}
	// sending serializes the sends.
	sending sync.Mutex

	mu sync.Mutex
	// returned holds the returns taken from the listener for the copies yet to claim them, by
	// copy, as their reply texts.
	returned map[returnKey][]string
}

// newCopier returns the copier of p, a mandatory publisher whose return listener has room for
// maxCopiesInFlight returns.
func newCopier(p *publisher) *copier {
	return &copier{
		p:        p,
		returns:  p.returns,
		room:     make(chan struct{}, maxCopiesInFlight),
		returned: make(map[returnKey][]string),
	}
}

// publish publishes msg, whose message ID is id, mandatory, and waits for the broker's confirm
// until ctx ends.
func (c *copier) publish(ctx context.Context, exchange, key string, msg amqp.Publishing, id string) error {
	select {
	case c.room <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("rabbitmq: publish %s: wait for the copies in flight: %w", id, ctx.Err())
	}
	if err := c.p.flow.wait(ctx); err != nil {
		<-c.room
		return fmt.Errorf("rabbitmq: publish %s: %w", id, err)
	}
	var dc *amqp.DeferredConfirmation
	losses, err := c.p.gate.send(ctx, c.p.waitsForRecovery, c.p.ch.IsClosed, func() (err error) {
		c.sending.Lock()
		defer c.sending.Unlock()
		dc, err = c.p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, true, false, msg)
		return err
	})
	if err != nil {
		<-c.room
		return fmt.Errorf("rabbitmq: publish %s: %w", id, err)
	}
	if dc == nil {
		<-c.room
		return fmt.Errorf("%w: %s", errNotConfirming, id)
	}
	return c.await(ctx, dc, returnKey{id: id, exchange: exchange, key: key}, losses)
}

// await waits for the confirm of the copy k, sent once the channel had been lost losses times,
// until ctx ends, and frees the copy's room once it took the returns. A copy that stops waiting
// takes them once its confirm comes, which it does at the latest when the client reopens or
// closes the channel.
func (c *copier) await(ctx context.Context, conf confirmation, k returnKey, losses uint64) error {
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		go func() {
			_, _ = conf.WaitContext(context.Background())
			c.claim(k)
			<-c.room
		}()
		return fmt.Errorf("rabbitmq: wait for the confirm of %s: %w", k.id, err)
	}
	reason, returned := c.claim(k)
	<-c.room
	if !acked {
		if lost := c.p.gate.lostSince(losses); lost != nil {
			return fmt.Errorf("%w: %s: %w", errChannelLost, k.id, lost)
		}
		return fmt.Errorf("%w: %s", errNacked, k.id)
	}
	if returned {
		return fmt.Errorf("%w: %s to exchange %q with routing key %q: %s", errUnroutable, k.id, k.exchange, k.key, reason)
	}
	return nil
}

// claim takes every return the listener holds and claims the one of the copy k, if any, with the
// broker's reply text. The broker sends the return of a copy before its confirm, and the client
// hands the return to the listener before it processes the confirm, so a confirmed copy finds its
// return.
func (c *copier) claim(k returnKey) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.take()
	reasons := c.returned[k]
	switch len(reasons) {
	case 0:
		return "", false
	case 1:
		delete(c.returned, k)
	default:
		c.returned[k] = reasons[1:]
	}
	return reasons[0], true
}

// take moves the returns the listener holds to returned. The caller holds c.mu.
func (c *copier) take() {
	for {
		select {
		case ret, ok := <-c.returns:
			if !ok {
				return
			}
			k := returnKey{id: ret.MessageId, exchange: ret.Exchange, key: ret.RoutingKey}
			c.returned[k] = append(c.returned[k], ret.ReplyText)
		default:
			return
		}
	}
}

// Close closes the copier's channel.
func (c *copier) Close() error {
	return c.p.Close()
}
