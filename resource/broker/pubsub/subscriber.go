package pubsub

import (
	"context"
	"errors"
	"fmt"

	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/server/worker"

	gpubsub "cloud.google.com/go/pubsub/v2"
)

// errReceiveStopped reports that receiving stopped on its own, without an error and before ctx
// ended.
var errReceiveStopped = errors.New("pubsub: receiving stopped before the subscriber was stopped")

type Subscriber struct {
	conf SubscriberConfig
	sub  *gpubsub.Subscriber
	// deadLetter publishes the messages Reject drops. Nil acknowledges them instead.
	deadLetter *deadLetter
}

// Consume receives from the subscription and hands every message to state.Handler, with at most
// state.Concurrency in flight. It returns once ctx ends and every handler finished, or with the
// error that stopped receiving, such as a missing subscription or missing permissions.
func (s *Subscriber) Consume(ctx context.Context, state worker.ConsumerState) error {
	concurrency := max(state.Concurrency, 1)
	s.sub.ReceiveSettings.MaxOutstandingMessages = maxOutstanding(s.conf.MaxOutstandingMessages, concurrency)

	// Receive runs callbacks up to MaxOutstandingMessages at once, which may exceed the concurrency.
	slots := process.NewChannelSemaphore(concurrency)
	// Receive returns once every callback returned.
	err := s.sub.Receive(ctx, func(_ context.Context, m *gpubsub.Message) {
		if err := slots.WaitAcquire(ctx); err != nil {
			m.Nack()
			return
		}
		defer slots.Release()
		state.Handler(ctx, newDelivery(m, s.deadLetter))
	})
	if err != nil {
		return fmt.Errorf("pubsub: subscriber %s: receive from %s: %w", s.conf.Name, s.conf.Subscription, err)
	}
	if ctx.Err() == nil {
		return errReceiveStopped
	}
	return nil
}

// Close does nothing: Consume stops receiving when its context ends.
func (s *Subscriber) Close() error {
	return nil
}

// maxOutstanding returns the MaxOutstandingMessages of a subscriber: configured when positive, the
// worker's concurrency when zero, and no limit (negative for the client) when negative.
func maxOutstanding(configured, concurrency int) int {
	switch {
	case configured > 0:
		return configured
	case configured < 0:
		return -1
	default:
		return concurrency
	}
}
