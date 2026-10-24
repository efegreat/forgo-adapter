package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/server/worker"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// receiveRetryInitial is the pause before receiving again after the client gave up on an error
	// that may pass. It doubles with every failure in a row, up to receiveRetryMax.
	receiveRetryInitial = time.Second
	receiveRetryMax     = time.Minute
)

// errReceiveStopped reports that receiving stopped on its own, without an error and before ctx
// ended.
var errReceiveStopped = errors.New("pubsub: receiving stopped before the subscriber was stopped")

// subscriber is the worker.ISubscriber of a SubscriberConfig.
type subscriber struct {
	conf SubscriberConfig
	sub  *gpubsub.Subscriber
	// deadLetter publishes the messages Reject drops. Nil acknowledges them instead.
	deadLetter *deadLetter

	// retryInitial and retryMax time the pauses before receiving again. Tests shorten them.
	retryInitial, retryMax time.Duration
}

// Consume receives from the subscription and hands every message to state.Handler, with at most
// state.Concurrency in flight. It returns once ctx ends and every handler finished, or with the
// error that stopped receiving, such as a missing subscription or missing permissions.
//
// The client opens its stream again after most failures, but gives up on some that pass when a
// stream ends with them: an exceeded quota (RESOURCE_EXHAUSTED), an error a proxy or load balancer
// sent (UNKNOWN), or a server that shut down. Consume receives again after those, with a pause
// that doubles from a second up to a minute.
func (s *subscriber) Consume(ctx context.Context, state worker.ConsumerState) error {
	concurrency := max(state.Concurrency, 1)
	s.sub.ReceiveSettings.MaxOutstandingMessages = maxOutstanding(s.conf.MaxOutstandingMessages, concurrency)

	// Receive runs callbacks up to MaxOutstandingMessages at once, which may exceed the concurrency.
	slots := process.NewChannelSemaphore(concurrency)
	handle := func(_ context.Context, m *gpubsub.Message) {
		if err := slots.WaitAcquire(ctx); err != nil {
			m.Nack()
			return
		}
		defer slots.Release()
		state.Handler(ctx, newDelivery(m, s.deadLetter))
	}

	wait := s.retryInitial
	for {
		started := time.Now()
		// Receive returns once every callback returned.
		err := s.sub.Receive(ctx, handle)
		if err == nil {
			if ctx.Err() == nil {
				return errReceiveStopped
			}
			return nil
		}
		if ctx.Err() != nil || !retryable(err) {
			return fmt.Errorf("pubsub: subscriber %s: receive from %s: %w", s.conf.Name, s.conf.Subscription, err)
		}
		// A receive that ran for a while starts the pauses over.
		if time.Since(started) >= s.retryMax {
			wait = s.retryInitial
		}
		if !sleep(ctx, wait) {
			return nil
		}
		wait = min(2*wait, s.retryMax)
	}
}

// Close does nothing: Consume stops receiving when its context ends.
func (s *subscriber) Close() error {
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

// retryable reports whether receiving may succeed again after err, which ended Receive: a status
// that passes, such as an exceeded quota, which the client gives up on when a stream ends with it,
// or one it retries itself. A missing subscription, missing permissions, an invalid request or a
// closed client stop the subscriber, because receiving again cannot fix them.
func retryable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.ResourceExhausted, codes.Unknown, codes.Unavailable, codes.Internal, codes.DeadlineExceeded, codes.Aborted:
		return true
	}
	return false
}

// sleep waits for d and reports whether ctx is still running.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
