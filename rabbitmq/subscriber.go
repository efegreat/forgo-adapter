package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

// errDeliveriesClosed reports that the delivery channel closed while consuming, together with its
// channel, and no recovery replaces it: recovery is disabled or gave up.
var errDeliveriesClosed = errors.New("rabbitmq: delivery channel closed by the broker")

// consumeChannel is the part of *amqp.Channel a Subscriber uses.
type consumeChannel interface {
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
	IsClosed() bool
	Close() error
}

// subscriber is the worker.ISubscriber of a SubscriberConfig. It consumes on a channel of its own,
// and publishes the copies that retries and rejections need through copier, on another connection.
type subscriber struct {
	*subscription
	copier     *copier
	retry      *retrier
	deadLetter *deadLetter
	release    func()
}

// Consume sets the prefetch, subscribes to the queue and hands every delivery to state.Handler,
// with at most state.Concurrency in flight. It returns once ctx ends and every handler finished,
// or with an error once the consumer is gone: the delivery channel closed with its channel, or the
// consumer could not subscribe again after a recovery or after the broker cancelled it.
func (s *subscriber) Consume(ctx context.Context, state worker.ConsumerState) error {
	if err := s.ch.Qos(prefetch(s.conf.Prefetch, state.Concurrency), 0, false); err != nil {
		return fmt.Errorf("rabbitmq: subscriber %s: set prefetch: %w", s.conf.Name, err)
	}
	st, err := s.start()
	if err != nil {
		return fmt.Errorf("rabbitmq: subscriber %s: consume queue %s: %w", s.conf.Name, s.conf.Queue, err)
	}
	defer s.stop()

	runner := process.NewConcurrentRunner(process.NewChannelSemaphore(state.Concurrency))
	defer runner.Wait()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.lost.done:
			return s.lost.err
		case msg, ok := <-st.deliveries:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				if st, err = s.next(ctx, st); st == nil {
					return err
				}
				continue
			}
			if st.gen != s.guard.generation() {
				// The channel that delivered it was lost: the broker requeued the message, and an
				// acknowledgement would settle a message of the reopened channel.
				continue
			}
			d := newDelivery(msg, s.guard, st.gen, s.retry, s.deadLetter)
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

// Close closes the subscriber's channel, and the publisher of its copies.
func (s *subscriber) Close() error {
	if s.release != nil {
		s.release()
	}
	if s.copier == nil {
		return closeWithin(closeTimeout, s.ch.Close)
	}
	return closeAll(closeTimeout, s.ch.Close, s.copier.Close)
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

// subscription is the consumer of a subscriber's channel, and the streams it delivers on.
//
// The client's recovery would resume the consumer on the stream it had. Deliveries of the lost
// channel that the client still buffers would then come first on it, and pass for deliveries of
// the reopened channel, whose delivery tags start over: their acknowledgements would settle other
// messages, or close the channel with an unknown delivery tag, again and again. So once the client
// reopened the channel, the recovery cancels the client's consumer instead, which ends the stream
// after the deliveries it buffers, and subscribes again on a new stream. Each stream carries the
// ackGuard generation of the channel it delivers for, and Consume drops the deliveries of a stream
// whose channel was lost.
type subscription struct {
	conf SubscriberConfig
	ch   consumeChannel
	// tags returns a new consumer tag, unique on the connection.
	tags  func() string
	guard *ackGuard
	lost  *consumerLoss

	mu sync.Mutex
	// tag is the consumer's tag. The broker keeps the tag of a consumer it cancelled in use on the
	// channel, so consuming again after a cancellation takes a new one.
	tag string
	// stream is the current stream, nil until Consume subscribes.
	stream *stream
	// consuming is set while Consume runs. The recovery subscribes again only then.
	consuming bool
	// replacing is set from the moment the recovery canceled the client's consumer until it
	// subscribed again or gave up.
	replacing bool
	// changed is closed, and replaced, whenever stream or replacing changes.
	changed chan struct{}
}

// stream is the deliveries of a consumer on one incarnation of its channel, the one whose ackGuard
// generation is gen.
type stream struct {
	deliveries <-chan amqp.Delivery
	gen        uint64
}

func newSubscription(conf SubscriberConfig, ch consumeChannel, tags func() string) *subscription {
	return &subscription{
		conf:    conf,
		ch:      ch,
		tags:    tags,
		tag:     tags(),
		guard:   &ackGuard{},
		lost:    newConsumerLoss(),
		changed: make(chan struct{}),
	}
}

// subscribe subscribes the consumer on a new stream. The caller holds s.mu.
func (s *subscription) subscribe() error {
	deliveries, err := s.ch.Consume(s.conf.Queue, s.tag, false, s.conf.Exclusive, s.conf.NoLocal, s.conf.NoWait, s.conf.Args)
	if err != nil {
		return err
	}
	s.stream = &stream{deliveries: deliveries, gen: s.guard.generation()}
	s.notify()
	return nil
}

// notify wakes the callers of next. The caller holds s.mu.
func (s *subscription) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// start subscribes for Consume, and returns the stream.
func (s *subscription) start() (*stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.subscribe(); err != nil {
		return nil, err
	}
	s.consuming = true
	return s.stream, nil
}

// stop records that Consume returned.
func (s *subscription) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consuming = false
}

// next returns the stream that replaces cur, which ended. Without one, it returns nil and what
// Consume returns: errDeliveriesClosed when no recovery replaces cur, the consumer's loss, or nil
// once ctx ends. A stream that ends while its channel is open, and no recovery replaces it, ended
// because the broker cancelled the consumer, and next subscribes again.
func (s *subscription) next(ctx context.Context, cur *stream) (*stream, error) {
	for {
		s.mu.Lock()
		st, replacing, changed := s.stream, s.replacing, s.changed
		if st == cur && !replacing && !s.lost.reported() && !s.ch.IsClosed() {
			again, err := s.consumeAgain()
			s.mu.Unlock()
			return again, err
		}
		s.mu.Unlock()

		if s.lost.reported() {
			return nil, s.lost.err
		}
		if st != cur && st != nil {
			return st, nil
		}
		if !replacing {
			return nil, errDeliveriesClosed
		}
		select {
		case <-changed:
		case <-s.lost.done:
			return nil, s.lost.err
		case <-ctx.Done():
			return nil, nil
		}
	}
}

// consumeAgain subscribes again on a new stream, under a new tag, once the broker cancelled the
// consumer, which it does when the queue is deleted, and from RabbitMQ 4.3 on when a delivery of a
// quorum queue outlives its consumer timeout. The caller holds s.mu. The channel stays open, so the
// new stream keeps the guard generation: delivery tags go on, and the handlers of the cancelled
// consumer's deliveries still settle them. A subscription that cannot subscribe again, such as one
// whose queue was deleted, is lost.
//
// A quorum queue keeps a consumer that timed out, and can deliver to it again, although it told the
// client it cancelled it and the client dropped its stream: those deliveries would wait,
// unacknowledged, until the broker timed them out too and closed the channel. So consumeAgain first
// cancels the consumer itself, which removes it. The broker acknowledges the cancellation of a
// consumer it no longer has, such as that of a deleted queue.
func (s *subscription) consumeAgain() (*stream, error) {
	cancelled := s.tag
	if err := s.ch.Cancel(cancelled, false); err != nil {
		s.lost.report(fmt.Errorf("consumer %s of queue %s, which the broker cancelled: cancel it: %w", cancelled, s.conf.Queue, err))
		return nil, s.lost.err
	}
	s.tag = s.tags()
	if err := s.subscribe(); err != nil {
		s.lost.report(fmt.Errorf("consumer %s of queue %s, replacing %s, which the broker cancelled: %w", s.tag, s.conf.Queue, cancelled, err))
		return nil, s.lost.err
	}
	return s.stream, nil
}

// lose marks the deliveries of the channel stale, before the client reopens it. The hook of a
// channel of a lost connection can run late, once the connection's recovery reopened the channel
// and subscribed again, so unless always, for the connection's own hook, a channel that is open
// again keeps its stream.
func (s *subscription) lose(always bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if always || s.ch.IsClosed() {
		s.guard.invalidate()
	}
}

// detach cancels the client's consumer on the reopened channel, before the client's recovery would
// resume it on the current stream. The client ends the stream once it handed out what it buffers.
// detach reports whether the subscription waits for resubscribe.
func (s *subscription) detach() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stream == nil || s.lost.reported() {
		return false, nil
	}
	if s.replacing {
		// An earlier attempt of the client's recovery canceled it.
		return true, nil
	}
	// The broker has no consumer of that tag on the reopened channel, and acknowledges the
	// cancellation anyway.
	if err := s.ch.Cancel(s.tag, false); err != nil {
		return false, fmt.Errorf("rabbitmq: subscriber %s: cancel the consumer of the lost channel: %w", s.conf.Name, err)
	}
	s.replacing = true
	return true, nil
}

// resubscribe subscribes a detached subscription again on a new stream, after declare, or reports
// it lost. A subscription that Consume no longer reads stays without a consumer.
func (s *subscription) resubscribe(declare func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.replacing {
		return
	}
	s.replacing = false
	defer s.notify()
	if !s.consuming {
		s.stream = nil
		return
	}

	err := declare()
	if err == nil {
		err = s.subscribe()
	}
	if err != nil {
		s.lost.report(fmt.Errorf("consumer %s of queue %s: %w", s.tag, s.conf.Queue, err))
	}
}

// giveUp reports a subscription that a recovery detached and could not subscribe again, unless
// skip reports another recovery in progress.
func (s *subscription) giveUp(skip func() bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.replacing || skip() {
		return
	}
	s.lost.report(errRecoveryFailed)
	s.replacing = false
	s.notify()
}
