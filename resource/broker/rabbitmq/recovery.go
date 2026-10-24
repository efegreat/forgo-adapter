package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// errStaleDelivery is returned when a delivery is settled after the channel that delivered it
	// was lost. The broker requeued the message when the channel closed and delivers it again.
	errStaleDelivery = errors.New("rabbitmq: delivery belongs to a lost channel; the broker redelivers it")
	// errConsumerLost is returned by Consume when the subscriber's consumer could not subscribe
	// again, after a recovery or after the broker cancelled it. Its channel delivers nothing.
	errConsumerLost = errors.New("rabbitmq: recovery could not re-subscribe the consumer")
	// errRecoveryFailed is the loss of a consumer whose connection or channel the client could not
	// recover once the recovery had canceled the consumer.
	errRecoveryFailed = errors.New("rabbitmq: the connection or channel could not be recovered")
	// errBlocked is returned by a publish that waited in vain for the broker to stop blocking the
	// publishing connection, which it does while it is low on memory or disk space.
	errBlocked = errors.New("rabbitmq: the broker blocks publishing")
)

// ackGuard keeps acknowledgements of deliveries from a lost channel off its recovered successor.
//
// A recovered channel reuses the same *amqp.Channel, but delivery tags start over at 1. A late
// acknowledgement of an old delivery would therefore settle whichever new message got its tag.
// Every delivery records the generation of the channel that delivered it, and the recovery hooks
// advance the generation before the channel is reopened. The read lock spans the check and the
// acknowledgement, so no acknowledgement can pass the check and then reach the reopened channel.
type ackGuard struct {
	mu  sync.RWMutex
	gen uint64
}

func (g *ackGuard) generation() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.gen
}

// invalidate marks every delivery received so far as stale.
func (g *ackGuard) invalidate() {
	g.mu.Lock()
	g.gen++
	g.mu.Unlock()
}

// settle runs ack unless the delivery of generation gen is stale.
func (g *ackGuard) settle(gen uint64, ack func() error) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if gen != g.gen {
		return errStaleDelivery
	}
	return ack()
}

// gate holds the publishes of a publisher channel back while the client reopens it.
//
// The client resets a channel before it opens it again and puts it back in confirm mode, and lets
// frames through in between. A message published then is never confirmed, and it shifts the
// client's count of confirms by one for good: every later publish would wait for a confirm in
// vain, while the broker routes it. The recovery hooks close the gate before the client reopens
// the channel and open it once the client returns, and publish sends under the gate's read lock,
// so no publish slips into the reopening. While the channel is still closed, such as while the
// connection is down, publishes fail at once instead of waiting.
type gate struct {
	mu sync.RWMutex
	// closed counts the recoveries in progress: the hooks of a channel and of its connection can
	// overlap.
	closed int
	// opened is closed once closed is back at zero.
	opened chan struct{}
	// losses counts the losses of the channel, and reason is why it was last lost, for the
	// publishes whose confirm a loss cut short.
	losses uint64
	reason error
	// lost, when set, is closed by the next close, for the publishes that found the channel closed
	// before the hooks closed the gate.
	lost chan struct{}
}

// close holds publishes back until open, for a loss of the channel for reason.
func (g *gate) close(reason error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed == 0 {
		g.opened = make(chan struct{})
	}
	g.closed++
	g.losses++
	g.reason = reason
	if g.lost != nil {
		close(g.lost)
		g.lost = nil
	}
}

// open lets publishes through again once every close is matched.
func (g *gate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed--
	if g.closed == 0 {
		close(g.opened)
	}
}

// send runs publish once the gate is open, holding it open until publish returns, and returns the
// losses counted so far, for lostSince. While the gate is closed, it fails with amqp.ErrClosed as
// long as closed reports the channel closed, as publishing would while the connection is down, and
// otherwise waits for the client to finish reopening the channel, until ctx ends. A publisher that
// recovers waits for the client to reopen the channel either way. The client closes a lost channel
// before it runs the hooks, so such a publisher can also find the channel closed while the gate is
// still open: send then waits for the hooks to close the gate, and goes on as above. A nil gate is
// always open.
func (g *gate) send(ctx context.Context, recovers bool, closed func() bool, publish func() error) (uint64, error) {
	if g == nil {
		return 0, publish()
	}
	for {
		g.mu.RLock()
		if g.closed == 0 {
			losses := g.losses
			err := publish()
			g.mu.RUnlock()
			if !recovers || !errors.Is(err, amqp.ErrClosed) {
				return losses, err
			}
			if err := g.waitLoss(ctx, losses); err != nil {
				return 0, err
			}
			continue
		}
		opened := g.opened
		g.mu.RUnlock()
		if !recovers && closed() {
			return 0, amqp.ErrClosed
		}

		select {
		case <-opened:
		case <-ctx.Done():
			return 0, fmt.Errorf("wait for the recovery of the channel: %w", ctx.Err())
		}
	}
}

// waitLoss waits until the hooks closed the gate for a loss after the first losses, or ctx ends.
func (g *gate) waitLoss(ctx context.Context, losses uint64) error {
	g.mu.Lock()
	if g.losses != losses {
		g.mu.Unlock()
		return nil
	}
	if g.lost == nil {
		g.lost = make(chan struct{})
	}
	lost := g.lost
	g.mu.Unlock()

	select {
	case <-lost:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for the recovery of the channel: %w", ctx.Err())
	}
}

// lostSince returns why the channel was last lost, if it was lost after send returned losses.
func (g *gate) lostSince(losses uint64) error {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.losses == losses {
		return nil
	}
	return g.reason
}

// flow holds the publishes of a connection back while the broker blocks it. RabbitMQ blocks every
// connection that publishes while it is low on memory or disk space, and stops reading from it.
// The client would still write a publish, and once the socket's buffer is full, block in the write
// for as long as the alarm lasts, whatever the publish's context, holding the publisher's lock and
// its gate: the relay could not stop, nor the app shut down. A publish waits for the broker to
// unblock the connection instead, until its context ends. One the broker blocks between the check
// and the write waits for its confirm, which its context bounds.
type flow struct {
	mu sync.Mutex
	// reason is why the broker blocks the connection, such as "low on memory". unblocked is closed
	// while it does not.
	reason    string
	unblocked chan struct{}
}

func newFlow() *flow {
	unblocked := make(chan struct{})
	close(unblocked)
	return &flow{unblocked: unblocked}
}

// watch follows the notices of conn that the broker blocks it, and unblocks it, until the client
// closes the connection for good. The client waits for every notice to be received.
func (f *flow) watch(conn *amqp.Connection) {
	blocks := conn.NotifyBlocked(make(chan amqp.Blocking, 1))
	go func() {
		for b := range blocks {
			if b.Active {
				f.block(b.Reason)
			} else {
				f.unblock()
			}
		}
	}()
}

func (f *flow) block(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.isUnblocked() {
		f.unblocked = make(chan struct{})
	}
	f.reason = reason
}

// unblock lets publishes through again. A lost connection unblocks too: the broker blocks the next
// one anew while its alarm lasts.
func (f *flow) unblock() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.isUnblocked() {
		close(f.unblocked)
	}
}

// isUnblocked reports whether unblocked is closed. The caller holds f.mu.
func (f *flow) isUnblocked() bool {
	select {
	case <-f.unblocked:
		return true
	default:
		return false
	}
}

// wait returns once the broker does not block the connection, or with errBlocked and ctx's error
// once ctx ends. A nil flow never blocks.
func (f *flow) wait(ctx context.Context) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	unblocked, reason := f.unblocked, f.reason
	f.mu.Unlock()
	select {
	case <-unblocked:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w (%s): %w", errBlocked, reason, ctx.Err())
	}
}

// consumerLoss hands the first failure to re-subscribe a subscriber's consumer to its Consume.
type consumerLoss struct {
	once sync.Once
	done chan struct{}
	err  error
}

func newConsumerLoss() *consumerLoss {
	return &consumerLoss{done: make(chan struct{})}
}

// report records err and closes done. Later reports are ignored.
func (l *consumerLoss) report(err error) {
	l.once.Do(func() {
		l.err = fmt.Errorf("%w: %w", errConsumerLost, err)
		close(l.done)
	})
}

// reported tells whether a loss was reported, without waiting.
func (l *consumerLoss) reported() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// recoveryHooks wraps the client's connection and topology recovery of one of the broker's
// connections. The close hooks run on the client's close watchers, after the lost connection or
// channel is closed. The client's recovery, which they call, reopens it and recovers the topology
// through RecoverTopology before it returns.
type recoveryHooks struct {
	broker   *broker
	next     amqp.ConnectionRecovery
	topology amqp.TopologyRecovery
	// publishers is set on the hooks of the publishing connection, whose flow is flow. The other
	// connection carries the subscribers.
	publishers bool
	flow       *flow
	// reconnecting counts the connection recoveries in progress. The channels of a lost connection
	// are closed too, and their hooks leave them to the connection's.
	reconnecting atomic.Int32
}

// OnConnectionClose recovers a lost connection. The client also closes the connection of every
// reconnection attempt that fails once it dialed, such as a handshake that a load balancer cuts
// short while the broker behind it is down, and hands those closes to the hook once it returns
// from the recovery that reconnected. The connection is open by then, and the client's recovery
// would return at once: marking every delivery stale would leave each subscriber dropping its
// deliveries for good, with no recovery left to subscribe it again.
func (h *recoveryHooks) OnConnectionClose(conn *amqp.Connection, err *amqp.Error) {
	if conn != nil && !conn.IsClosed() {
		return
	}
	h.reconnecting.Add(1)
	gates := h.broker.loseConnection(h.publishers, lossReason(err))
	if h.flow != nil {
		h.flow.unblock()
	}
	h.next.OnConnectionClose(conn, err)
	h.reconnecting.Add(-1)

	for _, g := range gates {
		g.open()
	}
	// A recovered connection re-subscribed every subscriber it detached. One that gave up, or that
	// Close stopped, leaves them waiting.
	if !h.publishers && (conn == nil || conn.IsClosed()) {
		h.broker.giveUp()
	}
}

// OnChannelClose recovers only the channels the broker owns. A channel closed by a failed check
// before it was handed out, such as a subscriber whose queue does not exist, stays closed.
func (h *recoveryHooks) OnChannelClose(ch *amqp.Channel, err *amqp.Error) {
	o, owned := h.broker.owner(ch)
	if !owned {
		return
	}
	if o.sub != nil {
		o.sub.lose(false)
	}
	if o.gate != nil {
		o.gate.close(lossReason(err))
	}

	h.next.OnChannelClose(ch, err)

	if o.gate != nil {
		o.gate.open()
	}
	if o.sub != nil {
		o.sub.giveUp(func() bool { return h.reconnecting.Load() > 0 })
	}
}

// RecoverTopology recovers the exchanges, queues and bindings through the client, and re-subscribes
// the subscribers of the reopened channels itself, each on a new delivery stream (see subscription).
// The client runs it once it reopened the channels.
//
// An exchange, queue or binding that fails to recover is skipped, as the client does by default,
// so the rest of the topology still recovers. Aborting instead would make the client reconnect
// again and again while every publisher and subscriber waits, without end when the failure is
// permanent, such as a deleted queue. A subscriber that cannot consume again is told, and its
// Consume fails instead of waiting forever.
func (h *recoveryHooks) RecoverTopology(conn *amqp.Connection, channels []*amqp.Channel) ([]amqp.TopologyRecoveryEntity, error) {
	subs, err := h.broker.detach(channels)
	if err != nil {
		return nil, err
	}
	skipped, err := h.topology.RecoverTopology(conn, channels)
	if err != nil {
		// The client starts over, or gives up and closes the channels. The detached subscribers
		// wait for the next attempt, or for giveUp.
		return skipped, err
	}
	h.broker.resubscribe(subs)
	return skipped, nil
}

// lossReason returns err as an error. The client passes nil for a connection closed on purpose.
func lossReason(err *amqp.Error) error {
	if err == nil {
		return amqp.ErrClosed
	}
	return err
}

// amqpRecovery returns the client recovery configuration, or nil when recovery is disabled.
func (r RecoveryConfig) amqpRecovery(hooks *recoveryHooks) *amqp.Recovery {
	if r.Disabled {
		return nil
	}
	return &amqp.Recovery{
		ReconnectionConfig: &amqp.ReconnectionConfig{
			MaxRetryCount: r.MaxRetries,
			RetryInterval: r.RetryInterval,
		},
		ConnectionRecovery: hooks,
		TopologyRecovery:   hooks,
	}
}
