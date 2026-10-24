package rabbitmq

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultPublisherName = "default"
	// returnBuffer holds the returns of unroutable mandatory messages until Publish reads them.
	// The client stalls the connection while a return listener is full.
	returnBuffer = 16

	consumerTagPrefix = "forgo-"
	// maxConsumerTag is the longest AMQP short string.
	maxConsumerTag = 255

	// publishConnSuffix names the publishing connection after the resource's.
	publishConnSuffix = "-publish"

	// closeTimeout bounds the wait for the close of a channel or a connection, which a broker that
	// hangs, or that the network cut off, never answers. A healthy broker answers within a round trip.
	closeTimeout = 2 * time.Second
)

// broker is the resource New returns and its worker.IBroker Value. It holds two connections, with a
// channel for every publisher and subscriber.
type broker struct {
	conf        Config
	subscribers map[string]SubscriberConfig
	publishers  map[string]PublisherConfig
	// dial, when set, replaces the client's dialer of the publishing connection when publishing is
	// set, and of the other one otherwise, for the tests that cut the connections it makes.
	dial func(publishing bool, network, addr string) (net.Conn, error)
	// conn carries the topology and subscriber channels, and publishConn the publisher channels,
	// those that copy the subscribers' retries and dead letters included. RabbitMQ blocks every
	// connection that publishes while it is low on memory or disk space, with all its channels, so
	// the subscribers keep consuming, which frees what the broker lacks, only on a connection that
	// never publishes.
	conn        *amqp.Connection
	publishConn *amqp.Connection
	// flow holds publishes back while the broker blocks publishConn.
	flow *flow
	// topology is the channel the exchanges, queues and bindings were declared on. The client
	// recovers the topology recorded on open channels only, so it stays open until Close.
	topology declarer
	// redeclaring serializes the declarations of redeclare: the recoveries of two subscriber
	// channels run at once, and the client matches the replies of a channel to calls in order.
	redeclaring sync.Mutex

	mu sync.Mutex
	// owned holds the channels to recover.
	owned map[*amqp.Channel]ownedChannel
	// consumerSeq numbers the consumer tags, so they are unique on the connection.
	consumerSeq uint64
}

// ownedChannel is what the recovery of an owned channel takes care of: the gate of a publisher
// channel, on publishConn, or the subscription of a subscriber channel, on conn. The topology
// channel, on conn, has neither.
type ownedChannel struct {
	gate *gate
	sub  *subscription
}

// Name returns Config.Name.
func (r *broker) Name() string {
	return r.conf.Name
}

// NewDefaultPublisher returns a publisher of persistent messages that needs no PublisherConfig.
func (r *broker) NewDefaultPublisher() (worker.IPublisher, error) {
	return r.newPublisher(PublisherConfig{Name: defaultPublisherName}.Sanitize(), false)
}

// NewPublisher returns a publisher with the PublisherConfig named name.
func (r *broker) NewPublisher(name string) (worker.IPublisher, error) {
	conf, ok := r.publishers[name]
	if !ok {
		return nil, fmt.Errorf("rabbitmq: publisher config not found for name: %s", name)
	}
	return r.newPublisher(conf, false)
}

// newPublisher opens a channel in confirm mode on publishConn, so Publish returns once the broker
// has the message. A publisher that waits for recovery waits while the connection is down, as it
// does while the client reopens the channel, instead of failing at once.
func (r *broker) newPublisher(conf PublisherConfig, waitsForRecovery bool) (*publisher, error) {
	ch, err := r.publishConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open publisher channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return nil, errors.Join(fmt.Errorf("rabbitmq: enable publisher confirms: %w", err), ch.Close())
	}

	p := &publisher{conf: conf, ch: ch, gate: &gate{}, flow: r.flow, waitsForRecovery: waitsForRecovery}
	if conf.Mandatory {
		p.returns = ch.NotifyReturn(make(chan amqp.Return, returnBuffer))
	}
	r.own(ch, ownedChannel{gate: p.gate})
	p.release = func() { r.disown(ch) }
	return p, nil
}

// NewSubscriber opens the subscriber's channel and checks that its queue exists. Consumption
// starts in Consume, once the worker's concurrency sets the default prefetch.
func (r *broker) NewSubscriber(name string) (worker.ISubscriber, error) {
	conf, ok := r.subscribers[name]
	if !ok {
		return nil, fmt.Errorf("rabbitmq: subscriber config not found for name: %s", name)
	}
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open subscriber channel: %w", err)
	}

	// A missing queue makes the broker close the channel. It is not owned yet, so it is not recovered.
	if _, err := ch.QueueDeclarePassive(conf.Queue, false, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("rabbitmq: subscriber %s: queue %s: %w", name, conf.Queue, err)
	}

	sub := newSubscription(conf, ch, func() string { return r.consumerTag(name) })
	r.own(ch, ownedChannel{sub: sub})
	copier, retry, deadLetter, err := r.newCopier(conf.Queue)
	if err != nil {
		r.disown(ch)
		return nil, errors.Join(fmt.Errorf("rabbitmq: subscriber %s: %w", name, err), ch.Close())
	}
	return &subscriber{
		subscription: sub,
		copier:       copier,
		retry:        retry,
		deadLetter:   deadLetter,
		release:      func() { r.disown(ch) },
	}, nil
}

// newCopier returns the publisher of the copies that the deliveries of the configured queue named
// queue need, for its retry queues and its dead-letter exchange, with the retrier and the dead
// letter that publish through it. The retrier is nil without Retry, the dead letter without
// DeadLetter, and all three without either.
func (r *broker) newCopier(queue string) (*publisher, *retrier, *deadLetter, error) {
	q, ok := queueConfig(r.conf, queue)
	if !ok || !q.Retry.enabled() && !q.DeadLetter.Enabled {
		return nil, nil, nil, nil
	}
	// Mandatory: a retry queue or dead-letter queue deleted behind the broker's back fails the copy
	// instead of losing it, and the delivery is requeued. The publisher waits while its connection
	// is down, within the acknowledgement's context: a copy that failed at once would requeue the
	// delivery at once, and its handler would run again and again until the connection is back.
	// Without recovery, nothing brings the connection back.
	pub, err := r.newPublisher(PublisherConfig{Name: "copies", Mandatory: true, DeliveryMode: amqp.Persistent}, !r.conf.Recovery.Disabled)
	if err != nil {
		return nil, nil, nil, err
	}
	var retry *retrier
	if queues := q.Retry.queues(q.Name); len(queues) > 0 {
		retry = &retrier{pub: pub, queues: queues}
	}
	var dl *deadLetter
	if q.DeadLetter.Enabled {
		dl = &deadLetter{pub: pub, exchange: q.DeadLetter.Exchange, key: q.DeadLetter.RoutingKey, retry: retry}
	}
	return pub, retry, dl, nil
}

// queueConfig returns the configured queue named queue.
func queueConfig(cfg Config, queue string) (QueueConfig, bool) {
	for _, q := range cfg.Queues {
		if q.Name == queue {
			return q, true
		}
	}
	return QueueConfig{}, false
}

// Value returns the broker as a worker.IBroker.
func (r *broker) Value() any {
	return worker.IBroker(r)
}

// Close closes the connections and with them every channel. It also stops a recovery in progress.
func (r *broker) Close() error {
	return closeAll(closeTimeout, closeConn(r.publishConn), closeConn(r.conn))
}

// closeConn returns the close of conn, whose socket waits at most closeTimeout for the broker's
// answer.
func closeConn(conn *amqp.Connection) func() error {
	return func() error { return conn.CloseDeadline(time.Now().Add(closeTimeout)) }
}

// isSocketError reports whether err is the client's error for a socket that failed, which it uses
// for a read that timed out too.
func isSocketError(err error) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == amqp.FrameError
}

// closeWithin runs close, the Close of a channel or a connection, and waits for it at most timeout.
// The client's Close waits for the broker's answer, and a connection's also for the reconnection
// attempt in progress, whose handshake a broker that hangs keeps going for the client's 30 seconds.
// Neither a channel or connection that is closed already nor a close that does not return in time
// fails: the client goes on closing in the background, and the broker requeues what the channel or
// connection had not acknowledged once it notices it is gone.
func closeWithin(timeout time.Duration, close func() error) error {
	done := make(chan error, 1)
	go func() { done <- close() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if errors.Is(err, amqp.ErrClosed) || isSocketError(err) {
			return nil
		}
		return err
	case <-timer.C:
		return nil
	}
}

// closeAll runs every close through closeWithin at once, so that a broker that does not answer
// delays them all by timeout once.
func closeAll(timeout time.Duration, closes ...func() error) error {
	errs := make(chan error, len(closes))
	for _, c := range closes {
		go func() { errs <- closeWithin(timeout, c) }()
	}
	var err error
	for range closes {
		err = errors.Join(err, <-errs)
	}
	return err
}

func (r *broker) own(ch *amqp.Channel, o ownedChannel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.owned[ch] = o
}

func (r *broker) disown(ch *amqp.Channel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.owned, ch)
}

func (r *broker) owner(ch *amqp.Channel) (ownedChannel, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.owned[ch]
	return o, ok
}

// ownedChannels returns what the broker owns, so the recovery handles it without holding r.mu.
func (r *broker) ownedChannels() []ownedChannel {
	r.mu.Lock()
	defer r.mu.Unlock()
	owned := make([]ownedChannel, 0, len(r.owned))
	for _, o := range r.owned {
		owned = append(owned, o)
	}
	return owned
}

// consumerTag returns a new consumer tag for the subscriber name, unique on the connection.
func (r *broker) consumerTag(name string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consumerSeq++
	return consumerTag(name, r.consumerSeq)
}

// consumerTag names a consumer after its subscriber, so it can be told apart in the management
// UI, unless the name makes the tag too long for AMQP.
func consumerTag(name string, seq uint64) string {
	suffix := "-" + strconv.FormatUint(seq, 10)
	if len(consumerTagPrefix)+len(name)+len(suffix) > maxConsumerTag {
		return consumerTagPrefix + strconv.FormatUint(seq, 10)
	}
	return consumerTagPrefix + name + suffix
}

// loseConnection holds back the publishes of every publisher channel when publishers is set, the
// publishing connection was lost, and otherwise marks the deliveries of every subscriber channel
// stale, before the client reconnects. It returns the gates to open once the client is done.
func (r *broker) loseConnection(publishers bool, reason error) []*gate {
	var gates []*gate
	for _, o := range r.ownedChannels() {
		if o.sub != nil && !publishers {
			o.sub.lose(true)
		}
		if o.gate != nil && publishers {
			o.gate.close(reason)
			gates = append(gates, o.gate)
		}
	}
	return gates
}

// detach detaches the subscriptions of the reopened channels, and returns them.
func (r *broker) detach(channels []*amqp.Channel) ([]*subscription, error) {
	var subs []*subscription
	for _, ch := range channels {
		o, ok := r.owner(ch)
		if !ok || o.sub == nil {
			continue
		}
		detached, err := o.sub.detach()
		if err != nil {
			return nil, err
		}
		if detached {
			subs = append(subs, o.sub)
		}
	}
	return subs, nil
}

// resubscribe subscribes the detached subscriptions again, once the client recovered the rest of
// the topology.
func (r *broker) resubscribe(subs []*subscription) {
	for _, s := range subs {
		s.resubscribe(func() error { return r.redeclare(s.conf.Queue) })
	}
}

// redeclare declares the queue named queue again when it is a configured auto-delete queue. The
// client forgets an auto-delete queue once no consumer of its own is left on it, which detach
// brings about, so it neither recovered the queue nor its bindings. The broker deletes such a
// queue when it loses its last consumer.
func (r *broker) redeclare(queue string) error {
	for _, q := range r.conf.Queues {
		if q.Name == queue && q.AutoDelete {
			r.redeclaring.Lock()
			defer r.redeclaring.Unlock()
			return declareQueue(r.topology, q)
		}
	}
	return nil
}

// giveUp reports the subscriptions that a failed recovery detached.
func (r *broker) giveUp() {
	for _, o := range r.ownedChannels() {
		if o.sub != nil {
			o.sub.giveUp(func() bool { return false })
		}
	}
}

func (r *broker) init() (err error) {
	conn, err := r.connect(r.conf.Name, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq: connect: %w", err)
	}
	r.conn = conn
	defer func() {
		if err != nil {
			err = errors.Join(err, conn.Close())
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq: open topology channel: %w", err)
	}
	r.topology = ch

	if err := declare(ch, r.conf); err != nil {
		return err
	}
	r.own(ch, ownedChannel{})

	publishName := ""
	if r.conf.Name != "" {
		publishName = r.conf.Name + publishConnSuffix
	}
	r.flow = newFlow()
	publishConn, err := r.connect(publishName, r.flow)
	if err != nil {
		return fmt.Errorf("rabbitmq: connect for publishing: %w", err)
	}
	r.publishConn = publishConn
	r.flow.watch(publishConn)

	for _, s := range r.conf.Subscribers {
		r.subscribers[s.Name] = s
	}
	for _, p := range r.conf.Publishers {
		r.publishers[p.Name] = p
	}

	return nil
}

// connect opens a connection named name, which publishes when flow is set: its recovery then holds
// back the publishes of the publisher channels, and forgets that the broker blocked the connection.
func (r *broker) connect(name string, flow *flow) (*amqp.Connection, error) {
	cfg := amqp.Config{
		Vhost:      r.conf.VHost,
		Properties: amqp.NewConnectionProperties(),
		Recovery: r.conf.Recovery.amqpRecovery(&recoveryHooks{
			broker:     r,
			next:       &amqp.DefaultConnectionRecovery{},
			topology:   &amqp.DefaultTopologyRecovery{},
			publishers: flow != nil,
			flow:       flow,
		}),
	}
	if r.dial != nil {
		cfg.Dial = func(network, addr string) (net.Conn, error) { return r.dial(flow != nil, network, addr) }
	}
	if r.conf.Username != "" {
		cfg.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: r.conf.Username, Password: r.conf.Password}}
	}
	if name != "" {
		cfg.Properties.SetClientConnectionName(name)
	}

	scheme := "amqp://"
	if r.conf.TLSConfig != nil {
		scheme = "amqps://"
		// The client writes ServerName into the TLS config it is given, which belongs to the caller.
		cfg.TLSClientConfig = r.conf.TLSConfig.Clone()
	}

	// Credentials and the vhost travel in cfg, so the URL needs no escaping.
	return amqp.DialConfig(scheme+net.JoinHostPort(r.conf.Addr, r.conf.Port)+"/", cfg)
}

// declarer is the part of *amqp.Channel that declares the topology.
type declarer interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}

// declare declares the exchanges, then every queue after its dead-letter exchange and queue, so a
// queue never points at a dead-letter exchange that does not exist yet, and before its retry queues.
func declare(ch declarer, cfg Config) error {
	for _, e := range cfg.Exchanges {
		if err := ch.ExchangeDeclare(e.Name, e.Kind, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Args); err != nil {
			return fmt.Errorf("rabbitmq: declare exchange %s: %w", e.Name, err)
		}
	}

	for _, q := range cfg.Queues {
		if err := declareQueue(ch, q); err != nil {
			return err
		}
	}
	return nil
}

// declareQueue declares q with its dead-letter exchange and queue, its bindings and its retry queues.
func declareQueue(ch declarer, q QueueConfig) error {
	if dl := q.DeadLetter; dl.Enabled {
		if err := ch.ExchangeDeclare(dl.Exchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
			return fmt.Errorf("rabbitmq: declare dead-letter exchange %s: %w", dl.Exchange, err)
		}
		if _, err := ch.QueueDeclare(dl.Queue, true, false, false, false, dl.Args); err != nil {
			return fmt.Errorf("rabbitmq: declare dead-letter queue %s: %w", dl.Queue, err)
		}
		if err := ch.QueueBind(dl.Queue, dl.RoutingKey, dl.Exchange, false, nil); err != nil {
			return fmt.Errorf("rabbitmq: bind dead-letter queue %s to exchange %s: %w", dl.Queue, dl.Exchange, err)
		}
	}

	if _, err := ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.args()); err != nil {
		return fmt.Errorf("rabbitmq: declare queue %s: %w", q.Name, err)
	}

	for _, b := range q.Bindings {
		if err := ch.QueueBind(q.Name, b.RoutingKey, b.Exchange, b.NoWait, b.Args); err != nil {
			return fmt.Errorf("rabbitmq: bind queue %s to exchange %s with routing key %s: %w", q.Name, b.Exchange, b.RoutingKey, err)
		}
	}

	for _, rq := range q.Retry.queues(q.Name) {
		if _, err := ch.QueueDeclare(rq.name, true, false, false, false, rq.args); err != nil {
			return fmt.Errorf("rabbitmq: declare retry queue %s: %w", rq.name, err)
		}
	}
	return nil
}
