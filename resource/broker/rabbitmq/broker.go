package rabbitmq

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

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
)

// broker is the resource New returns and its worker.IBroker Value. It holds one connection, and a
// channel for every publisher and subscriber.
type broker struct {
	conf        Config
	subscribers map[string]SubscriberConfig
	publishers  map[string]PublisherConfig
	// dial, when set, replaces the client's dialer, for the tests that cut the connections it makes.
	dial func(network, addr string) (net.Conn, error)
	conn *amqp.Connection
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
// channel, or the subscription of a subscriber channel. The topology channel has neither.
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
	return r.newPublisher(PublisherConfig{Name: defaultPublisherName}.Sanitize())
}

// NewPublisher returns a publisher with the PublisherConfig named name.
func (r *broker) NewPublisher(name string) (worker.IPublisher, error) {
	conf, ok := r.publishers[name]
	if !ok {
		return nil, fmt.Errorf("rabbitmq: publisher config not found for name: %s", name)
	}
	return r.newPublisher(conf)
}

// newPublisher opens a channel in confirm mode, so Publish returns once the broker has the message.
func (r *broker) newPublisher(conf PublisherConfig) (*publisher, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open publisher channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return nil, errors.Join(fmt.Errorf("rabbitmq: enable publisher confirms: %w", err), ch.Close())
	}

	p := &publisher{conf: conf, ch: ch, gate: &gate{}}
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
	retry, err := r.newRetrier(conf.Queue)
	if err != nil {
		r.disown(ch)
		return nil, errors.Join(fmt.Errorf("rabbitmq: subscriber %s: %w", name, err), ch.Close())
	}
	return &subscriber{
		subscription: sub,
		retry:        retry,
		release:      func() { r.disown(ch) },
	}, nil
}

// newRetrier returns the retrier of a queue that has Retry, with a publisher of its own, or nil.
func (r *broker) newRetrier(queue string) (*retrier, error) {
	queues := retryQueues(r.conf, queue)
	if len(queues) == 0 {
		return nil, nil
	}
	// Mandatory: a retry queue deleted behind the broker's back fails the retry instead of losing
	// the copy, and the delivery is requeued at once.
	pub, err := r.newPublisher(PublisherConfig{Name: "retry", Mandatory: true, DeliveryMode: amqp.Persistent})
	if err != nil {
		return nil, err
	}
	return &retrier{pub: pub, queues: queues}, nil
}

// retryQueues returns the retry queues of the configured queue named queue, or none.
func retryQueues(cfg Config, queue string) []retryQueue {
	for _, q := range cfg.Queues {
		if q.Name == queue {
			return q.Retry.queues(q.Name)
		}
	}
	return nil
}

// Value returns the broker as a worker.IBroker.
func (r *broker) Value() any {
	return worker.IBroker(r)
}

// Close closes the connection and with it every channel. It also stops a recovery in progress.
func (r *broker) Close() error {
	return r.conn.Close()
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

// loseConnection marks the deliveries of every subscriber channel stale and holds back the
// publishes of every publisher channel, before the client reconnects. It returns the gates to
// open once the client is done.
func (r *broker) loseConnection(reason error) []*gate {
	var gates []*gate
	for _, o := range r.ownedChannels() {
		if o.sub != nil {
			o.sub.lose(true)
		}
		if o.gate != nil {
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
	cfg := amqp.Config{
		Vhost:      r.conf.VHost,
		Properties: amqp.NewConnectionProperties(),
		Dial:       r.dial,
		Recovery: r.conf.Recovery.amqpRecovery(&recoveryHooks{
			broker:   r,
			next:     &amqp.DefaultConnectionRecovery{},
			topology: &amqp.DefaultTopologyRecovery{},
		}),
	}
	if r.conf.Username != "" {
		cfg.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: r.conf.Username, Password: r.conf.Password}}
	}
	if r.conf.Name != "" {
		cfg.Properties.SetClientConnectionName(r.conf.Name)
	}

	scheme := "amqp://"
	if r.conf.TLSConfig != nil {
		scheme = "amqps://"
		// The client writes ServerName into the TLS config it is given, which belongs to the caller.
		cfg.TLSClientConfig = r.conf.TLSConfig.Clone()
	}

	// Credentials and the vhost travel in cfg, so the URL needs no escaping.
	conn, err := amqp.DialConfig(scheme+net.JoinHostPort(r.conf.Addr, r.conf.Port)+"/", cfg)
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

	for _, s := range r.conf.Subscribers {
		r.subscribers[s.Name] = s
	}
	for _, p := range r.conf.Publishers {
		r.publishers[p.Name] = p
	}

	return nil
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
