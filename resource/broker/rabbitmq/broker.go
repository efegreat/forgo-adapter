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

type Broker struct {
	conf        Config
	subscribers map[string]SubscriberConfig
	publishers  map[string]PublisherConfig
	conn        *amqp.Connection
	// topology is the channel the exchanges, queues and bindings were declared on. The client
	// recovers the topology recorded on open channels only, so it stays open until Close.
	topology *amqp.Channel

	mu sync.Mutex
	// owned holds the channels to recover, with the ack guard of subscriber channels.
	owned map[*amqp.Channel]*ackGuard
	// consumers holds the loss of every open subscriber by consumer tag, the only name the
	// recovery gives a consumer it could not re-subscribe.
	consumers map[string]*consumerLoss
	// consumerSeq numbers the consumer tags, so they are unique on the connection.
	consumerSeq uint64
}

func (r *Broker) Name() string {
	return r.conf.Name
}

func (r *Broker) NewDefaultPublisher() (worker.IPublisher, error) {
	return r.newPublisher(PublisherConfig{Name: defaultPublisherName}.Sanitize())
}

func (r *Broker) NewPublisher(name string) (worker.IPublisher, error) {
	conf, ok := r.publishers[name]
	if !ok {
		return nil, fmt.Errorf("rabbitmq: publisher config not found for name: %s", name)
	}
	return r.newPublisher(conf)
}

// newPublisher opens a channel in confirm mode, so Publish returns once the broker has the message.
func (r *Broker) newPublisher(conf PublisherConfig) (*Publisher, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open publisher channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return nil, errors.Join(fmt.Errorf("rabbitmq: enable publisher confirms: %w", err), ch.Close())
	}

	p := &Publisher{conf: conf, ch: ch}
	if conf.Mandatory {
		p.returns = ch.NotifyReturn(make(chan amqp.Return, returnBuffer))
	}
	r.own(ch, nil)
	p.release = func() { r.disown(ch) }
	return p, nil
}

// NewSubscriber opens the subscriber's channel and checks that its queue exists. Consumption
// starts in Consume, once the worker's concurrency sets the default prefetch.
func (r *Broker) NewSubscriber(name string) (worker.ISubscriber, error) {
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

	guard := &ackGuard{}
	r.own(ch, guard)
	retry, err := r.newRetrier(conf.Queue)
	if err != nil {
		r.disown(ch)
		return nil, errors.Join(fmt.Errorf("rabbitmq: subscriber %s: %w", name, err), ch.Close())
	}
	tag, lost := r.addConsumer(name)
	return &Subscriber{
		conf:  conf,
		ch:    ch,
		tag:   tag,
		guard: guard,
		lost:  lost,
		retry: retry,
		release: func() {
			r.disown(ch)
			r.removeConsumer(tag)
		},
	}, nil
}

// newRetrier returns the retrier of a queue that has Retry, with a publisher of its own, or nil.
func (r *Broker) newRetrier(queue string) (*retrier, error) {
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

func (r *Broker) Value() any {
	return worker.IBroker(r)
}

// Close closes the connection and with it every channel. It also stops a recovery in progress.
func (r *Broker) Close() error {
	return r.conn.Close()
}

func (r *Broker) own(ch *amqp.Channel, guard *ackGuard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.owned[ch] = guard
}

func (r *Broker) disown(ch *amqp.Channel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.owned, ch)
}

func (r *Broker) owner(ch *amqp.Channel) (*ackGuard, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	guard, ok := r.owned[ch]
	return guard, ok
}

// addConsumer returns a new consumer tag for the subscriber name, and the loss the recovery
// reports to when it cannot re-subscribe that consumer.
func (r *Broker) addConsumer(name string) (string, *consumerLoss) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consumerSeq++
	tag := consumerTag(name, r.consumerSeq)
	lost := newConsumerLoss()
	r.consumers[tag] = lost
	return tag, lost
}

func (r *Broker) removeConsumer(tag string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.consumers, tag)
}

// consumerLost reports err to the subscriber that consumes under tag, unless it is closed.
func (r *Broker) consumerLost(tag string, err error) {
	r.mu.Lock()
	lost, ok := r.consumers[tag]
	r.mu.Unlock()
	if ok {
		lost.report(err)
	}
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

// invalidateAll marks the deliveries of every subscriber channel as stale.
func (r *Broker) invalidateAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, guard := range r.owned {
		if guard != nil {
			guard.invalidate()
		}
	}
}

func (r *Broker) init() (err error) {
	cfg := amqp.Config{
		Vhost:      r.conf.VHost,
		Properties: amqp.NewConnectionProperties(),
		Recovery: r.conf.Recovery.amqpRecovery(&recoveryHooks{
			broker: r,
			next:   &amqp.DefaultConnectionRecovery{},
		}),
	}
	if r.conf.Username != "" {
		cfg.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: r.conf.Username, Password: r.conf.Password}}
	}
	if r.conf.Name != "" {
		cfg.Properties.SetClientConnectionName(r.conf.Name)
	}

	// Credentials and the vhost travel in cfg, so the URL needs no escaping.
	conn, err := amqp.DialConfig("amqp://"+net.JoinHostPort(r.conf.Addr, r.conf.Port)+"/", cfg)
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
	r.own(ch, nil)

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
	}

	return nil
}
