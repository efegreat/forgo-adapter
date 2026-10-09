package rabbitmq

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/efegreat/forgo"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultAddr  = "localhost"
	defaultPort  = "5672"
	defaultVHost = "/"

	deadLetterExchangeArg   = "x-dead-letter-exchange"
	deadLetterRoutingKeyArg = "x-dead-letter-routing-key"

	defaultRetryInterval = 5 * time.Second
	maxPrefetch          = math.MaxUint16
)

var errDeadLetterUnnamedQueue = errors.New("rabbitmq: a queue with DeadLetter enabled needs a Name")

// Config is a RabbitMQ broker resource. Its Value is the worker.IBroker the worker servers resolve.
//
// New connects and declares every exchange, queue, binding and dead-letter queue, so a topology
// error fails the bootstrap instead of the first publish or consume.
type Config struct {
	Name string
	// Addr is the broker host. Default: localhost.
	Addr string
	// Port is the broker port. Default: 5672.
	Port string
	// Username and Password authenticate with PLAIN. Without a Username the client default applies (guest).
	Username string
	Password string
	// VHost is the virtual host. Default: /.
	VHost string
	// Recovery reconnects after the connection or a channel is lost. Enabled by default.
	Recovery    RecoveryConfig
	Exchanges   []ExchangeConfig
	Queues      []QueueConfig
	Publishers  []PublisherConfig
	Subscribers []SubscriberConfig
}

// Sanitize returns a copy of c with defaults applied. It never modifies the slices of c.
func (c Config) Sanitize() Config {
	if c.Addr == "" {
		c.Addr = defaultAddr
	}
	if c.Port == "" {
		c.Port = defaultPort
	}
	if c.VHost == "" {
		c.VHost = defaultVHost
	}
	c.Recovery = c.Recovery.Sanitize()

	queues := make([]QueueConfig, len(c.Queues))
	for i, q := range c.Queues {
		queues[i] = q.Sanitize()
	}
	c.Queues = queues

	publishers := make([]PublisherConfig, len(c.Publishers))
	for i, p := range c.Publishers {
		publishers[i] = p.Sanitize()
	}
	c.Publishers = publishers

	return c
}

func (c Config) New() (forgo.Resource, error) {
	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	broker := &Broker{
		conf:        cfg,
		subscribers: make(map[string]SubscriberConfig, len(cfg.Subscribers)),
		publishers:  make(map[string]PublisherConfig, len(cfg.Publishers)),
		owned:       make(map[*amqp.Channel]*ackGuard),
	}
	if err := broker.init(); err != nil {
		return nil, err
	}

	return broker, nil
}

// validate rejects configurations that would otherwise only fail once the broker is reached.
func (c Config) validate() error {
	if c.Recovery.MaxRetries < 0 || c.Recovery.RetryInterval < 0 {
		return errors.New("rabbitmq: Recovery.MaxRetries and Recovery.RetryInterval cannot be negative")
	}
	for _, s := range c.Subscribers {
		if s.Prefetch > maxPrefetch {
			return fmt.Errorf("rabbitmq: subscriber %q: Prefetch cannot exceed %d", s.Name, maxPrefetch)
		}
	}
	for _, q := range c.Queues {
		if !q.DeadLetter.Enabled {
			continue
		}
		if q.Name == "" {
			return errDeadLetterUnnamedQueue
		}
		for _, arg := range []string{deadLetterExchangeArg, deadLetterRoutingKeyArg} {
			if _, ok := q.Args[arg]; ok {
				return fmt.Errorf("rabbitmq: queue %q sets %s in Args and enables DeadLetter; use one of them", q.Name, arg)
			}
		}
	}
	return nil
}

// RecoveryConfig configures the automatic recovery of the client (amqp091-go). After the connection
// or a channel is lost, it reconnects, reopens every channel with its prefetch and confirm mode,
// redeclares the exchanges, queues and bindings, and resubscribes the consumers.
//
// Messages that were delivered but not yet acknowledged when the connection was lost are
// redelivered by the broker. Their late acknowledgements are dropped with errStaleDelivery
// instead of reaching the recovered channel, whose delivery tags start over.
type RecoveryConfig struct {
	// Disabled turns recovery off: a lost connection stops every subscriber and fails every
	// publish until the application restarts.
	Disabled bool
	// MaxRetries is the number of reconnection attempts before giving up. Default: unlimited.
	MaxRetries int
	// RetryInterval is the wait before each attempt, plus up to 500ms of jitter. Default: 5 seconds.
	RetryInterval time.Duration
}

func (r RecoveryConfig) Sanitize() RecoveryConfig {
	if r.MaxRetries == 0 {
		r.MaxRetries = math.MaxInt
	}
	if r.RetryInterval == 0 {
		r.RetryInterval = defaultRetryInterval
	}
	return r
}

type ExchangeConfig struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]any
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]any
	Bindings   []BindingConfig
	// DeadLetter routes the messages this queue dead-letters to a dead-letter queue instead of
	// dropping them. Disabled by default.
	DeadLetter DeadLetterConfig
}

// Sanitize returns a copy of q with the dead-letter defaults applied.
func (q QueueConfig) Sanitize() QueueConfig {
	q.DeadLetter = q.DeadLetter.sanitize(q.Name)
	return q
}

// args returns the arguments to declare q with: Args, plus the dead-letter arguments when
// DeadLetter is enabled. Args itself is never modified, because configs are shared values.
func (q QueueConfig) args() amqp.Table {
	if !q.DeadLetter.Enabled {
		return q.Args
	}
	args := make(amqp.Table, len(q.Args)+2)
	for k, v := range q.Args {
		args[k] = v
	}
	args[deadLetterExchangeArg] = q.DeadLetter.Exchange
	args[deadLetterRoutingKeyArg] = q.DeadLetter.RoutingKey
	return args
}

// DeadLetterConfig declares a dead-letter exchange and queue for a queue, and points the queue at
// them with the x-dead-letter-exchange and x-dead-letter-routing-key arguments.
//
// RabbitMQ dead-letters the messages the worker rejects: messages whose attempts reached
// MaxAttempt, messages that cannot be bound and messages no handler is registered for. Messages
// that expire or overflow the queue are dead-lettered as well. Retried messages are requeued, not
// dead-lettered.
//
// The exchange is a durable direct exchange and the queue is durable, so dead letters survive a
// broker restart.
type DeadLetterConfig struct {
	// Enabled declares the dead-letter exchange and queue.
	Enabled bool
	// Exchange is the dead-letter exchange. Default: "<queue>.dlx".
	Exchange string
	// Queue is the dead-letter queue. Default: "<queue>.dlq".
	Queue string
	// RoutingKey binds Queue to Exchange and replaces the routing key of dead-lettered messages.
	// Default: the name of the queue.
	RoutingKey string
	// Args are the arguments of the dead-letter queue, such as x-message-ttl or x-queue-type.
	Args map[string]any
}

func (d DeadLetterConfig) sanitize(queue string) DeadLetterConfig {
	if !d.Enabled {
		return d
	}
	if d.Exchange == "" {
		d.Exchange = queue + ".dlx"
	}
	if d.Queue == "" {
		d.Queue = queue + ".dlq"
	}
	if d.RoutingKey == "" {
		d.RoutingKey = queue
	}
	return d
}

type BindingConfig struct {
	Exchange   string
	RoutingKey string
	NoWait     bool
	Args       map[string]any
}

type PublisherConfig struct {
	// Name matches worker.PublisherConfig.Name.
	Name string
	// Mandatory makes a publish fail when no queue is bound to receive the message. Otherwise
	// RabbitMQ drops unroutable messages and the publish succeeds.
	Mandatory bool
	// DeliveryMode is amqp.Persistent or amqp.Transient. Default: amqp.Persistent.
	DeliveryMode uint8
}

func (p PublisherConfig) Sanitize() PublisherConfig {
	if p.DeliveryMode == 0 {
		p.DeliveryMode = amqp.Persistent
	}
	return p
}

type SubscriberConfig struct {
	// Name matches worker.SubscriberConfig.Name.
	Name  string
	Queue string
	// Prefetch is how many messages RabbitMQ delivers before they are acknowledged. Default: the
	// Concurrency of the worker subscriber, so every handler slot has a message and the rest of
	// the backlog stays in the queue for other instances. A negative value removes the limit.
	Prefetch  int
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      map[string]any
}
