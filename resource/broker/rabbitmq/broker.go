package rabbitmq

import (
	"errors"
	"fmt"
	"net"

	"github.com/efegreat/forgo/server/worker"

	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultPublisherName = "default"

type Broker struct {
	conf        Config
	subscribers map[string]SubscriberConfig
	publishers  map[string]PublisherConfig
	conn        *amqp.Connection
}

func (r *Broker) Name() string {
	return r.conf.Name
}

func (r *Broker) NewDefaultPublisher() (worker.IPublisher, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open publisher channel: %w", err)
	}

	return &Publisher{
		conf: PublisherConfig{Name: defaultPublisherName}.Sanitize(),
		ch:   ch,
	}, nil
}

func (r *Broker) NewPublisher(name string) (worker.IPublisher, error) {
	conf, ok := r.publishers[name]
	if !ok {
		return nil, fmt.Errorf("rabbitmq: publisher config not found for name: %s", name)
	}
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open publisher channel: %w", err)
	}

	return &Publisher{
		conf: conf,
		ch:   ch,
	}, nil
}

func (r *Broker) NewSubscriber(name string) (worker.ISubscriber, error) {
	conf, ok := r.subscribers[name]
	if !ok {
		return nil, fmt.Errorf("rabbitmq: subscriber config not found for name: %s", name)
	}
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open subscriber channel: %w", err)
	}

	msgs, err := ch.Consume(
		conf.Queue,
		"",
		conf.AutoAck,
		conf.Exclusive,
		conf.NoLocal,
		conf.NoWait,
		conf.Args,
	)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("rabbitmq: consume queue %s: %w", conf.Queue, err),
			ch.Close(),
		)
	}

	return &Subscriber{
		msgs: msgs,
		ch:   ch,
	}, nil
}

func (r *Broker) Value() any {
	return worker.IBroker(r)
}

func (r *Broker) Close() error {
	return r.conn.Close()
}

func (r *Broker) init() (err error) {
	cfg := amqp.Config{
		Vhost:      r.conf.VHost,
		Properties: amqp.NewConnectionProperties(),
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
	defer ch.Close()

	if err := declare(ch, r.conf); err != nil {
		return err
	}

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
// queue never points at a dead-letter exchange that does not exist yet.
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
	}

	return nil
}
