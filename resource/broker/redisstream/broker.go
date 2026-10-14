package redisstream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/worker"

	"github.com/redis/go-redis/v9"
)

// Broker is the resource New returns and its worker.IBroker Value. It owns its Redis client.
type Broker struct {
	name string
	// conn is the redisdb resource of the client, which closes it.
	conn        forgo.Resource
	client      redis.UniversalClient
	streams     *streams
	subscribers map[string]SubscriberConfig
}

func newBroker(cfg Config) (_ *Broker, err error) {
	conn, err := cfg.Redis.New()
	if err != nil {
		return nil, fmt.Errorf("redisstream: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, conn.Close())
		}
	}()
	client := conn.Value().(redis.UniversalClient)

	if err := declare(context.Background(), client, cfg.Streams); err != nil {
		return nil, err
	}

	b := &Broker{
		name:        cfg.Name,
		conn:        conn,
		client:      client,
		streams:     newStreams(client, cfg.Streams),
		subscribers: make(map[string]SubscriberConfig, len(cfg.Subscribers)),
	}
	for _, s := range cfg.Subscribers {
		b.subscribers[s.Name] = s
	}
	return b, nil
}

// Name returns Config.Name.
func (b *Broker) Name() string {
	return b.name
}

// NewDefaultPublisher returns a publisher. Redis Streams publishers have no settings of their own.
func (b *Broker) NewDefaultPublisher() (worker.IPublisher, error) {
	return &Publisher{streams: b.streams}, nil
}

// NewPublisher returns a publisher for any name. Redis Streams publishers have no settings of their
// own.
func (b *Broker) NewPublisher(string) (worker.IPublisher, error) {
	return &Publisher{streams: b.streams}, nil
}

// NewSubscriber returns the subscriber name, which reads as a consumer of its own. Creating the
// consumer checks that the stream and the group exist, so a missing one fails the server at
// startup.
func (b *Broker) NewSubscriber(name string) (worker.ISubscriber, error) {
	conf, ok := b.subscribers[name]
	if !ok {
		return nil, fmt.Errorf("redisstream: subscriber config not found for name: %s", name)
	}
	s := newSubscriber(b.client, b.streams, conf, consumerName())
	if err := b.client.XGroupCreateConsumer(context.Background(), conf.Stream, conf.Group, s.consumer).Err(); err != nil {
		return nil, fmt.Errorf("redisstream: subscriber %s: create consumer in group %s of stream %s: %w", name, conf.Group, conf.Stream, err)
	}
	return s, nil
}

// Value returns the broker as a worker.IBroker.
func (b *Broker) Value() any {
	return worker.IBroker(b)
}

// Close closes the client.
func (b *Broker) Close() error {
	return b.conn.Close()
}

// declare creates every group of streams that does not exist, and its stream when needed. An
// existing group is kept as it is.
func declare(ctx context.Context, client redis.UniversalClient, streams []StreamConfig) error {
	for _, s := range streams {
		for _, g := range s.Groups {
			err := client.XGroupCreateMkStream(ctx, s.Name, g.Name, g.StartID).Err()
			if err != nil && !redis.HasErrorPrefix(err, "BUSYGROUP") {
				return fmt.Errorf("redisstream: create group %s of stream %s: %w", g.Name, s.Name, err)
			}
		}
	}
	return nil
}

// consumerName returns a consumer name unique to a subscriber of this process: the host name, which
// tells operators which instance a consumer belongs to, and a random suffix.
func consumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "forgo"
	}
	return host + "-" + rand.Text()[:10]
}
