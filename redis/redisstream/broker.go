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

// broker is the resource New returns and its worker.IBroker Value. It owns its Redis client.
type broker struct {
	name string
	// conn is the redisdb resource of the client, which closes it.
	conn        forgo.Resource
	client      redis.UniversalClient
	streams     *streams
	subscribers map[string]SubscriberConfig
}

func newBroker(cfg Config) (_ *broker, err error) {
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

	b := &broker{
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
func (b *broker) Name() string {
	return b.name
}

// NewDefaultPublisher returns a publisher. Redis Streams publishers have no settings of their own.
func (b *broker) NewDefaultPublisher() (worker.IPublisher, error) {
	return &publisher{streams: b.streams}, nil
}

// NewPublisher returns a publisher for any name. Redis Streams publishers have no settings of their
// own.
func (b *broker) NewPublisher(string) (worker.IPublisher, error) {
	return &publisher{streams: b.streams}, nil
}

// NewSubscriber returns the subscriber name, which reads as a consumer of its own and handles
// opts.Concurrency entries at once. Creating the consumer checks that the stream and the group
// exist, so a missing one fails the server at startup.
func (b *broker) NewSubscriber(name string, opts worker.SubscriberOptions) (worker.ISubscriber, error) {
	conf, ok := b.subscribers[name]
	if !ok {
		return nil, fmt.Errorf("redisstream: subscriber config not found for name: %s", name)
	}
	s := newSubscriber(b.client, b.streams, conf, consumerName(), opts.Concurrency)
	ctx := context.Background()
	// Out of memory, Redis refuses to create the consumer, which the subscriber's first read creates.
	err := b.client.XGroupCreateConsumer(ctx, conf.Stream, conf.Group, s.consumer).Err()
	if err != nil && !(outOfMemory(err) && hasGroup(ctx, b.client, conf.Stream, conf.Group)) {
		return nil, fmt.Errorf("redisstream: subscriber %s: create consumer in group %s of stream %s: %w", name, conf.Group, conf.Stream, err)
	}
	return s, nil
}

// Value returns the broker as a worker.IBroker.
func (b *broker) Value() any {
	return worker.IBroker(b)
}

// Close closes the client.
func (b *broker) Close() error {
	return b.conn.Close()
}

// declare creates every group of streams that does not exist, and its stream when needed. An
// existing group is kept as it is. Out of memory, Redis refuses to create a group even when it
// exists, so declare then only checks that it does.
func declare(ctx context.Context, client redis.UniversalClient, streams []StreamConfig) error {
	for _, s := range streams {
		for _, g := range s.Groups {
			err := client.XGroupCreateMkStream(ctx, s.Name, g.Name, g.StartID).Err()
			if err == nil || redis.HasErrorPrefix(err, "BUSYGROUP") || outOfMemory(err) && hasGroup(ctx, client, s.Name, g.Name) {
				continue
			}
			return fmt.Errorf("redisstream: create group %s of stream %s: %w", g.Name, s.Name, err)
		}
	}
	return nil
}

// outOfMemory reports the reply of Redis to a command it refuses while it is out of memory with the
// noeviction policy: the commands that may add data, such as XADD, XGROUP CREATE and XGROUP
// CREATECONSUMER, even when they would add nothing. Reads, claims and acknowledgements still work.
func outOfMemory(err error) bool {
	return redis.HasErrorPrefix(err, "OOM ")
}

// hasGroup reports whether stream has the consumer group, through XINFO GROUPS, which Redis answers
// while it is out of memory.
func hasGroup(ctx context.Context, client redis.UniversalClient, stream, group string) bool {
	groups, err := client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return false
	}
	for _, g := range groups {
		if g.Name == group {
			return true
		}
	}
	return false
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
