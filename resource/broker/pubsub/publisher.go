package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/efegreat/forgo/server/worker"

	gpubsub "cloud.google.com/go/pubsub/v2"
)

var (
	// errNoTopic is returned for a message without a Destination, which names its topic.
	errNoTopic = errors.New("pubsub: message has no Destination topic")
	// errClosed is returned by a publish once the broker is closed.
	errClosed = errors.New("pubsub: broker is closed")
)

// publisher publishes to the topic of each message. Publish returns once Pub/Sub has stored the
// message. The outbox only marks a message processed after that.
//
// Every publisher of a broker shares one client publisher per topic, so the publishes the worker
// runs in parallel are sent to Pub/Sub in batches.
type publisher struct {
	topics *topics
}

// Publish sends msg to the topic msg.Destination with the ordering key msg.Key, and waits until
// Pub/Sub has stored it or ctx ends.
func (p *publisher) Publish(ctx context.Context, msg worker.Message) error {
	if msg.Destination == "" {
		return fmt.Errorf("%w: %s", errNoTopic, msg.ID)
	}
	if err := p.topics.publish(ctx, msg.Destination, message(msg)); err != nil {
		return fmt.Errorf("pubsub: publish %s to topic %s: %w", msg.ID, msg.Destination, err)
	}
	return nil
}

// Close does nothing: the client publishers belong to the broker, which stops them on Close.
func (p *publisher) Close() error {
	return nil
}

// message converts msg to a Pub/Sub message. The identity attributes are written from the message
// fields, so the consuming side rebuilds the same ID, name and creation time.
func message(msg worker.Message) *gpubsub.Message {
	attributes := make(map[string]string)
	if msg.Headers != nil {
		msg.Headers.Range(func(k, v string) bool {
			attributes[k] = v
			return true
		})
	}
	attributes[worker.MessageIDHeader] = msg.ID
	attributes[worker.MessageNameHeader] = msg.Name
	if !msg.CreatedAt.IsZero() {
		attributes[worker.MessageTimestampHeader] = strconv.FormatInt(msg.CreatedAt.UnixNano(), 10)
	}

	return &gpubsub.Message{
		Data:        msg.Payload,
		Attributes:  attributes,
		OrderingKey: msg.Key,
	}
}

// topics publishes to topics by name through one client publisher per topic, created on first
// use. A client publisher batches the messages published to it concurrently.
type topics struct {
	client *gpubsub.Client

	mu      sync.Mutex
	stopped bool
	byName  map[string]*gpubsub.Publisher
}

func newTopics(client *gpubsub.Client) *topics {
	return &topics{client: client, byName: make(map[string]*gpubsub.Publisher)}
}

// publish sends m to the topic name and waits until Pub/Sub has stored it or ctx ends.
//
// After a failed publish, the client pauses the publishes of m's ordering key, so that later
// messages of the key do not overtake m. publish resumes them at once: the outbox publishes m
// again on a later cycle, and does not keep the order of its messages anyway.
func (t *topics) publish(ctx context.Context, name string, m *gpubsub.Message) error {
	p, err := t.get(name)
	if err != nil {
		return err
	}
	if _, err := p.Publish(ctx, m).Get(ctx); err != nil {
		if m.OrderingKey != "" {
			p.ResumePublish(m.OrderingKey)
		}
		return err
	}
	return nil
}

// get returns the client publisher of the topic name.
func (t *topics) get(name string) (*gpubsub.Publisher, error) {
	name = topicName(t.client.Project(), name)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return nil, errClosed
	}
	p, ok := t.byName[name]
	if !ok {
		p = t.client.Publisher(name)
		// Message.Key is the ordering key. A message without one is published unordered.
		p.EnableMessageOrdering = true
		t.byName[name] = p
	}
	return p, nil
}

// stop sends the messages still being published and stops every client publisher.
func (t *topics) stop() {
	t.mu.Lock()
	t.stopped = true
	publishers := t.byName
	t.byName = nil
	t.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range publishers {
		wg.Go(p.Stop)
	}
	wg.Wait()
}
