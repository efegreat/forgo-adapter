package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/efegreat/forgo/server/worker"

	gpubsub "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// broker is the resource New returns and its worker.IBroker Value. Its publishers share one client
// publisher per topic, which batches their messages.
type broker struct {
	conf   Config
	client *gpubsub.Client
	topics *topics
	// subscribers holds the config of every subscriber by name, with its Subscription and
	// DeadLetterTopic resolved to full names.
	subscribers map[string]SubscriberConfig
}

func newBroker(cfg Config) (_ *broker, err error) {
	// The client keeps the context it is created with for its credentials, so it must outlive New.
	client, err := gpubsub.NewClient(context.Background(), cfg.ProjectID, cfg.Options...)
	if err != nil {
		return nil, fmt.Errorf("pubsub: create client: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, client.Close())
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DeclareTimeout)
	defer cancel()
	if err := declare(ctx, client, cfg); err != nil {
		return nil, err
	}

	b := &broker{
		conf:        cfg,
		client:      client,
		topics:      newTopics(client),
		subscribers: make(map[string]SubscriberConfig, len(cfg.Subscribers)),
	}
	for _, s := range cfg.Subscribers {
		b.subscribers[s.Name] = resolveSubscriber(cfg, client.Project(), s)
	}
	return b, nil
}

// resolveSubscriber returns s with its Subscription and DeadLetterTopic as full names in project,
// and the dead-letter topic of its subscription in cfg when it has none of its own.
func resolveSubscriber(cfg Config, project string, s SubscriberConfig) SubscriberConfig {
	s.Subscription = subscriptionName(project, s.Subscription)
	if s.DeadLetterTopic != "" {
		s.DeadLetterTopic = topicName(project, s.DeadLetterTopic)
		return s
	}
	for _, sub := range cfg.Subscriptions {
		if sub.DeadLetter.Enabled && subscriptionName(project, sub.Name) == s.Subscription {
			s.DeadLetterTopic = topicName(project, sub.DeadLetter.Topic)
			break
		}
	}
	return s
}

// Name returns Config.Name.
func (b *broker) Name() string {
	return b.conf.Name
}

// NewDefaultPublisher returns a publisher. Pub/Sub publishers have no settings of their own.
func (b *broker) NewDefaultPublisher() (worker.IPublisher, error) {
	return &publisher{topics: b.topics}, nil
}

// NewPublisher returns a publisher for any name. Pub/Sub publishers have no settings of their own.
func (b *broker) NewPublisher(string) (worker.IPublisher, error) {
	return &publisher{topics: b.topics}, nil
}

// NewSubscriber returns the subscriber name. It does not check that its subscription exists,
// because the subscriber role cannot read subscriptions: receiving from a missing one fails
// instead.
func (b *broker) NewSubscriber(name string) (worker.ISubscriber, error) {
	conf, ok := b.subscribers[name]
	if !ok {
		return nil, fmt.Errorf("pubsub: subscriber config not found for name: %s", name)
	}
	s := &subscriber{conf: conf, sub: b.client.Subscriber(conf.Subscription)}
	if conf.DeadLetterTopic != "" {
		s.deadLetter = &deadLetter{topics: b.topics, topic: conf.DeadLetterTopic}
	}
	return s, nil
}

// Value returns the broker as a worker.IBroker.
func (b *broker) Value() any {
	return worker.IBroker(b)
}

// Close sends the messages still being published, then closes the client.
func (b *broker) Close() error {
	b.topics.stop()
	return b.client.Close()
}

// declare creates the topics, then every subscription after its dead-letter topic and
// subscription, so a dead-letter policy never points at a topic that does not exist yet.
func declare(ctx context.Context, client *gpubsub.Client, cfg Config) error {
	project := client.Project()
	for _, t := range cfg.Topics {
		if err := createTopic(ctx, client, topicName(project, t.Name)); err != nil {
			return err
		}
	}

	for _, s := range cfg.Subscriptions {
		if dl := s.DeadLetter; dl.Enabled {
			topic := topicName(project, dl.Topic)
			if err := createTopic(ctx, client, topic); err != nil {
				return err
			}
			dlq := &pb.Subscription{
				Name:             subscriptionName(project, dl.Subscription),
				Topic:            topic,
				ExpirationPolicy: &pb.ExpirationPolicy{},
			}
			// The dead-letter subscription only has to receive the dead letters: its other
			// settings, such as its retention, are left to the user.
			if err := createSubscription(ctx, client, dlq, topicDifference); err != nil {
				return err
			}
		}
		if err := createSubscription(ctx, client, s.subscription(project), differences); err != nil {
			return err
		}
	}
	return nil
}

// createTopic creates the topic name unless it exists.
func createTopic(ctx context.Context, client *gpubsub.Client, name string) error {
	_, err := client.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: name})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("pubsub: create topic %s: %w", name, err)
	}
	return nil
}

// createSubscription creates want unless it exists. An existing subscription must have no
// differences from want, because most subscription settings cannot change after it is created
// and New never changes the others behind the user's back.
func createSubscription(ctx context.Context, client *gpubsub.Client, want *pb.Subscription, diff func(want, got *pb.Subscription) []string) error {
	_, err := client.SubscriptionAdminClient.CreateSubscription(ctx, want)
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("pubsub: create subscription %s: %w", want.Name, err)
	}

	got, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: want.Name})
	if err != nil {
		return fmt.Errorf("pubsub: get subscription %s: %w", want.Name, err)
	}
	if d := diff(want, got); len(d) > 0 {
		return fmt.Errorf("%w: %s has another %s; update or recreate it", errSubscriptionDiffers, want.Name, strings.Join(d, ", "))
	}
	return nil
}

// errSubscriptionDiffers is returned when a configured subscription exists with other settings.
var errSubscriptionDiffers = errors.New("pubsub: subscription exists with other settings")

// differences names the settings of want that got does not have. The expiration policy is not
// compared: New only sets it on the subscriptions it creates.
func differences(want, got *pb.Subscription) []string {
	d := topicDifference(want, got)
	if want.Filter != got.Filter {
		d = append(d, "filter")
	}
	if want.EnableMessageOrdering != got.EnableMessageOrdering {
		d = append(d, "message ordering")
	}
	if want.EnableExactlyOnceDelivery != got.EnableExactlyOnceDelivery {
		d = append(d, "exactly-once delivery")
	}
	if !proto.Equal(want.RetryPolicy, got.RetryPolicy) {
		d = append(d, "retry policy")
	}
	if !proto.Equal(want.DeadLetterPolicy, got.DeadLetterPolicy) {
		d = append(d, "dead-letter policy")
	}
	return d
}

// topicDifference names the topic when got receives from another topic than want, for example
// because its topic was deleted.
func topicDifference(want, got *pb.Subscription) []string {
	if want.Topic != got.Topic {
		return []string{"topic"}
	}
	return nil
}
