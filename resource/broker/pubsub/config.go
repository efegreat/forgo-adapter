package pubsub

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/efegreat/forgo"

	gpubsub "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultDeclareTimeout = 30 * time.Second

	// maxBackoff is the longest backoff of a retry policy, and Pub/Sub's default maximum.
	maxBackoff = 10 * time.Minute

	// minDeliveryAttempts and maxDeliveryAttempts bound the MaxDeliveryAttempts of a dead-letter
	// policy.
	minDeliveryAttempts = 5
	maxDeliveryAttempts = 100
)

// Config is a Google Cloud Pub/Sub broker resource. Its Value is the worker.IBroker the worker
// servers resolve.
//
// New creates the topics and subscriptions it is given that do not exist yet, and checks the
// settings of the subscriptions that do, so a topology error fails the bootstrap instead of the
// first publish or receive.
//
// A topic or subscription is an ID in ProjectID, or a full name such as
// projects/<project>/topics/<topic> for one in another project.
type Config struct {
	Name string
	// ProjectID is the project of the topics and subscriptions. Default: the GOOGLE_CLOUD_PROJECT
	// environment variable, or the project of the credentials. The emulator needs it set.
	ProjectID string
	// Options configure the client, for example with option.WithCredentialsFile or
	// option.WithEndpoint. Default: Application Default Credentials. When PUBSUB_EMULATOR_HOST is
	// set, the client connects to the emulator at that address without credentials.
	Options []option.ClientOption
	// DeclareTimeout bounds the calls New makes to create and check the topics and subscriptions.
	// Default: 30 seconds.
	DeclareTimeout time.Duration
	// Topics are created when they do not exist.
	Topics []TopicConfig
	// Subscriptions are created when they do not exist, after their dead-letter topic and
	// subscription. An existing subscription must have the configured settings.
	Subscriptions []SubscriptionConfig
	Subscribers   []SubscriberConfig
}

// Sanitize returns a copy of c with defaults applied. It never modifies the slices of c.
func (c Config) Sanitize() Config {
	if c.ProjectID == "" {
		c.ProjectID = gpubsub.DetectProjectID
	}
	if c.DeclareTimeout == 0 {
		c.DeclareTimeout = defaultDeclareTimeout
	}

	subscriptions := make([]SubscriptionConfig, len(c.Subscriptions))
	for i, s := range c.Subscriptions {
		subscriptions[i] = s.Sanitize()
	}
	c.Subscriptions = subscriptions

	return c
}

func (c Config) New() (forgo.Resource, error) {
	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newBroker(cfg)
}

// validate rejects configurations that would otherwise only fail once Pub/Sub is reached.
func (c Config) validate() error {
	if c.DeclareTimeout < 0 {
		return errors.New("pubsub: DeclareTimeout cannot be negative")
	}
	for _, t := range c.Topics {
		if t.Name == "" {
			return errors.New("pubsub: a topic needs a Name")
		}
	}
	for _, s := range c.Subscriptions {
		if err := s.validate(); err != nil {
			return err
		}
	}
	for _, s := range c.Subscribers {
		if s.Subscription == "" {
			return fmt.Errorf("pubsub: subscriber %q needs a Subscription", s.Name)
		}
	}
	return nil
}

type TopicConfig struct {
	// Name is the topic.
	Name string
}

type SubscriptionConfig struct {
	// Name is the subscription.
	Name string
	// Topic is the topic the subscription receives from.
	Topic string
	// Filter delivers only the messages whose attributes match it, such as
	// attributes.tenant = "acme". Pub/Sub acknowledges the others itself.
	Filter string
	// Ordering delivers the messages that share an ordering key (Message.Key) one at a time and in
	// the order Pub/Sub received them.
	Ordering bool
	// ExactlyOnce enables exactly-once delivery: an acknowledged message is not delivered again,
	// and acknowledgements wait for Pub/Sub to confirm them.
	ExactlyOnce bool
	// Retry delays the redelivery of the messages the worker retries. Disabled by default: a
	// retried message is delivered again at once.
	Retry RetryConfig
	// DeadLetter keeps the messages the worker rejects in a dead-letter topic. Disabled by default:
	// rejected messages are dropped.
	DeadLetter DeadLetterConfig
}

// Sanitize returns a copy of s with the dead-letter and retry defaults applied.
func (s SubscriptionConfig) Sanitize() SubscriptionConfig {
	s.DeadLetter = s.DeadLetter.sanitize(s.Name)
	s.Retry = s.Retry.sanitize()
	return s
}

func (s SubscriptionConfig) validate() error {
	if s.Name == "" {
		return errors.New("pubsub: a subscription needs a Name")
	}
	if s.Topic == "" {
		return fmt.Errorf("pubsub: subscription %q needs a Topic", s.Name)
	}
	if err := s.Retry.validate(s.Name); err != nil {
		return err
	}
	return s.DeadLetter.validate(s.Name)
}

// subscription returns the subscription New creates for s, whose topics and subscriptions are
// full names in project. It never expires, unlike a subscription created with Pub/Sub's defaults
// after 31 days without subscribers.
func (s SubscriptionConfig) subscription(project string) *pb.Subscription {
	sub := &pb.Subscription{
		Name:                      subscriptionName(project, s.Name),
		Topic:                     topicName(project, s.Topic),
		Filter:                    s.Filter,
		EnableMessageOrdering:     s.Ordering,
		EnableExactlyOnceDelivery: s.ExactlyOnce,
		ExpirationPolicy:          &pb.ExpirationPolicy{},
	}
	if s.Retry.enabled() {
		sub.RetryPolicy = &pb.RetryPolicy{
			MinimumBackoff: durationpb.New(s.Retry.Initial),
			MaximumBackoff: durationpb.New(s.Retry.Max),
		}
	}
	if dl := s.DeadLetter; dl.Enabled && dl.MaxDeliveryAttempts > 0 {
		sub.DeadLetterPolicy = &pb.DeadLetterPolicy{
			DeadLetterTopic:     topicName(project, dl.Topic),
			MaxDeliveryAttempts: int32(dl.MaxDeliveryAttempts),
		}
	}
	return sub
}

// RetryConfig sets the retry policy of a subscription, which delays the redelivery of the messages
// the worker retries. The delay starts at Initial after the first failed delivery and grows
// exponentially with every further one, up to Max.
//
// Pub/Sub counts the deliveries itself and cannot delay a single message, so the attempt the
// worker passes to Retry is not used, and the retries that count no attempt are delayed as well.
type RetryConfig struct {
	// Initial is the delay after the first failed delivery, at most 10 minutes. Zero leaves the
	// subscription without a retry policy: a retried message is delivered again at once.
	Initial time.Duration
	// Max is the longest delay, at most 10 minutes. Default: 10 minutes.
	Max time.Duration
}

func (r RetryConfig) enabled() bool {
	return r.Initial > 0
}

func (r RetryConfig) sanitize() RetryConfig {
	if r.enabled() && r.Max == 0 {
		r.Max = maxBackoff
	}
	return r
}

func (r RetryConfig) validate(subscription string) error {
	if r.Initial < 0 || r.Max < 0 {
		return fmt.Errorf("pubsub: subscription %q: Retry.Initial and Retry.Max cannot be negative", subscription)
	}
	if !r.enabled() {
		return nil
	}
	switch {
	case r.Initial > maxBackoff || r.Max > maxBackoff:
		return fmt.Errorf("pubsub: subscription %q: Retry.Initial and Retry.Max cannot exceed %s", subscription, maxBackoff)
	case r.Max < r.Initial:
		return fmt.Errorf("pubsub: subscription %q: Retry.Max cannot be shorter than Retry.Initial", subscription)
	}
	return nil
}

// DeadLetterConfig keeps the messages the worker rejects in a dead-letter topic instead of
// dropping them.
//
// New creates the dead-letter topic and a subscription to it before the subscription itself.
// The dead-letter subscription keeps the dead letters, because Pub/Sub drops the messages of a
// topic without subscriptions. To reject a message, the subscriber publishes a copy of it to the
// dead-letter topic, waits until Pub/Sub has stored the copy, and acknowledges the message.
type DeadLetterConfig struct {
	// Enabled creates the dead-letter topic and subscription, and makes Reject publish to the topic.
	Enabled bool
	// Topic is the dead-letter topic. Default: "<subscription>.dlq".
	Topic string
	// Subscription keeps the dead letters. Default: the ID of Topic.
	Subscription string
	// MaxDeliveryAttempts also gives the subscription a dead-letter policy, so that Pub/Sub itself
	// forwards a message to Topic once it was delivered that many times without being
	// acknowledged, such as a message that keeps failing before the worker counts an attempt.
	// Between 5 and 100. The policy needs IAM grants for the Pub/Sub service agent. Zero sets no
	// policy: only the messages the worker rejects reach Topic.
	MaxDeliveryAttempts int
}

func (d DeadLetterConfig) sanitize(subscription string) DeadLetterConfig {
	if !d.Enabled {
		return d
	}
	if d.Topic == "" {
		d.Topic = resourceID(subscription) + ".dlq"
	}
	if d.Subscription == "" {
		d.Subscription = resourceID(d.Topic)
	}
	return d
}

func (d DeadLetterConfig) validate(subscription string) error {
	if !d.Enabled {
		return nil
	}
	if d.MaxDeliveryAttempts != 0 && (d.MaxDeliveryAttempts < minDeliveryAttempts || d.MaxDeliveryAttempts > maxDeliveryAttempts) {
		return fmt.Errorf("pubsub: subscription %q: DeadLetter.MaxDeliveryAttempts must be between %d and %d", subscription, minDeliveryAttempts, maxDeliveryAttempts)
	}
	return nil
}

type SubscriberConfig struct {
	// Name matches worker.SubscriberConfig.Name.
	Name string
	// Subscription is the subscription to receive from.
	Subscription string
	// MaxOutstandingMessages is how many messages Pub/Sub delivers before they are settled.
	// Default: the Concurrency of the worker subscriber, so every handler slot has a message and
	// the rest of the backlog stays in the subscription for other instances. A negative value
	// removes the limit.
	MaxOutstandingMessages int
	// DeadLetterTopic is the topic Reject publishes to, for a subscription created outside New,
	// such as with Terraform. Default: the DeadLetter topic of the subscription in Subscriptions.
	// Without either, Reject acknowledges the message and Pub/Sub drops it.
	DeadLetterTopic string
}

// topicName returns the full name of the topic name, an ID in project or already a full name.
func topicName(project, name string) string {
	return fullName(project, "topics", name)
}

// subscriptionName returns the full name of the subscription name, an ID in project or already a
// full name.
func subscriptionName(project, name string) string {
	return fullName(project, "subscriptions", name)
}

// fullName treats a name with a slash as a full name, like the client does.
func fullName(project, collection, name string) string {
	if strings.Contains(name, "/") {
		return name
	}
	return "projects/" + project + "/" + collection + "/" + name
}

// resourceID returns the last segment of a full name, or name itself when it is an ID.
func resourceID(name string) string {
	return name[strings.LastIndex(name, "/")+1:]
}
