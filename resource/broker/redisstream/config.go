package redisstream

import (
	"errors"
	"fmt"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo-adapter/resource/redisdb"
)

const (
	// PayloadField is the field of a stream entry that holds Message.Payload.
	PayloadField = "payload"
	// KeyField is the field of a stream entry that holds Message.Key.
	KeyField = "x-msg-key"

	defaultStartID   = "$"
	defaultClaimIdle = time.Minute
	// minClaimIdle keeps consumers from claiming the entries of a live consumer during a short
	// pause, such as a slow heartbeat.
	minClaimIdle = time.Second
)

// Config is a Redis Streams broker resource. Its Value is the worker.IBroker the worker servers
// resolve.
//
// New connects with Redis and creates the consumer groups of Streams that do not exist yet, so a
// connection or key error fails the bootstrap instead of the first publish or read. It needs
// Redis 6.2 or newer.
type Config struct {
	// Name identifies the resource and names the broker in logs. Required.
	Name string
	// Redis is the connection of the broker, which opens a client of its own: blocking reads hold
	// a connection each, so they never take connections from the pool of the application's
	// providers. Its Name is not used.
	Redis redisdb.Config
	// Streams are the streams the broker trims and the consumer groups it creates.
	Streams []StreamConfig
	// Subscribers are the subscribers the worker consumer names.
	Subscribers []SubscriberConfig
}

// Sanitize returns a copy of c with defaults applied. It never modifies the slices of c.
func (c Config) Sanitize() Config {
	streams := make([]StreamConfig, len(c.Streams))
	for i, s := range c.Streams {
		streams[i] = s.Sanitize()
	}
	c.Streams = streams

	subscribers := make([]SubscriberConfig, len(c.Subscribers))
	for i, s := range c.Subscribers {
		subscribers[i] = s.Sanitize()
	}
	c.Subscribers = subscribers

	return c
}

// New validates the config, connects to Redis and creates the missing consumer groups. A failure
// closes the client.
func (c Config) New() (forgo.Resource, error) {
	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newBroker(cfg)
}

// validate rejects configurations that would otherwise only fail once Redis is reached.
func (c Config) validate() error {
	names := make(map[string]bool, len(c.Streams))
	for _, s := range c.Streams {
		if s.Name == "" {
			return errors.New("redisstream: a stream needs a Name")
		}
		if names[s.Name] {
			return fmt.Errorf("redisstream: stream %q is listed twice", s.Name)
		}
		names[s.Name] = true
		if s.MaxLen < 0 {
			return fmt.Errorf("redisstream: stream %q: MaxLen cannot be negative", s.Name)
		}
		for _, g := range s.Groups {
			if g.Name == "" {
				return fmt.Errorf("redisstream: stream %q: a group needs a Name", s.Name)
			}
		}
	}
	for _, s := range c.Subscribers {
		if err := s.validate(); err != nil {
			return err
		}
	}
	return nil
}

// StreamConfig is a stream the broker trims, and the consumer groups New creates on it.
type StreamConfig struct {
	// Name is the key of the stream.
	Name string
	// MaxLen trims the stream to about MaxLen entries on every entry the broker adds to it
	// (XADD MAXLEN ~), so Redis drops the oldest ones, whether or not every group has read and
	// acknowledged them. Redis trims in whole nodes of its stream, so it keeps a little more than
	// MaxLen. Zero never trims.
	MaxLen int64
	// Groups are created when they do not exist, with the stream when it does not exist either.
	Groups []GroupConfig
}

// Sanitize returns a copy of s with the defaults of its groups applied. It never modifies
// s.Groups.
func (s StreamConfig) Sanitize() StreamConfig {
	groups := make([]GroupConfig, len(s.Groups))
	for i, g := range s.Groups {
		if g.StartID == "" {
			g.StartID = defaultStartID
		}
		groups[i] = g
	}
	s.Groups = groups
	return s
}

// GroupConfig is a consumer group, which receives every entry of its stream once, spread over the
// consumers that read it.
type GroupConfig struct {
	Name string
	// StartID is the ID of the last entry the group counts as delivered when New creates it: "$"
	// delivers only the entries added afterwards, "0" every entry the stream holds. It does not
	// change a group that exists. Default: "$".
	StartID string
}

// SubscriberConfig configures the subscriber of the worker.SubscriberConfig of the same Name.
type SubscriberConfig struct {
	// Name matches worker.SubscriberConfig.Name.
	Name string
	// Stream is the stream to read.
	Stream string
	// Group is the consumer group to read as. Default: Name.
	Group string
	// ClaimIdle is how long an entry stays pending without a sign of its consumer before another
	// consumer of the group claims it, such as an entry of an instance that crashed. While an entry
	// is handled, its consumer resets its idle time every ClaimIdle/3, so a handler may run longer.
	// It is also the longest retry delay. Every subscriber of a group needs the same ClaimIdle.
	// At least one second. Default: one minute.
	ClaimIdle time.Duration
	// Retry delays the redelivery of the entries the worker retries. Disabled by default: a
	// retried entry is delivered again at once.
	Retry RetryConfig
	// DeadLetter keeps the entries the worker rejects in a dead-letter stream. Disabled by
	// default: rejected entries are acknowledged and stay in the stream until it is trimmed.
	DeadLetter DeadLetterConfig
}

// Sanitize returns a copy of s with defaults applied.
func (s SubscriberConfig) Sanitize() SubscriberConfig {
	if s.Group == "" {
		s.Group = s.Name
	}
	if s.ClaimIdle == 0 {
		s.ClaimIdle = defaultClaimIdle
	}
	s.Retry = s.Retry.sanitize(s.ClaimIdle)
	s.DeadLetter = s.DeadLetter.sanitize(s.Stream, s.Group)
	return s
}

// longestDelay is the longest wait before a parked entry is claimed again: Retry.Max, or ClaimIdle
// without delayed retries.
func (s SubscriberConfig) longestDelay() time.Duration {
	if s.Retry.enabled() {
		return s.Retry.Max
	}
	return s.ClaimIdle
}

func (s SubscriberConfig) validate() error {
	switch {
	case s.Stream == "":
		return fmt.Errorf("redisstream: subscriber %q needs a Stream", s.Name)
	case s.Group == "":
		return fmt.Errorf("redisstream: subscriber %q needs a Group", s.Name)
	case s.ClaimIdle < minClaimIdle:
		return fmt.Errorf("redisstream: subscriber %q: ClaimIdle must be at least %s", s.Name, minClaimIdle)
	case s.DeadLetter.Enabled && s.DeadLetter.Stream == s.Stream:
		return fmt.Errorf("redisstream: subscriber %q: DeadLetter.Stream cannot be the stream it reads", s.Name)
	}
	return s.Retry.validate(s.Name, s.ClaimIdle)
}

// RetryConfig delays the redelivery of the entries the worker retries.
//
// The delay doubles with every failed attempt, from Initial up to Max. To retry an entry, the
// subscriber hands it to the group's retry consumer, forgo-retry, with an idle time of ClaimIdle
// minus the delay, so that the consumers of the group claim it once the delay has passed. A
// waiting entry holds no handler slot.
type RetryConfig struct {
	// Initial is the delay after the first failed attempt, at least 1ms. Zero disables the
	// delays: a retried entry is claimed again at once.
	Initial time.Duration
	// Max is the longest delay, at most ClaimIdle. Default: ClaimIdle.
	Max time.Duration
}

func (r RetryConfig) enabled() bool {
	return r.Initial > 0
}

func (r RetryConfig) sanitize(claimIdle time.Duration) RetryConfig {
	if r.enabled() && r.Max == 0 {
		r.Max = claimIdle
	}
	return r
}

func (r RetryConfig) validate(subscriber string, claimIdle time.Duration) error {
	if r.Initial < 0 || r.Max < 0 {
		return fmt.Errorf("redisstream: subscriber %q: Retry.Initial and Retry.Max cannot be negative", subscriber)
	}
	if !r.enabled() {
		return nil
	}
	switch {
	case r.Initial < time.Millisecond:
		return fmt.Errorf("redisstream: subscriber %q: Retry.Initial must be at least 1ms", subscriber)
	case r.Initial > claimIdle || r.Max > claimIdle:
		return fmt.Errorf("redisstream: subscriber %q: Retry.Initial and Retry.Max cannot exceed ClaimIdle (%s)", subscriber, claimIdle)
	case r.Max < r.Initial:
		return fmt.Errorf("redisstream: subscriber %q: Retry.Max cannot be shorter than Retry.Initial", subscriber)
	}
	return nil
}

// delay returns the delay before the retry of a failed attempt: Initial for attempt 1, and for 0, a
// failure before the attempt was counted, then twice the previous delay for every later attempt,
// up to Max. It is zero when the delays are disabled.
func (r RetryConfig) delay(attempt int) time.Duration {
	if !r.enabled() {
		return 0
	}
	d := r.Initial
	for range attempt - 1 {
		if d >= r.Max {
			break
		}
		d *= 2
	}
	return min(d, r.Max)
}

// DeadLetterConfig keeps the entries the worker rejects in a dead-letter stream instead of
// dropping them.
//
// To reject an entry, the subscriber adds a copy of it to the dead-letter stream, then
// acknowledges it. The dead-letter stream is trimmed like any stream when it is listed in Streams
// with a MaxLen.
type DeadLetterConfig struct {
	// Enabled makes Reject copy entries to Stream.
	Enabled bool
	// Stream is the dead-letter stream. Default: "<stream>:<group>:dlq".
	Stream string
}

func (d DeadLetterConfig) sanitize(stream, group string) DeadLetterConfig {
	if d.Enabled && d.Stream == "" {
		d.Stream = stream + ":" + group + ":dlq"
	}
	return d
}
