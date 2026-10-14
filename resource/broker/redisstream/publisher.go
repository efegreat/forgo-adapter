package redisstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/efegreat/forgo/server/worker"

	"github.com/redis/go-redis/v9"
)

// errNoStream is returned for a message without a Destination, which names its stream.
var errNoStream = errors.New("redisstream: message has no Destination stream")

// publisher adds every message to the stream its Destination names. Publish returns once Redis has
// added the entry. The outbox only marks a message processed after that.
type publisher struct {
	streams *streams
}

// Publish adds msg to the stream msg.Destination, which Redis creates when it does not exist.
func (p *publisher) Publish(ctx context.Context, msg worker.Message) error {
	if msg.Destination == "" {
		return fmt.Errorf("%w: %s", errNoStream, msg.ID)
	}
	if err := p.streams.add(ctx, msg.Destination, fields(msg)); err != nil {
		return fmt.Errorf("redisstream: publish %s to stream %s: %w", msg.ID, msg.Destination, err)
	}
	return nil
}

// Close does nothing: the client belongs to the broker.
func (p *publisher) Close() error {
	return nil
}

// fields converts msg to the fields of a stream entry: the identity fields, the headers, KeyField
// and PayloadField. The identity fields are written from the message fields over any header of the
// same name, so the consuming side rebuilds the same ID, name and creation time. Headers named
// KeyField or PayloadField are left out.
func fields(msg worker.Message) []any {
	values := make([]any, 0, 10)
	values = append(values, worker.MessageIDHeader, msg.ID, worker.MessageNameHeader, msg.Name)
	stamped := !msg.CreatedAt.IsZero()
	if stamped {
		values = append(values, worker.MessageTimestampHeader, strconv.FormatInt(msg.CreatedAt.UnixNano(), 10))
	}
	if msg.Headers != nil {
		msg.Headers.Range(func(k, v string) bool {
			switch k {
			case worker.MessageIDHeader, worker.MessageNameHeader, KeyField, PayloadField:
			case worker.MessageTimestampHeader:
				if !stamped {
					values = append(values, k, v)
				}
			default:
				values = append(values, k, v)
			}
			return true
		})
	}
	if msg.Key != "" {
		values = append(values, KeyField, msg.Key)
	}
	return append(values, PayloadField, msg.Payload)
}

// streams adds entries to streams, and trims the streams of Config.Streams that have a MaxLen.
type streams struct {
	client redis.UniversalClient
	maxLen map[string]int64
}

func newStreams(client redis.UniversalClient, conf []StreamConfig) *streams {
	s := &streams{client: client, maxLen: make(map[string]int64)}
	for _, c := range conf {
		if c.MaxLen > 0 {
			s.maxLen[c.Name] = c.MaxLen
		}
	}
	return s
}

// add adds an entry of values to stream.
func (s *streams) add(ctx context.Context, stream string, values []any) error {
	args := &redis.XAddArgs{Stream: stream, Values: values}
	if n := s.maxLen[stream]; n > 0 {
		args.MaxLen, args.Approx = n, true
	}
	return s.client.XAdd(ctx, args).Err()
}
