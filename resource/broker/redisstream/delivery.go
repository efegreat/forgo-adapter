package redisstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"

	"github.com/redis/go-redis/v9"
)

// Delivery is the worker.IDelivery of a stream entry.
type Delivery struct {
	sub   *Subscriber
	entry redis.XMessage
	msg   worker.Message
}

func newDelivery(s *Subscriber, e redis.XMessage) *Delivery {
	return &Delivery{sub: s, entry: e, msg: message(s.conf.Stream, e)}
}

// Message returns the message the entry holds.
func (d *Delivery) Message() worker.Message {
	return d.msg
}

// Complete acknowledges the entry, which removes it from the pending entries of the group. The
// entry stays in the stream until it is trimmed.
func (d *Delivery) Complete(ctx context.Context) error {
	d.sub.inflight.remove(d)
	return d.ack(ctx)
}

// Retry hands the entry to the retry consumer of the group, with an idle time that makes it due
// once the delay of RetryConfig for attempt has passed, or at once without one. The consumers of
// the group then claim it like the entry of a stopped consumer.
func (d *Delivery) Retry(ctx context.Context, attempt int) error {
	d.sub.inflight.remove(d)
	return d.park(ctx, d.sub.conf.Retry.delay(attempt))
}

// Reject adds a copy of the entry to the dead-letter stream and acknowledges the entry, or only
// acknowledges it without a dead-letter stream. If the copy cannot be added, the entry is
// delivered again at once instead of being acknowledged.
func (d *Delivery) Reject(ctx context.Context) error {
	d.sub.inflight.remove(d)
	dl := d.sub.conf.DeadLetter
	if !dl.Enabled {
		return d.ack(ctx)
	}
	if err := d.sub.streams.add(ctx, dl.Stream, deadLetterFields(d)); err != nil {
		err = fmt.Errorf("redisstream: dead-letter %s to %s, delivering it again: %w", d.msg.ID, dl.Stream, err)
		return errors.Join(err, d.park(ctx, 0))
	}
	return d.ack(ctx)
}

func (d *Delivery) ack(ctx context.Context) error {
	if err := d.sub.client.XAck(ctx, d.sub.conf.Stream, d.sub.conf.Group, d.entry.ID).Err(); err != nil {
		return fmt.Errorf("redisstream: acknowledge %s: %w", d.msg.ID, err)
	}
	return nil
}

// park claims the entry for the retry consumer with an idle time of ClaimIdle minus delay, so it is
// claimed again once delay has passed. JUSTID leaves its delivery count alone.
func (d *Delivery) park(ctx context.Context, delay time.Duration) error {
	idle := d.sub.conf.ClaimIdle - delay
	err := d.sub.client.Do(ctx, "XCLAIM", d.sub.conf.Stream, d.sub.conf.Group, retryConsumer, 0, d.entry.ID,
		"IDLE", idle.Milliseconds(), "JUSTID").Err()
	if err != nil {
		return fmt.Errorf("redisstream: retry %s: %w", d.msg.ID, err)
	}
	return nil
}

// message converts a stream entry to a message. PayloadField is the payload and KeyField the key,
// every other field is a header. An entry added without the identity fields, for example by
// another producer, takes its ID from the entry ID, which stays the same across redeliveries, and
// its creation time from the time in the entry ID.
func message(stream string, e redis.XMessage) worker.Message {
	headers := make(map[string]string, len(e.Values)+2)
	var payload []byte
	var key string
	for k, v := range e.Values {
		s, _ := v.(string)
		switch k {
		case PayloadField:
			payload = []byte(s)
		case KeyField:
			key = s
		default:
			headers[k] = s
		}
	}
	if _, ok := headers[worker.MessageIDHeader]; !ok {
		headers[worker.MessageIDHeader] = e.ID
	}
	if _, ok := headers[worker.MessageTimestampHeader]; !ok {
		if ms, ok := entryMillis(e.ID); ok {
			headers[worker.MessageTimestampHeader] = strconv.FormatInt(ms*1e6, 10)
		}
	}
	return worker.NewMessageFromDelivery(key, stream, payload, types.NewJSONMapWithData(headers))
}

// entryMillis returns the Unix time in milliseconds that starts the entry ID id.
func entryMillis(id string) (int64, bool) {
	ms, _, _ := strings.Cut(id, "-")
	n, err := strconv.ParseInt(ms, 10, 64)
	return n, err == nil
}

// deadLetterFields copies the fields of a delivery for the dead-letter stream, with the message ID
// in its identity field, so an entry that arrived without one keeps the ID it was given.
func deadLetterFields(d *Delivery) []any {
	values := make([]any, 0, 2*len(d.entry.Values)+2)
	values = append(values, worker.MessageIDHeader, d.msg.ID)
	for k, v := range d.entry.Values {
		if k != worker.MessageIDHeader {
			values = append(values, k, v)
		}
	}
	return values
}
