package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Delivery is the worker.IDelivery of an AMQP delivery.
type Delivery struct {
	delivery amqp.Delivery
	msg      worker.Message
	// guard and gen drop acknowledgements once the channel that delivered the message was lost.
	guard *ackGuard
	gen   uint64
	// retry delays retries through the retry queues of the delivery's queue. Nil requeues at once.
	retry *retrier
}

// NewDelivery wraps an AMQP delivery. Header values that are not strings are converted by
// headerValue. Messages published without the identity headers, for example by another producer,
// take their ID from the message_id property and their name from the type property.
func NewDelivery(delivery amqp.Delivery) *Delivery {
	headers := types.NewJSONMap[string]()
	for k, v := range delivery.Headers {
		headers.Set(k, headerValue(v))
	}
	if _, ok := headers.Get(worker.MessageIDHeader); !ok && delivery.MessageId != "" {
		headers.Set(worker.MessageIDHeader, delivery.MessageId)
	}
	if _, ok := headers.Get(worker.MessageNameHeader); !ok && delivery.Type != "" {
		headers.Set(worker.MessageNameHeader, delivery.Type)
	}

	return &Delivery{
		delivery: delivery,
		msg:      worker.NewMessageFromDelivery(delivery.RoutingKey, delivery.Exchange, delivery.Body, headers),
	}
}

// newDelivery is NewDelivery for a subscriber channel guarded against recovery, whose queue has the
// retry queues of retry, if any.
func newDelivery(delivery amqp.Delivery, guard *ackGuard, retry *retrier) *Delivery {
	d := NewDelivery(delivery)
	d.guard = guard
	d.gen = guard.generation()
	d.retry = retry
	return d
}

// Message returns the message, with the identity its headers carry.
func (d *Delivery) Message() worker.Message {
	return d.msg
}

// Complete acknowledges the message.
func (d *Delivery) Complete(context.Context) error {
	return d.settle(func() error { return d.delivery.Ack(false) })
}

// Retry redelivers the message after the delay of the retry queue for attempt when its queue has
// Retry, and at once otherwise.
func (d *Delivery) Retry(ctx context.Context, attempt int) error {
	if d.retry == nil {
		return d.requeue()
	}
	return d.retry.retry(ctx, d, attempt)
}

// requeue returns the message to its queue, which redelivers it at once.
func (d *Delivery) requeue() error {
	return d.settle(func() error { return d.delivery.Nack(false, true) })
}

// Reject discards the message, or dead-letters it when its queue has DeadLetter enabled.
func (d *Delivery) Reject(context.Context) error {
	return d.settle(func() error { return d.delivery.Reject(false) })
}

// stale reports whether the channel that delivered the message was lost since it arrived.
func (d *Delivery) stale() bool {
	return d.guard != nil && d.guard.generation() != d.gen
}

// settle sends an acknowledgement unless the delivery's channel was lost since it arrived.
func (d *Delivery) settle(ack func() error) error {
	if d.guard == nil {
		return ack()
	}
	return d.guard.settle(d.gen, ack)
}

// headerValue converts an AMQP header value to the string Message.Headers holds, so that no header
// is lost. Strings and byte arrays are kept as they are, numbers and booleans take their Go text
// form, decimals their decimal notation, timestamps RFC 3339 in UTC and void an empty string.
// Tables and arrays, such as the x-death header of a dead-lettered or retried message, are encoded
// as JSON whose fields read as they would as headers of their own.
func headerValue(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	case bool:
		return strconv.FormatBool(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case amqp.Decimal:
		return decimal(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339)
	case amqp.Table, []any:
		if b, err := json.Marshal(jsonField(v)); err == nil {
			return string(b)
		}
	}
	return fmt.Sprint(v)
}

// jsonField prepares a field of an AMQP table or array for encoding/json. Numbers stay JSON
// numbers, except NaN and the infinities, which JSON has none for, and every field other than a
// table, an array, a boolean or void becomes a JSON string of its headerValue.
func jsonField(v any) any {
	switch v := v.(type) {
	case amqp.Table:
		fields := make(map[string]any, len(v))
		for k, f := range v {
			fields[k] = jsonField(f)
		}
		return fields
	case []any:
		fields := make([]any, len(v))
		for i, f := range v {
			fields[i] = jsonField(f)
		}
		return fields
	case nil, bool, int, int8, int16, int32, int64, uint8, uint16, uint32:
		return v
	case float32:
		return jsonFloat(float64(v), headerValue(v))
	case float64:
		return jsonFloat(v, headerValue(v))
	case amqp.Decimal:
		return json.Number(decimal(v))
	}
	return headerValue(v)
}

// jsonFloat returns text, the headerValue of f, as a JSON number, or as a string when f is NaN or
// infinite.
func jsonFloat(f float64, text string) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return text
	}
	return json.Number(text)
}

// decimal formats d, which stands for d.Value × 10^-d.Scale, in decimal notation with d.Scale
// fractional digits.
func decimal(d amqp.Decimal) string {
	digits := strconv.FormatInt(int64(d.Value), 10)
	if d.Scale == 0 {
		return digits
	}
	sign := ""
	if d.Value < 0 {
		sign, digits = "-", digits[1:]
	}
	scale := int(d.Scale)
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	return sign + digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
}
