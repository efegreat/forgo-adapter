// Package observer is the operation.IObserver that records the metrics of every forgo server with
// OpenTelemetry: the duration of every operation, labelled alike on every transport, the HTTP semantic
// conventions' request duration, and the signals outside operations, such as message settlements, outbox
// relay batches and backlog, lock contention and connections.
package observer

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/efegreat/forgo/operation"
	fhttp "github.com/efegreat/forgo/server/http"
	"github.com/efegreat/forgo/server/worker"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// scope is the instrumentation scope of the metrics.
const scope = "github.com/efegreat/forgo-adapter/otel/observer"

// The attributes of forgo's own instruments.
const (
	attrOperationType       = attribute.Key("forgo.operation.type")
	attrOperationName       = attribute.Key("forgo.operation.name")
	attrOperationStatus     = attribute.Key("forgo.operation.status")
	attrErrorCode           = attribute.Key("forgo.error.code")
	attrSubscriber          = attribute.Key("forgo.subscriber")
	attrSettlementKind      = attribute.Key("forgo.settlement.kind")
	attrSettlementDuplicate = attribute.Key("forgo.settlement.duplicate")
	attrSettlementFailed    = attribute.Key("forgo.settlement.failed")
	attrBatchFailed         = attribute.Key("forgo.outbox.batch.failed")
	attrServerName          = attribute.Key("forgo.server.name")
	attrServerAddress       = attribute.Key("forgo.server.address")
	attrConnState           = attribute.Key("forgo.conn.state")
	attrConnDirection       = attribute.Key("forgo.conn.direction")
)

// statusOK is the forgo.operation.status of an operation that succeeded, which has no operation.Status.
const statusOK = "ok"

// defaultBuckets are the boundaries, in seconds, of the semantic conventions' HTTP request duration.
var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

// defaultLagBuckets are the boundaries, in seconds, of forgo.message.lag: from 5 ms to an hour.
var defaultLagBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}

// Config configures the observer. Its zero value records every instrument with the default boundaries.
type Config struct {
	// Buckets are the boundaries, in seconds, of the duration histograms: forgo.operation.duration,
	// http.server.request.duration and forgo.outbox.batch.duration. Default: the HTTP semantic conventions'
	// boundaries, from 5 ms to 10 s.
	Buckets []float64
	// LagBuckets are the boundaries, in seconds, of forgo.message.lag. Default: from 5 ms to an hour.
	LagBuckets []float64
	// DisableHTTPSemconv leaves out http.server.request.duration, the semantic conventions' HTTP histogram.
	DisableHTTPSemconv bool
	// ConnIO also counts the read and write errors of connections, as forgo.conn.io_errors. It wraps every
	// connection of the HTTP and gRPC servers, which hides the zero-copy paths of *net.TCPConn, so it is off
	// by default.
	ConnIO bool
}

// Sanitize returns a copy of the config with defaults applied to its zero fields.
func (c Config) Sanitize() Config {
	if len(c.Buckets) == 0 {
		c.Buckets = slices.Clone(defaultBuckets)
	}
	if len(c.LagBuckets) == 0 {
		c.LagBuckets = slices.Clone(defaultLagBuckets)
	}
	return c
}

// validate rejects boundaries that do not increase strictly.
func (c Config) validate() error {
	for name, buckets := range map[string][]float64{"Buckets": c.Buckets, "LagBuckets": c.LagBuckets} {
		for i := 1; i < len(buckets); i++ {
			if buckets[i] <= buckets[i-1] {
				return fmt.Errorf("observer: %s must increase strictly", name)
			}
		}
	}
	return nil
}

// New returns the provider of the observer, for forgo.WithProviders, after a meter resource:
//
//	forgo.WithResources(meter.Config{}),
//	forgo.WithProviders(observer.New(observer.Config{})),
//
// The provider takes the metric.MeterProvider of the bootstrap and returns the observer as an
// operation.IObserver, which every server picks up. It also implements worker.IDeliveryObserver,
// worker.IRelayObserver, worker.IBacklogObserver, operation.ILockObserver and
// transport.IConnObserverFactory. A config it rejects fails the bootstrap.
func New(cfg Config) func(metric.MeterProvider) (operation.IObserver, error) {
	return func(mp metric.MeterProvider) (operation.IObserver, error) {
		if mp == nil {
			return nil, errors.New("observer: nil MeterProvider")
		}
		c := cfg.Sanitize()
		if err := c.validate(); err != nil {
			return nil, err
		}
		return newObserver(mp.Meter(scope), c)
	}
}

// observer records the metrics of every server. Its caches hold the options of every label tuple seen.
type observer struct {
	duration     metric.Float64Histogram
	active       metric.Int64UpDownCounter
	httpDuration metric.Float64Histogram // nil with DisableHTTPSemconv
	settled      metric.Int64Counter
	lag          metric.Float64Histogram
	published    metric.Int64Counter
	unpublished  metric.Int64Counter
	batch        metric.Float64Histogram
	backlog      metric.Int64Gauge
	backlogAge   metric.Float64Gauge
	lockHeld     metric.Int64Counter
	lockFailed   metric.Int64Counter
	acceptErrors metric.Int64Counter
	opened       metric.Int64Counter
	open         metric.Int64UpDownCounter
	ioErrors     metric.Int64Counter // nil without ConnIO

	durations   cache[durationKey, []metric.RecordOption]
	actives     cache[opKey, []metric.AddOption]
	https       cache[httpKey, []metric.RecordOption]
	settlements cache[settlementKey, []metric.AddOption]
	lags        cache[messageKey, []metric.RecordOption]
	locks       cache[opKey, []metric.AddOption]

	batchOK, batchFailed []metric.RecordOption
}

// newObserver creates the instruments of cfg on m.
func newObserver(m metric.Meter, cfg Config) (*observer, error) {
	o := &observer{
		batchOK:     []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(attrBatchFailed.Bool(false)))},
		batchFailed: []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(attrBatchFailed.Bool(true)))},
	}
	var errs []error
	histogram := func(name, unit, description string, buckets []float64) metric.Float64Histogram {
		h, err := m.Float64Histogram(name, metric.WithUnit(unit), metric.WithDescription(description),
			metric.WithExplicitBucketBoundaries(buckets...))
		errs = append(errs, err)
		return h
	}
	counter := func(name, unit, description string) metric.Int64Counter {
		c, err := m.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(description))
		errs = append(errs, err)
		return c
	}
	gauge := func(name, unit, description string) metric.Int64Gauge {
		g, err := m.Int64Gauge(name, metric.WithUnit(unit), metric.WithDescription(description))
		errs = append(errs, err)
		return g
	}
	upDown := func(name, unit, description string) metric.Int64UpDownCounter {
		c, err := m.Int64UpDownCounter(name, metric.WithUnit(unit), metric.WithDescription(description))
		errs = append(errs, err)
		return c
	}

	o.duration = histogram("forgo.operation.duration", "s",
		"Duration of forgo operations: HTTP requests, gRPC calls, cron runs, messages and commands.", cfg.Buckets)
	o.active = upDown("forgo.operation.active", "{operation}", "Number of forgo operations in progress.")
	if !cfg.DisableHTTPSemconv {
		o.httpDuration = histogram("http.server.request.duration", "s", "Duration of HTTP server requests.", cfg.Buckets)
	}
	o.settled = counter("forgo.message.settled", "{message}", "Number of consumed messages settled at the broker.")
	o.lag = histogram("forgo.message.lag", "s", "Time from the creation of a message to the start of its handling.",
		cfg.LagBuckets)
	o.published = counter("forgo.outbox.published", "{message}", "Number of outbox messages published.")
	o.unpublished = counter("forgo.outbox.failed", "{message}",
		"Number of claimed outbox messages that failed to publish, which go back to the outbox.")
	o.batch = histogram("forgo.outbox.batch.duration", "s", "Duration of outbox relay batches, the empty ones included.",
		cfg.Buckets)
	o.backlog = gauge("forgo.outbox.backlog", "{message}", "Number of outbox messages not published yet.")
	backlogAge, err := m.Float64Gauge("forgo.outbox.backlog.age", metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest outbox message not published yet, 0 when there is none."))
	errs = append(errs, err)
	o.backlogAge = backlogAge
	o.lockHeld = counter("forgo.lock.held", "{operation}", "Number of operations skipped or retried for a lock another holder had.")
	o.lockFailed = counter("forgo.lock.failed", "{operation}", "Number of operations whose lock could not be acquired.")
	o.acceptErrors = counter("forgo.conn.accept_errors", "{error}", "Number of failed accepts of a server's listener.")
	o.opened = counter("forgo.conn.opened", "{connection}", "Number of connections a server accepted.")
	o.open = upDown("forgo.conn.open", "{connection}", "Number of open connections of a server, by state.")
	if cfg.ConnIO {
		o.ioErrors = counter("forgo.conn.io_errors", "{error}", "Number of failed reads and writes of a server's connections.")
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("observer: %w", err)
	}
	return o, nil
}

// opKey is an operation's labels.
type opKey struct {
	typ, name string
}

func (k opKey) attrs() []attribute.KeyValue {
	return []attribute.KeyValue{attrOperationType.String(k.typ), attrOperationName.String(k.name)}
}

func buildActive(k opKey) []metric.AddOption {
	return []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(k.attrs()...))}
}

// durationKey is the labels of forgo.operation.duration: the operation, and the status and code of its error.
type durationKey struct {
	opKey
	failed bool
	status operation.Status
	code   string
}

func buildDuration(k durationKey) []metric.RecordOption {
	attrs := k.attrs()
	if k.failed {
		attrs = append(attrs, attrOperationStatus.String(k.status.String()), attrErrorCode.String(k.code))
	} else {
		attrs = append(attrs, attrOperationStatus.String(statusOK))
	}
	return []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(attrs...))}
}

// Start counts the operation in progress. For a message, it records the time since the message was
// created, unless its producer did not say.
func (o *observer) Start(ctx *operation.Context) {
	o.active.Add(ctx, 1, o.actives.get(opKey{ctx.Type, ctx.Name}, buildActive)...)
	if st, ok := ctx.State.(*worker.State); ok {
		if created := st.Delivery.Message().CreatedAt; !created.IsZero() {
			o.lag.Record(ctx, time.Since(created).Seconds(), o.lags.get(messageKey{st.Subscriber, ctx.Name}, buildMessage)...)
		}
	}
}

// End records the operation's duration with its labels, and an HTTP request's with the semantic
// conventions'.
func (o *observer) End(ctx *operation.Context, obs operation.Observation) {
	op := opKey{ctx.Type, ctx.Name}
	o.active.Add(ctx, -1, o.actives.get(op, buildActive)...)
	k := durationKey{opKey: op}
	if obs.Err != nil {
		k.failed, k.status, k.code = true, obs.Error.Status, obs.Error.Code
	}
	o.duration.Record(ctx, obs.Elapsed.Seconds(), o.durations.get(k, buildDuration)...)

	if o.httpDuration != nil {
		if st, ok := ctx.State.(*fhttp.State); ok {
			o.recordHTTP(ctx, st, ctx.Name, obs.Elapsed)
		}
	}
}

// httpKey is the labels of http.server.request.duration.
type httpKey struct {
	method, route, scheme, version string
	status                         int
}

func buildHTTP(k httpKey) []metric.RecordOption {
	attrs := []attribute.KeyValue{
		semconv.HTTPRequestMethodKey.String(k.method),
		semconv.URLSchemeKey.String(k.scheme),
		semconv.HTTPResponseStatusCodeKey.Int(k.status),
	}
	if k.route != "" {
		attrs = append(attrs, semconv.HTTPRouteKey.String(k.route))
	}
	if k.version != "" {
		attrs = append(attrs, semconv.NetworkProtocolVersionKey.String(k.version))
	}
	if k.status >= 500 {
		attrs = append(attrs, semconv.ErrorTypeKey.String(strconv.Itoa(k.status)))
	}
	return []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(attrs...))}
}

// recordHTTP records the duration of an HTTP request. A request whose handler hijacked the connection has no
// status of its own, and lasts as long as what it served, such as a WebSocket, so it is left out.
func (o *observer) recordHTTP(ctx *operation.Context, st *fhttp.State, name string, elapsed time.Duration) {
	status := st.Writer.StatusCode()
	if status == 0 {
		return
	}
	r := st.Request
	k := httpKey{method: httpMethod(r.Method), route: httpRoute(r.Pattern, name), scheme: "http", status: status}
	if r.TLS != nil {
		k.scheme = "https"
	}
	switch {
	case r.ProtoMajor == 1 && r.ProtoMinor == 1:
		k.version = "1.1"
	case r.ProtoMajor == 1 && r.ProtoMinor == 0:
		k.version = "1.0"
	case r.ProtoMajor == 2:
		k.version = "2"
	case r.ProtoMajor == 3:
		k.version = "3"
	}
	o.httpDuration.Record(ctx, elapsed.Seconds(), o.https.get(k, buildHTTP)...)
}

// httpMethod returns method, or "_OTHER" for a method the semantic conventions do not know, which keeps
// the label bounded whatever clients send.
func httpMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH", "QUERY":
		return method
	default:
		return "_OTHER"
	}
}

// httpRoute returns the path template of a ServeMux pattern, such as "/v1/users/{id}" for
// "GET /v1/users/{id}", without its method and host. The not_found fallback matched no route, so it has none.
func httpRoute(pattern, name string) string {
	if name == "not_found" && pattern == "/" {
		return ""
	}
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		pattern = pattern[i+1:]
	}
	if i := strings.IndexByte(pattern, '/'); i > 0 {
		pattern = pattern[i:]
	}
	return pattern
}
