package observer

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
	fhttp "github.com/efegreat/forgo/server/http"
	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/transport"
	"github.com/efegreat/forgo/types"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestObserver returns the observer of cfg on a MeterProvider whose metrics reader collects.
func newTestObserver(t testing.TB, cfg Config) (*observer, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	o, err := New(cfg)(mp)
	if err != nil {
		t.Fatal(err)
	}
	return o.(*observer), reader
}

// collect returns the metrics reader collected, by name.
func collect(t testing.TB, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	metrics := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metrics[m.Name] = m
		}
	}
	return metrics
}

// point is a data point of a histogram or a sum: its attributes and its count or value.
type point struct {
	attrs attribute.Set
	n     int64
}

// points returns the data points of the metric, with their counts for a histogram and their values for a sum.
func points(t testing.TB, m metricdata.Metrics) []point {
	t.Helper()
	var pts []point
	switch data := m.Data.(type) {
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			pts = append(pts, point{dp.Attributes, int64(dp.Count)})
		}
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			pts = append(pts, point{dp.Attributes, dp.Value})
		}
	default:
		t.Fatalf("%s is a %T", m.Name, m.Data)
	}
	return pts
}

// find returns the value of the data point of m whose attributes are attrs, and false when there is none.
func find(t testing.TB, m metricdata.Metrics, attrs ...attribute.KeyValue) (int64, bool) {
	t.Helper()
	want := attribute.NewSet(attrs...)
	for _, p := range points(t, m) {
		if p.attrs.Equals(&want) {
			return p.n, true
		}
	}
	return 0, false
}

// mustFind is find that fails t when there is no such data point.
func mustFind(t testing.TB, m metricdata.Metrics, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	n, ok := find(t, m, attrs...)
	if !ok {
		var got []string
		for _, p := range points(t, m) {
			got = append(got, p.attrs.Encoded(attribute.DefaultEncoder()))
		}
		want := attribute.NewSet(attrs...)
		t.Fatalf("%s has no data point %v, only %v", m.Name, want.Encoded(attribute.DefaultEncoder()), got)
	}
	return n
}

func newOp(typ, name string, state any) *operation.Context {
	return &operation.Context{Context: context.Background(), Type: typ, Name: name, Args: new(types.Map[any, any]), State: state}
}

// TestOperationDuration checks that the same error gets the same labels on every transport.
func TestOperationDuration(t *testing.T) {
	o, reader := newTestObserver(t, Config{})
	gone := operation.Error{Code: "order_gone", Status: operation.StatusNotFound, Message: "gone"}
	ops := []*operation.Context{
		newOp("http_handler", "orders", nil),
		newOp("grpc_handler", "/shop.Orders/Get", nil),
		newOp("cron_job", "sweep", nil),
		newOp("message_handler", "order.paid", nil),
		newOp("command", "reindex", nil),
	}
	for _, op := range ops {
		o.Start(op)
		o.End(op, operation.Observation{Elapsed: 20 * time.Millisecond})
		o.Start(op)
		o.End(op, operation.Observation{Elapsed: time.Second, Err: gone, Error: gone})
	}

	metrics := collect(t, reader)
	duration := metrics["forgo.operation.duration"]
	if duration.Unit != "s" {
		t.Fatalf("unit %q", duration.Unit)
	}
	for _, op := range ops {
		labels := []attribute.KeyValue{attrOperationType.String(op.Type), attrOperationName.String(op.Name)}
		if n := mustFind(t, duration, append(labels, attrOperationStatus.String("ok"))...); n != 1 {
			t.Fatalf("%s: %d successes", op.Type, n)
		}
		if n := mustFind(t, duration, append(labels, attrOperationStatus.String("not_found"), attrErrorCode.String("order_gone"))...); n != 1 {
			t.Fatalf("%s: %d failures", op.Type, n)
		}
		if n := mustFind(t, metrics["forgo.operation.active"], labels...); n != 0 {
			t.Fatalf("%s: %d operations still active", op.Type, n)
		}
	}
}

// httpOp returns an HTTP operation whose response has status, a hijacked connection's for 0.
func httpOp(t testing.TB, name, method, target, pattern string, status int) *operation.Context {
	t.Helper()
	r := httptest.NewRequest(method, target, nil)
	r.Pattern = pattern
	w := &fhttp.ResponseRecorder{ResponseWriter: httptest.NewRecorder()}
	if status != 0 {
		w.WriteHeader(status)
	}
	return newOp(fhttp.OperationTypeHTTPHandler, name, &fhttp.State{Request: r, Writer: w})
}

func TestHTTPSemconv(t *testing.T) {
	o, reader := newTestObserver(t, Config{})
	for _, op := range []*operation.Context{
		httpOp(t, "user", "GET", "/v1/users/42", "GET /v1/users/{id}", 200),
		httpOp(t, "user", "GET", "/v1/users/43", "GET /v1/users/{id}", 200),
		httpOp(t, "items", "BREW", "https://shop.example/items", "/items", 500),
		httpOp(t, "not_found", "GET", "/nowhere", "/", 404),
		httpOp(t, "socket", "GET", "/ws", "GET /ws", 0),
	} {
		o.Start(op)
		o.End(op, operation.Observation{Elapsed: time.Millisecond})
	}

	m := collect(t, reader)["http.server.request.duration"]
	if n := mustFind(t, m,
		attribute.String("http.request.method", "GET"),
		attribute.String("url.scheme", "http"),
		attribute.Int("http.response.status_code", 200),
		attribute.String("http.route", "/v1/users/{id}"),
		attribute.String("network.protocol.version", "1.1"),
	); n != 2 {
		t.Fatalf("%d requests of the route", n)
	}
	mustFind(t, m,
		attribute.String("http.request.method", "_OTHER"),
		attribute.String("url.scheme", "https"),
		attribute.Int("http.response.status_code", 500),
		attribute.String("http.route", "/items"),
		attribute.String("network.protocol.version", "1.1"),
		attribute.String("error.type", "500"),
	)
	// The not_found fallback matched no route.
	mustFind(t, m,
		attribute.String("http.request.method", "GET"),
		attribute.String("url.scheme", "http"),
		attribute.Int("http.response.status_code", 404),
		attribute.String("network.protocol.version", "1.1"),
	)
	if pts := points(t, m); len(pts) != 3 {
		t.Fatalf("%d data points: a hijacked connection was recorded", len(pts))
	}

	o, reader = newTestObserver(t, Config{DisableHTTPSemconv: true})
	op := httpOp(t, "user", "GET", "/v1/users/42", "GET /v1/users/{id}", 200)
	o.Start(op)
	o.End(op, operation.Observation{})
	if _, ok := collect(t, reader)["http.server.request.duration"]; ok {
		t.Fatal("recorded http.server.request.duration with DisableHTTPSemconv")
	}
}

func TestHTTPRoute(t *testing.T) {
	for _, tc := range []struct{ pattern, name, want string }{
		{"GET /v1/users/{id}", "user", "/v1/users/{id}"},
		{"/v1/any", "any", "/v1/any"},
		{"GET shop.example/items", "items", "/items"},
		{"/", "not_found", ""},
		{"/", "root", "/"},
	} {
		if got := httpRoute(tc.pattern, tc.name); got != tc.want {
			t.Errorf("httpRoute(%q, %q) = %q, want %q", tc.pattern, tc.name, got, tc.want)
		}
	}
}

// stubDelivery is a delivery of msg.
type stubDelivery struct{ msg worker.Message }

func (d stubDelivery) Message() worker.Message        { return d.msg }
func (stubDelivery) Complete(context.Context) error   { return nil }
func (stubDelivery) Retry(context.Context, int) error { return nil }
func (stubDelivery) Reject(context.Context) error     { return nil }

func TestMessages(t *testing.T) {
	o, reader := newTestObserver(t, Config{})
	message := func(created time.Time) *operation.Context {
		return newOp(worker.OperationTypeMessageHandler, "order.paid",
			&worker.State{Delivery: stubDelivery{worker.Message{CreatedAt: created}}, Subscriber: "billing"})
	}
	waited := message(time.Now().Add(-2 * time.Second))
	o.Start(waited)
	o.End(waited, operation.Observation{})
	o.Settled(waited, worker.Settlement{Kind: worker.SettlementCompleted, Attempt: 1})
	undated := message(time.Time{})
	o.Start(undated)
	o.End(undated, operation.Observation{})
	o.Settled(undated, worker.Settlement{Kind: worker.SettlementRejected, Duplicate: true, Err: errors.New("closed")})

	metrics := collect(t, reader)
	msg := []attribute.KeyValue{attrSubscriber.String("billing"), attrOperationName.String("order.paid")}
	lag := metrics["forgo.message.lag"]
	if n := mustFind(t, lag, msg...); n != 1 {
		t.Fatalf("%d lags recorded, want the dated message's only", n)
	}
	if sum := lag.Data.(metricdata.Histogram[float64]).DataPoints[0].Sum; sum < 2 || sum > 60 {
		t.Fatalf("lag %vs, want about 2s", sum)
	}
	settled := metrics["forgo.message.settled"]
	mustFind(t, settled, append(msg, attrSettlementKind.String("completed"), attrSettlementDuplicate.Bool(false),
		attrSettlementFailed.Bool(false))...)
	mustFind(t, settled, append(msg, attrSettlementKind.String("rejected"), attrSettlementDuplicate.Bool(true),
		attrSettlementFailed.Bool(true))...)
}

func TestRelayed(t *testing.T) {
	o, reader := newTestObserver(t, Config{})
	ctx := context.Background()
	o.Relayed(ctx, worker.RelayBatch{Elapsed: time.Millisecond})
	o.Relayed(ctx, worker.RelayBatch{Claimed: 10, Published: 8, Elapsed: 50 * time.Millisecond})
	o.Relayed(ctx, worker.RelayBatch{Elapsed: time.Millisecond, Err: errors.New("database down")})

	metrics := collect(t, reader)
	if n := mustFind(t, metrics["forgo.outbox.published"]); n != 8 {
		t.Fatalf("%d published", n)
	}
	if n := mustFind(t, metrics["forgo.outbox.failed"]); n != 2 {
		t.Fatalf("%d failed", n)
	}
	batches := metrics["forgo.outbox.batch.duration"]
	if ok, failed := mustFind(t, batches, attrBatchFailed.Bool(false)), mustFind(t, batches, attrBatchFailed.Bool(true)); ok != 2 || failed != 1 {
		t.Fatalf("%d batches, %d failed, want 2 and 1", ok, failed)
	}
}

type derivedKey struct{}

func TestLocks(t *testing.T) {
	o, reader := newTestObserver(t, Config{})
	job := newOp("cron_job", "sweep", nil)
	o.LockHeld(job, "sweep-lock")
	o.LockHeld(context.WithValue(job, derivedKey{}, 1), "sweep-lock") // derived from the operation
	o.LockFailed(job, "sweep-lock", errors.New("redis down"))
	o.LockFailed(context.Background(), "other", errors.New("redis down"))

	metrics := collect(t, reader)
	labels := []attribute.KeyValue{attrOperationType.String("cron_job"), attrOperationName.String("sweep")}
	if n := mustFind(t, metrics["forgo.lock.held"], labels...); n != 2 {
		t.Fatalf("%d held", n)
	}
	if n := mustFind(t, metrics["forgo.lock.failed"], labels...); n != 1 {
		t.Fatalf("%d failed", n)
	}
	if n := mustFind(t, metrics["forgo.lock.failed"]); n != 1 {
		t.Fatalf("%d failed outside an operation", n)
	}
}

func TestConnections(t *testing.T) {
	o, reader := newTestObserver(t, Config{})
	api := o.ConnObserver(transport.Server{Name: "http", Addr: ":8080", ReportsActivity: true})
	// A gRPC server with a ServerConfig.Name of its own: its connections are open whatever its name.
	rpc := o.ConnObserver(transport.Server{Name: "internal", Addr: ":9090"})
	if _, ok := api.(transport.IConnIOObserver); ok {
		t.Fatal("observes reads and writes without ConnIO")
	}
	states := api.(transport.IConnStateObserver)
	a, b := pipe(t), pipe(t)
	for _, s := range []transport.ConnState{transport.ConnNew, transport.ConnActive, transport.ConnIdle, transport.ConnActive} {
		states.OnConnState(a, s)
	}
	states.OnConnState(b, transport.ConnNew)
	states.OnConnState(b, transport.ConnActive)
	states.OnConnState(b, transport.ConnHijacked)
	api.OnAcceptError(errors.New("too many open files"))
	rpcStates := rpc.(transport.IConnStateObserver)
	c := pipe(t)
	rpcStates.OnConnState(c, transport.ConnNew)

	server := func(name, addr string, extra ...attribute.KeyValue) []attribute.KeyValue {
		return append([]attribute.KeyValue{attrServerName.String(name), attrServerAddress.String(addr)}, extra...)
	}
	metrics := collect(t, reader)
	open := metrics["forgo.conn.open"]
	for state, want := range map[string]int64{"new": 0, "active": 1, "idle": 0, "hijacked": 1} {
		if n, _ := find(t, open, server("http", ":8080", attrConnState.String(state))...); n != want {
			t.Fatalf("%d connections %s, want %d", n, state, want)
		}
	}
	if n := mustFind(t, open, server("internal", ":9090", attrConnState.String("open"))...); n != 1 {
		t.Fatalf("%d gRPC connections open", n)
	}
	if n := mustFind(t, metrics["forgo.conn.opened"], server("http", ":8080")...); n != 2 {
		t.Fatalf("%d HTTP connections opened", n)
	}
	if n := mustFind(t, metrics["forgo.conn.accept_errors"], server("http", ":8080")...); n != 1 {
		t.Fatalf("%d accept errors", n)
	}

	states.OnConnState(a, transport.ConnClosed)
	states.OnConnState(b, transport.ConnClosed)
	rpcStates.OnConnState(c, transport.ConnClosed)
	for _, p := range points(t, collect(t, reader)["forgo.conn.open"]) {
		if p.n != 0 {
			t.Fatalf("%d connections %s after every one closed", p.n, p.attrs.Encoded(attribute.DefaultEncoder()))
		}
	}

	o, reader = newTestObserver(t, Config{ConnIO: true})
	io, ok := o.ConnObserver(transport.Server{Name: "http", Addr: ":8080", ReportsActivity: true}).(transport.IConnIOObserver)
	if !ok {
		t.Fatal("observes no reads and writes with ConnIO")
	}
	io.OnReadError(a, errors.New("reset"))
	io.OnWriteError(a, errors.New("broken pipe"))
	io.OnReadError(a, errors.New("reset"))
	ioErrors := collect(t, reader)["forgo.conn.io_errors"]
	if r, w := mustFind(t, ioErrors, server("http", ":8080", attrConnDirection.String("read"))...),
		mustFind(t, ioErrors, server("http", ":8080", attrConnDirection.String("write"))...); r != 2 || w != 1 {
		t.Fatalf("%d read and %d write errors", r, w)
	}
}

func pipe(t testing.TB) net.Conn {
	c, other := net.Pipe()
	t.Cleanup(func() {
		_ = c.Close()
		_ = other.Close()
	})
	return c
}

func TestConfig(t *testing.T) {
	mp := sdkmetric.NewMeterProvider()
	defer func() { _ = mp.Shutdown(context.Background()) }()
	for _, cfg := range []Config{
		{Buckets: []float64{1, 0.5}},
		{LagBuckets: []float64{1, 1}},
	} {
		if _, err := New(cfg)(mp); err == nil {
			t.Fatalf("New(%+v) accepted boundaries that do not increase", cfg)
		}
	}
	if _, err := New(Config{})(nil); err == nil {
		t.Fatal("New accepted a nil MeterProvider")
	}

	buckets := []float64{0.1, 1}
	cfg := Config{Buckets: buckets}.Sanitize()
	if !slices.Equal(cfg.Buckets, buckets) || !slices.Equal(cfg.LagBuckets, defaultLagBuckets) {
		t.Fatalf("Sanitize = %+v", cfg)
	}
	cfg.LagBuckets[0] = 42
	if defaultLagBuckets[0] == 42 {
		t.Fatal("Sanitize shares the default boundaries")
	}
}

// TestAllocations checks that recording an operation that was seen before allocates nothing.
func TestAllocations(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	o, _ := newTestObserver(t, Config{})
	gone := operation.Error{Code: "order_gone", Status: operation.StatusNotFound}
	message := newOp(worker.OperationTypeMessageHandler, "order.paid",
		&worker.State{Delivery: stubDelivery{worker.Message{CreatedAt: time.Now()}}, Subscriber: "billing"})
	for _, tc := range []struct {
		name string
		op   *operation.Context
		obs  operation.Observation
	}{
		{"success", newOp("cron_job", "sweep", nil), operation.Observation{Elapsed: time.Millisecond}},
		{"failure", newOp("cron_job", "sweep", nil), operation.Observation{Err: gone, Error: gone}},
		{"http", httpOp(t, "user", "GET", "/v1/users/42", "GET /v1/users/{id}", 200), operation.Observation{}},
		{"message", message, operation.Observation{}},
	} {
		run := func() {
			o.Start(tc.op)
			o.End(tc.op, tc.obs)
		}
		run()
		if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
			t.Errorf("%s: %v allocations per operation, want 0", tc.name, allocs)
		}
	}
	settled := worker.Settlement{Kind: worker.SettlementCompleted, Attempt: 1}
	o.Settled(message, settled)
	if allocs := testing.AllocsPerRun(100, func() { o.Settled(message, settled) }); allocs != 0 {
		t.Errorf("Settled: %v allocations, want 0", allocs)
	}
}

// gaugeValue returns the value of the one data point of a gauge, as a float64.
func gaugeValue(t testing.TB, m metricdata.Metrics) float64 {
	t.Helper()
	switch data := m.Data.(type) {
	case metricdata.Gauge[int64]:
		if len(data.DataPoints) == 1 {
			return float64(data.DataPoints[0].Value)
		}
	case metricdata.Gauge[float64]:
		if len(data.DataPoints) == 1 {
			return data.DataPoints[0].Value
		}
	default:
		t.Fatalf("%s is a %T", m.Name, m.Data)
	}
	t.Fatalf("%s has no single data point", m.Name)
	return 0
}

func TestBacklog(t *testing.T) {
	o, reader := newTestObserver(t, Config{})
	ctx := context.Background()
	o.BacklogSampled(ctx, worker.OutboxBacklog{Pending: 12, Oldest: time.Now().Add(-2 * time.Minute)})

	metrics := collect(t, reader)
	if n := gaugeValue(t, metrics["forgo.outbox.backlog"]); n != 12 {
		t.Fatalf("backlog %v, want 12", n)
	}
	if age := gaugeValue(t, metrics["forgo.outbox.backlog.age"]); age < 120 || age > 180 {
		t.Fatalf("backlog age %vs, want about 120s", age)
	}
	if unit := metrics["forgo.outbox.backlog.age"].Unit; unit != "s" {
		t.Fatalf("unit %q", unit)
	}

	// An empty outbox has no age, and the gauges show the last sample.
	o.BacklogSampled(ctx, worker.OutboxBacklog{})
	metrics = collect(t, reader)
	if n, age := gaugeValue(t, metrics["forgo.outbox.backlog"]), gaugeValue(t, metrics["forgo.outbox.backlog.age"]); n != 0 || age != 0 {
		t.Fatalf("backlog %v and age %v of an empty outbox, want 0 and 0", n, age)
	}
}
