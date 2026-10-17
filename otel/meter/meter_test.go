package meter

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

func TestValidate(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	for _, tc := range []struct {
		name string
		cfg  Config
		want string
	}{
		{"unknown exporter", Config{Exporter: "statsd"}, "unknown exporter"},
		{"unknown protocol", Config{OTLP: OTLPConfig{Protocol: "http/json"}}, "unknown OTLP protocol"},
		{"none without readers", Config{Exporter: ExporterNone}, "needs Readers"},
		{"nil reader", Config{Exporter: ExporterNone, Readers: []sdkmetric.Reader{reader, nil}}, "reader 1 is nil"},
		{"negative interval", Config{OTLP: OTLPConfig{Interval: -time.Second}}, "negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.cfg.New(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New = %v, want an error with %q", err, tc.want)
			}
		})
	}
}

func TestSanitize(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "")
	cfg := Config{}.Sanitize()
	if cfg.Name != "meter" || cfg.Exporter != ExporterOTLP || cfg.OTLP.Protocol != OTLPHTTP || cfg.ShutdownTimeout != 5*time.Second {
		t.Fatalf("Sanitize = %+v", cfg)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	if p := (Config{}).Sanitize().OTLP.Protocol; p != OTLPGRPC {
		t.Fatalf("protocol %q from OTEL_EXPORTER_OTLP_PROTOCOL=grpc", p)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf")
	if p := (Config{}).Sanitize().OTLP.Protocol; p != OTLPHTTP {
		t.Fatalf("protocol %q, want the metrics variable to win", p)
	}
	if p := (Config{OTLP: OTLPConfig{Protocol: OTLPGRPC}}).Sanitize().OTLP.Protocol; p != OTLPGRPC {
		t.Fatalf("protocol %q, want the config to win", p)
	}
}

// newProvider returns the Provider of cfg and closes its resource at the end of the test.
func newProvider(t *testing.T, cfg Config) *Provider {
	t.Helper()
	res, err := cfg.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Close() })
	if res.Name() != cfg.Sanitize().Name {
		t.Fatalf("name %q", res.Name())
	}
	return res.Value().(*Provider)
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

func names(rm metricdata.ResourceMetrics) []string {
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	return names
}

func TestRuntimeAndService(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := newProvider(t, Config{Exporter: ExporterNone, Readers: []sdkmetric.Reader{reader}, ServiceName: "orders"})
	counter, err := p.Meter("test").Int64Counter("orders.created")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(context.Background(), 3)

	rm := collect(t, reader)
	if name, ok := rm.Resource.Set().Value(semconv.ServiceNameKey); !ok || name.AsString() != "orders" {
		t.Fatalf("service.name %v", name)
	}
	got := strings.Join(names(rm), " ")
	if !strings.Contains(got, "orders.created") || !strings.Contains(got, "go.goroutine.count") {
		t.Fatalf("collected %s, want the counter and the runtime metrics", got)
	}

	reader = sdkmetric.NewManualReader()
	newProvider(t, Config{Exporter: ExporterNone, Readers: []sdkmetric.Reader{reader}, DisableRuntime: true})
	if got := names(collect(t, reader)); len(got) != 0 {
		t.Fatalf("collected %v with DisableRuntime", got)
	}
}

// memoryExporter keeps what it exports.
type memoryExporter struct {
	mu       sync.Mutex
	exported []string
}

func (*memoryExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(k)
}

func (*memoryExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

func (e *memoryExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.exported = append(e.exported, names(*rm)...)
	return nil
}

func (*memoryExporter) ForceFlush(context.Context) error { return nil }
func (*memoryExporter) Shutdown(context.Context) error   { return nil }

// TestCloseExports checks that Close exports what was recorded since the last export.
func TestCloseExports(t *testing.T) {
	exporter := &memoryExporter{}
	res, err := Config{
		Exporter:       ExporterNone,
		Readers:        []sdkmetric.Reader{sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Hour))},
		DisableRuntime: true,
	}.New()
	if err != nil {
		t.Fatal(err)
	}
	counter, err := res.Value().(*Provider).Meter("test").Int64Counter("orders.created")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(context.Background(), 1)
	if err := res.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(exporter.exported, " "), "orders.created") {
		t.Fatalf("exported %v on Close", exporter.exported)
	}
}

// TestOTLP checks that both protocols build without a collector to connect to.
func TestOTLP(t *testing.T) {
	for _, protocol := range []OTLPProtocol{OTLPHTTP, OTLPGRPC} {
		res, err := Config{
			OTLP:            OTLPConfig{Protocol: protocol, Endpoint: "127.0.0.1:1", Insecure: true, Headers: map[string]string{"api-key": "k"}, Interval: time.Hour},
			DisableRuntime:  true,
			ShutdownTimeout: 100 * time.Millisecond,
		}.New()
		if err != nil {
			t.Fatalf("%s: %v", protocol, err)
		}
		// The last export fails, since nothing listens.
		_ = res.Close()
	}
}

func TestGlobal(t *testing.T) {
	p := newProvider(t, Config{Exporter: ExporterNone, Readers: []sdkmetric.Reader{sdkmetric.NewManualReader()}, Global: true, DisableRuntime: true})
	if otel.GetMeterProvider() != p {
		t.Fatal("Global did not set the global MeterProvider")
	}
}
