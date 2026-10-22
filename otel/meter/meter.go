// Package meter is an OpenTelemetry MeterProvider resource: the SDK's MeterProvider with the reader of an
// exporter, built when the application starts and shut down, with a last export, when it stops. Its Value
// is a *Provider, which providers request as metric.MeterProvider, such as observer.New's and those of
// services that record business metrics.
//
// See the [meter] section of REFERENCE.md.
//
// [meter]: https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#meter
package meter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/efegreat/forgo"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Exporter is how metrics leave the process.
type Exporter string

const (
	// ExporterOTLP pushes metrics to an OpenTelemetry collector over OTLP, every OTLPConfig.Interval. The
	// default.
	ExporterOTLP Exporter = "otlp"
	// ExporterPrometheus lets Prometheus pull metrics from the route PrometheusRoute returns.
	ExporterPrometheus Exporter = "prometheus"
	// ExporterNone exports through Config.Readers only, such as an sdkmetric.ManualReader in tests.
	ExporterNone Exporter = "none"
)

// OTLPProtocol is the transport of OTLP.
type OTLPProtocol string

const (
	// OTLPHTTP is OTLP over HTTP with protobuf payloads, on port 4318 by default.
	OTLPHTTP OTLPProtocol = "http/protobuf"
	// OTLPGRPC is OTLP over gRPC, on port 4317 by default.
	OTLPGRPC OTLPProtocol = "grpc"
)

// Config is an OpenTelemetry MeterProvider resource. The zero value pushes over OTLP/HTTP to the collector
// that the standard OTEL_EXPORTER_OTLP_* environment variables name, localhost:4318 without them, every
// minute, with Go runtime metrics.
//
// Its Value is a *Provider. Providers request it as metric.MeterProvider, and PrometheusRoute as *Provider.
// Its Close shuts the provider down, which exports what is left: the app closes resources once every
// server stopped, so the last operations are exported.
type Config struct {
	// Name identifies the resource in the bootstrap. Default: "meter".
	Name string
	// ServiceName is the service.name of the metrics. Default: OTEL_SERVICE_NAME, else the SDK's
	// unknown_service:<executable>.
	ServiceName string
	// Exporter is how metrics leave the process. Default: ExporterOTLP.
	Exporter Exporter
	// OTLP configures ExporterOTLP.
	OTLP OTLPConfig
	// Readers are readers of the provider besides the exporter's, such as an sdkmetric.ManualReader in
	// tests, or an exporter of your own with ExporterNone.
	Readers []sdkmetric.Reader
	// DisableRuntime leaves out the Go runtime metrics of OpenTelemetry's runtime instrumentation: memory,
	// allocations, the garbage collection goal, goroutines and processors.
	DisableRuntime bool
	// Global makes the provider OpenTelemetry's global MeterProvider, for libraries that take it from there,
	// such as otelgrpc's stats handler in the StatsHandlers of the gRPC server's config. Leave it off when
	// something else sets the global provider. After Close, the global provider records nothing.
	Global bool
	// ShutdownTimeout bounds the last export of Close. Default: 5 seconds.
	ShutdownTimeout time.Duration
}

// OTLPConfig configures the OTLP exporter. Zero fields keep the exporter's own defaults, which the standard
// OTEL_EXPORTER_OTLP_* environment variables change.
type OTLPConfig struct {
	// Protocol is the transport. Default: OTEL_EXPORTER_OTLP_METRICS_PROTOCOL or OTEL_EXPORTER_OTLP_PROTOCOL,
	// else OTLPHTTP.
	Protocol OTLPProtocol
	// Endpoint is the collector's host:port. Default: OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or
	// OTEL_EXPORTER_OTLP_ENDPOINT, else localhost on the protocol's port.
	Endpoint string
	// Insecure sends without TLS.
	Insecure bool
	// Headers are sent with every export, such as an API key.
	Headers map[string]string
	// Interval is the time between two exports. Default: OTEL_METRIC_EXPORT_INTERVAL, else 1 minute.
	Interval time.Duration
}

// Sanitize returns a copy of the config with defaults applied to its zero fields.
func (c Config) Sanitize() Config {
	if c.Name == "" {
		c.Name = "meter"
	}
	if c.Exporter == "" {
		c.Exporter = ExporterOTLP
	}
	if c.OTLP.Protocol == "" {
		c.OTLP.Protocol = otlpProtocolFromEnv()
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 5 * time.Second
	}
	return c
}

// otlpProtocolFromEnv returns the protocol the standard environment variables choose, OTLPHTTP without them.
func otlpProtocolFromEnv() OTLPProtocol {
	for _, key := range []string{"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "OTEL_EXPORTER_OTLP_PROTOCOL"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return OTLPProtocol(v)
		}
	}
	return OTLPHTTP
}

// validate rejects what New cannot build, before anything is built.
func (c Config) validate() error {
	switch c.Exporter {
	case ExporterOTLP:
		if c.OTLP.Protocol != OTLPHTTP && c.OTLP.Protocol != OTLPGRPC {
			return fmt.Errorf("meter: unknown OTLP protocol %q", c.OTLP.Protocol)
		}
	case ExporterPrometheus:
	case ExporterNone:
		if len(c.Readers) == 0 {
			return errors.New("meter: ExporterNone needs Readers")
		}
	default:
		return fmt.Errorf("meter: unknown exporter %q", c.Exporter)
	}
	if c.OTLP.Interval < 0 || c.ShutdownTimeout < 0 {
		return errors.New("meter: negative OTLP.Interval or ShutdownTimeout")
	}
	for i, r := range c.Readers {
		if r == nil {
			return fmt.Errorf("meter: reader %d is nil", i)
		}
	}
	return nil
}

// New builds the MeterProvider, with the reader of the exporter, the Readers and, unless DisableRuntime,
// the Go runtime metrics. The OTLP exporter connects lazily, so an unreachable collector does not stop the
// application: its failed exports go to OpenTelemetry's error handler (otel.SetErrorHandler).
func (c Config) New() (forgo.Resource, error) {
	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	res, err := cfg.resource()
	if err != nil {
		return nil, err
	}
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	var registry *prometheus.Registry
	switch cfg.Exporter {
	case ExporterOTLP:
		exporter, err := cfg.OTLP.exporter()
		if err != nil {
			return nil, err
		}
		var readerOpts []sdkmetric.PeriodicReaderOption
		if cfg.OTLP.Interval > 0 {
			readerOpts = append(readerOpts, sdkmetric.WithInterval(cfg.OTLP.Interval))
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, readerOpts...)))
	case ExporterPrometheus:
		registry = prometheus.NewRegistry()
		exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(registry))
		if err != nil {
			return nil, fmt.Errorf("meter: prometheus exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(exporter))
	}
	for _, r := range cfg.Readers {
		opts = append(opts, sdkmetric.WithReader(r))
	}

	p := &Provider{MeterProvider: sdkmetric.NewMeterProvider(opts...), registry: registry}
	if !cfg.DisableRuntime {
		if err := runtime.Start(runtime.WithMeterProvider(p)); err != nil {
			return nil, errors.Join(fmt.Errorf("meter: runtime metrics: %w", err), p.Shutdown(context.Background()))
		}
	}
	if cfg.Global {
		otel.SetMeterProvider(p)
	}
	return &resource{name: cfg.Name, provider: p, timeout: cfg.ShutdownTimeout}, nil
}

// resource returns the OpenTelemetry resource of the metrics: the SDK's default, from the environment,
// with ServiceName.
func (c Config) resource() (*sdkresource.Resource, error) {
	if c.ServiceName == "" {
		return sdkresource.Default(), nil
	}
	res, err := sdkresource.Merge(sdkresource.Default(), sdkresource.NewSchemaless(semconv.ServiceName(c.ServiceName)))
	if err != nil {
		return nil, fmt.Errorf("meter: resource: %w", err)
	}
	return res, nil
}

// exporter returns the OTLP exporter of the config.
func (c OTLPConfig) exporter() (sdkmetric.Exporter, error) {
	ctx := context.Background()
	if c.Protocol == OTLPGRPC {
		var opts []otlpmetricgrpc.Option
		if c.Endpoint != "" {
			opts = append(opts, otlpmetricgrpc.WithEndpoint(c.Endpoint))
		}
		if c.Insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		if len(c.Headers) > 0 {
			opts = append(opts, otlpmetricgrpc.WithHeaders(c.Headers))
		}
		e, err := otlpmetricgrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("meter: OTLP gRPC exporter: %w", err)
		}
		return e, nil
	}
	var opts []otlpmetrichttp.Option
	if c.Endpoint != "" {
		opts = append(opts, otlpmetrichttp.WithEndpoint(c.Endpoint))
	}
	if c.Insecure {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}
	if len(c.Headers) > 0 {
		opts = append(opts, otlpmetrichttp.WithHeaders(c.Headers))
	}
	e, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("meter: OTLP HTTP exporter: %w", err)
	}
	return e, nil
}

// Provider is the Value of the resource: the SDK's MeterProvider, which providers request as
// metric.MeterProvider. PrometheusRoute requests it as *Provider, to reach the registry of
// ExporterPrometheus.
type Provider struct {
	*sdkmetric.MeterProvider
	// registry holds the metrics of ExporterPrometheus, nil with any other exporter.
	registry *prometheus.Registry
}

var _ metric.MeterProvider = (*Provider)(nil)

// resource is the MeterProvider resource Config.New creates.
type resource struct {
	name     string
	provider *Provider
	timeout  time.Duration
}

// Name returns Config.Name.
func (r *resource) Name() string {
	return r.name
}

// Value returns the *Provider.
func (r *resource) Value() any {
	return r.provider
}

// Close shuts the provider down within Config.ShutdownTimeout, which exports what is left.
func (r *resource) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	if err := r.provider.Shutdown(ctx); err != nil {
		return fmt.Errorf("meter: shutdown: %w", err)
	}
	return nil
}
