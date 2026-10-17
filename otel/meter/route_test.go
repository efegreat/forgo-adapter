package meter_test

import (
	"context"
	"io"
	"net"
	nethttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/http"

	"github.com/efegreat/forgo-adapter/otel/meter"
	"github.com/efegreat/forgo-adapter/otel/observer"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// serve builds the bootstrap of the meter resource cfg and the observer, and serves routes on an HTTP server
// of it. It returns the server's address.
func serve(t *testing.T, cfg meter.Config, providers []any, routes ...http.RouteConfig) string {
	t.Helper()
	boot := forgo.NewBootstrap([]forgo.ResourceFactory{cfg}, append([]any{observer.New(observer.Config{})}, providers...))
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	srv, err := http.Config{Addr: addr, Routes: routes}.New(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	t.Cleanup(func() {
		_ = srv.Terminate(context.Background())
		<-served
	})
	for range 100 {
		if c, err := net.Dial("tcp", addr); err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return ""
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := nethttp.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

var pingRoute = http.RouteConfig{Name: "ping", Method: "GET", Path: "/ping", Handler: http.EmptyHandler()}

// TestPrometheusRoute checks that the observer of the bootstrap records the requests of a server, and that the
// route of the Prometheus exporter serves them, also to providers that take the provider as
// metric.MeterProvider or *meter.Provider.
func TestPrometheusRoute(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var asInterface metric.MeterProvider
	var asProvider *meter.Provider
	addr := serve(t,
		meter.Config{Exporter: meter.ExporterPrometheus, Readers: []sdkmetric.Reader{reader}, DisableRuntime: true},
		[]any{func(mp metric.MeterProvider, p *meter.Provider) *struct{} {
			asInterface, asProvider = mp, p
			return &struct{}{}
		}},
		pingRoute, meter.PrometheusRoute(),
	)
	if asInterface == nil || asInterface != metric.MeterProvider(asProvider) {
		t.Fatal("providers do not receive the provider as both types")
	}

	if status, _ := get(t, "http://"+addr+"/ping"); status != 200 {
		t.Fatalf("ping answered %d", status)
	}
	if status, _ := get(t, "http://"+addr+"/nowhere"); status != 404 {
		t.Fatalf("nowhere answered %d", status)
	}
	status, body := get(t, "http://"+addr+"/metrics")
	if status != 200 {
		t.Fatalf("metrics answered %d: %s", status, body)
	}
	for _, want := range []string{
		`forgo_operation_duration_seconds_count{forgo_operation_name="ping",forgo_operation_status="ok",forgo_operation_type="http_handler"`,
		`forgo_operation_status="not_found"`,
		`http_server_request_duration_seconds_count{http_request_method="GET",http_response_status_code="200",http_route="/ping"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics lack %s:\n%s", want, body)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("the reader of the config collected nothing")
	}
}

// TestPrometheusRouteNeedsExporter checks that the route fails the server's New without the Prometheus
// exporter.
func TestPrometheusRouteNeedsExporter(t *testing.T) {
	boot := forgo.NewBootstrap(
		[]forgo.ResourceFactory{meter.Config{Exporter: meter.ExporterNone, Readers: []sdkmetric.Reader{sdkmetric.NewManualReader()}, DisableRuntime: true}},
		nil,
	)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = boot.Close() }()
	if _, err := (http.Config{Routes: []http.RouteConfig{meter.PrometheusRoute()}}).New(r); err == nil ||
		!strings.Contains(err.Error(), "ExporterPrometheus") {
		t.Fatalf("New = %v, want the missing exporter", err)
	}
}
