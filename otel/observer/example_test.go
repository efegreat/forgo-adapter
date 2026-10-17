package observer_test

import (
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/http"

	"github.com/efegreat/forgo-adapter/otel/meter"
	"github.com/efegreat/forgo-adapter/otel/observer"
)

func ExampleNew() {
	api := http.Config{
		Addr: ":8080",
		Routes: []http.RouteConfig{
			{Name: "health", Method: "GET", Path: "/health", Handler: http.HealthHandler()},
			// Prometheus scrapes it, without a log line per scrape.
			meter.PrometheusRoute(),
		},
	}

	app := forgo.NewApp(
		forgo.WithName("orders"),
		forgo.WithResources(meter.Config{ServiceName: "orders", Exporter: meter.ExporterPrometheus}),
		// Every server records its operations with the observer.
		forgo.WithProviders(observer.New(observer.Config{})),
		forgo.WithServers(api),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}
