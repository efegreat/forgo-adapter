package meter

import (
	"context"
	"errors"

	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusRoute returns the route that Prometheus scrapes: GET /metrics, named "metrics", in Prometheus's
// text format. It needs a resource with ExporterPrometheus: without one, the server fails at startup.
//
// Scrapes are not logged (ArgKeyRecorderDisabled) and the route is left out of API documents
// (DisableAPIDoc), but an operation.IObserver observes them like any request. Put the route on the API
// server, or on a second http.Config bound to a port of its own to keep it private, and change its Path or
// Middlewares on the copy it returns.
func PrometheusRoute() http.RouteConfig {
	return http.RouteConfig{
		Name:          "metrics",
		Method:        "GET",
		Path:          "/metrics",
		Args:          map[any]any{http.ArgKeyRecorderDisabled: true},
		DisableAPIDoc: true,
		Handler:       http.HandlerAs[struct{}, struct{}](newPrometheusHandler),
	}
}

// newPrometheusHandler returns the handler of PrometheusRoute, which writes the registry of p through the
// request's State.Writer, so that the server answers nothing more.
func newPrometheusHandler(p *Provider) (http.Handler[struct{}, struct{}], error) {
	if p.registry == nil {
		return nil, errors.New("meter: PrometheusRoute needs a meter resource with ExporterPrometheus")
	}
	h := promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{})
	return func(ctx context.Context, _ struct{}) (struct{}, error) {
		op, _ := operation.FromCtx(ctx)
		st := op.State.(*http.State)
		h.ServeHTTP(st.Writer, st.Request)
		return struct{}{}, nil
	}, nil
}
