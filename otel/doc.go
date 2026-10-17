// Package otel holds the OpenTelemetry metrics of forgo applications, in a module of its own so that the
// other adapters never pull in OpenTelemetry's SDK, its exporters or the Prometheus client. Its packages are
// meter, the MeterProvider resource, and observer, the operation.IObserver that records forgo's metrics.
package otel
