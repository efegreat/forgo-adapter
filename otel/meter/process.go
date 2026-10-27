package meter

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/processconv"
)

// scope is the instrumentation scope of the process metrics.
const scope = "github.com/efegreat/forgo-adapter/otel/meter"

// The options of the two cpu.mode values, built once for every collection.
var (
	userMode   = metric.WithAttributeSet(attribute.NewSet(semconv.CPUModeUser))
	systemMode = metric.WithAttributeSet(attribute.NewSet(semconv.CPUModeSystem))
)

// startProcess records the CPU time of the process, process.cpu.time, with p. The runtime instrumentation
// records none: the Go runtime only estimates its own CPU time, which is not comparable with the
// operating system's. Where the operating system gives a process no CPU time (js, wasip1, plan9), it
// records nothing.
func startProcess(p metric.MeterProvider) error {
	if _, _, err := cpuTime(); errors.Is(err, errors.ErrUnsupported) {
		return nil
	}
	_, err := processconv.NewCPUTime(p.Meter(scope), metric.WithFloat64Callback(observeCPUTime))
	return err
}

// observeCPUTime observes the user and the system CPU time of the process, read once per collection.
func observeCPUTime(_ context.Context, o metric.Float64Observer) error {
	user, system, err := cpuTime()
	if err != nil {
		return fmt.Errorf("meter: process CPU time: %w", err)
	}
	o.Observe(user.Seconds(), userMode)
	o.Observe(system.Seconds(), systemMode)
	return nil
}
