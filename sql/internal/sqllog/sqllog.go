// Package sqllog marks the statements of forgo-adapter's own storages. The sqldb query log writes
// them at debug instead of info, since an outbox relay polls every second whether or not there is
// work, so its lines would bury the application's own. Slow and failed statements are logged as
// usual.
package sqllog

import "context"

type quietKey struct{}

// Quiet returns ctx marked so that the statements run with it log at debug when they are neither
// slow nor failed.
func Quiet(ctx context.Context) context.Context {
	return context.WithValue(ctx, quietKey{}, true)
}

// IsQuiet reports whether ctx was marked by Quiet.
func IsQuiet(ctx context.Context) bool {
	return ctx.Value(quietKey{}) != nil
}
