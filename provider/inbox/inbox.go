// Package inbox implements worker.InboxStorage on SQL databases.
//
// MarkCompleted is strict: it succeeds only when it moves the message out of waiting itself. The
// worker calls it inside the handler's transaction, so when two deliveries of one message run at
// once, the one that completes second fails and rolls back its handler's writes instead of
// committing them a second time. MarkFailed and MarkRejected carry no business writes, so
// repeating them is harmless and succeeds.
//
// See the [inbox and outbox] section of REFERENCE.md.
//
// [inbox and outbox]: https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#inbox-and-outbox
package inbox

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/efegreat/forgo/server/worker"
)

var (
	// ErrNotWaiting is returned when a message is moved out of the waiting status it no longer
	// has, because another delivery of it already completed, failed or rejected it.
	ErrNotWaiting = errors.New("inbox: message is not waiting")

	errCompletedElsewhere = fmt.Errorf("%w: another delivery completed it first", ErrNotWaiting)
)

// unmoved returns the outcome of a transition to target that moved no row, given the status the
// message has now.
func unmoved(current worker.InboxStatus, exists bool, target worker.InboxStatus, repeatable bool) error {
	switch {
	case !exists:
		return sql.ErrNoRows
	case current != target:
		return ErrNotWaiting
	case repeatable:
		return nil
	default:
		return errCompletedElsewhere
	}
}
