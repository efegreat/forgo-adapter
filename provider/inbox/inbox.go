// Package inbox implements worker.InboxStorage on SQL databases.
package inbox

import "errors"

// ErrNotWaiting is returned when a message is moved out of the waiting status it no longer has,
// because another delivery of it already completed, failed or rejected it.
var ErrNotWaiting = errors.New("inbox: message is not waiting")
