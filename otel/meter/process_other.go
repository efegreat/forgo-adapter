//go:build !unix && !windows

package meter

import (
	"errors"
	"time"
)

// cpuTime fails with errors.ErrUnsupported: the platform gives a process no CPU time.
func cpuTime() (user, system time.Duration, err error) {
	return 0, 0, errors.ErrUnsupported
}
