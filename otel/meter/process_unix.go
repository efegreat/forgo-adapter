//go:build unix

package meter

import (
	"syscall"
	"time"
)

// cpuTime returns the user and the system CPU time the process used so far, all its threads included.
func cpuTime() (user, system time.Duration, err error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, 0, err
	}
	return time.Duration(usage.Utime.Nano()), time.Duration(usage.Stime.Nano()), nil
}
