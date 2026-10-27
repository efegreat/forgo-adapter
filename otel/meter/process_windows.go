package meter

import (
	"syscall"
	"time"
)

// cpuTime returns the user and the system (kernel) CPU time the process used so far, all its threads
// included.
func cpuTime() (user, system time.Duration, err error) {
	process, err := syscall.GetCurrentProcess() // a pseudo handle, which needs no closing
	if err != nil {
		return 0, 0, err
	}
	var creation, exit, kernel, userTime syscall.Filetime
	if err := syscall.GetProcessTimes(process, &creation, &exit, &kernel, &userTime); err != nil {
		return 0, 0, err
	}
	return interval(userTime), interval(kernel), nil
}

// interval returns the duration of a FILETIME that holds an interval, in 100-nanosecond units.
// Filetime.Nanoseconds converts a FILETIME that holds a date instead, since 1601.
func interval(ft syscall.Filetime) time.Duration {
	return time.Duration(int64(ft.HighDateTime)<<32|int64(ft.LowDateTime)) * 100
}
