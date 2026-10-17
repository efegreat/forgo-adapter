//go:build race

package observer

// raceEnabled skips allocation checks, whose counts the race detector changes.
const raceEnabled = true
