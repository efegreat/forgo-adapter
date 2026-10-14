//go:build race

package sqldb

// raceEnabled skips allocation checks that sync.Pool fails under the race detector, which drops
// pooled items at random.
const raceEnabled = true
