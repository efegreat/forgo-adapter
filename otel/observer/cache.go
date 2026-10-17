package observer

import (
	"maps"
	"sync"
	"sync/atomic"
)

// cache maps the label tuples of an instrument to the options of their attribute set, built once per tuple,
// so that recording a measurement allocates nothing. Reads take no lock: a tuple is added by replacing the
// map, never by changing it. Tuples are bounded by design (operation types and names, statuses, error codes,
// HTTP routes), so the copies stay small and stop once every tuple was seen.
type cache[K comparable, V any] struct {
	mu sync.Mutex
	m  atomic.Pointer[map[K]V]
}

// get returns the options of k, built by build the first time.
func (c *cache[K, V]) get(k K, build func(K) V) V {
	if m := c.m.Load(); m != nil {
		if v, ok := (*m)[k]; ok {
			return v
		}
	}
	return c.add(k, build)
}

// add builds the options of k and stores them in a copy of the map, unless another goroutine did first.
func (c *cache[K, V]) add(k K, build func(K) V) V {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.m.Load()
	if old != nil {
		if v, ok := (*old)[k]; ok {
			return v
		}
	}
	m := make(map[K]V, 1)
	if old != nil {
		m = maps.Clone(*old)
	}
	v := build(k)
	m[k] = v
	c.m.Store(&m)
	return v
}
