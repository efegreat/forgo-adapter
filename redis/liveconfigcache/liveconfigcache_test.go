package liveconfigcache

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/efegreat/forgo"
)

var _ forgo.LiveConfigCache = (*Redis)(nil)

const doc = `{"enabled":false,"budget":18446744073709551615}`

type checkout struct {
	Enabled  bool   `json:"enabled"`
	MaxItems int    `json:"max_items"`
	Budget   uint64 `json:"budget"`
}

func (checkout) Name() string      { return "checkout" }
func (checkout) Default() checkout { return checkout{Enabled: true, MaxItems: 50} }

func newRedis(t *testing.T) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	m := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: m.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = cl.Close() })
	return NewRedis(cl), m
}

func TestRedis(t *testing.T) {
	c, m := newRedis(t)
	ctx := context.Background()
	get := func() (string, bool) {
		t.Helper()
		data, found, err := c.Get(ctx, "checkout")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		return string(data), found
	}

	if data, found := get(); found || data != "" {
		t.Fatalf("Get of a missing entry = %q, %v", data, found)
	}

	steps := []struct {
		name    string
		data    string
		version uint64
		want    string
	}{
		{"first version is cached", "v2", 2, "v2"},
		{"same version is ignored", "v2 again", 2, "v2"},
		{"older version arriving late is ignored", "v1", 1, "v2"},
		{"newer version replaces it", doc, 3, doc},
	}
	for _, s := range steps {
		if err := c.Set(ctx, "checkout", []byte(s.data), s.version); err != nil {
			t.Fatalf("%s: Set: %v", s.name, err)
		}
		if data, found := get(); !found || data != s.want {
			t.Fatalf("%s: Get = %q, %v; want %q", s.name, data, found, s.want)
		}
	}

	key := KeyPrefix + "checkout"
	if got := m.HGet(key, "version"); got != "3" {
		t.Fatalf("cached version %q, want 3", got)
	}
	if ttl := m.TTL(key); ttl != 0 {
		t.Fatalf("entry expires in %v, want never", ttl)
	}

	m.Close()
	if _, _, err := c.Get(ctx, "checkout"); err == nil || !strings.Contains(err.Error(), `liveconfigcache: get "checkout"`) {
		t.Fatalf("Get err = %v", err)
	}
	if err := c.Set(ctx, "checkout", []byte(doc), 4); err == nil || !strings.Contains(err.Error(), `liveconfigcache: set "checkout"`) {
		t.Fatalf("Set err = %v", err)
	}
}

// memoryStore is a forgo.LiveConfigStore in memory with the versioned saves of the SQL stores of the root
// module's liveconfig, whose tests run forgo.LiveConfigs on them. It counts its reads.
type memoryStore struct {
	mu   sync.Mutex
	docs map[string]memoryDoc
	gets int
}

type memoryDoc struct {
	data    []byte
	version uint64
}

func (s *memoryStore) Get(_ context.Context, name string) ([]byte, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	d := s.docs[name]
	return d.data, d.version, nil
}

func (s *memoryStore) Set(_ context.Context, name string, data []byte, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.docs[name].version != version {
		return forgo.ErrLiveConfigConflict
	}
	if s.docs == nil {
		s.docs = map[string]memoryDoc{}
	}
	s.docs[name] = memoryDoc{data: data, version: version + 1}
	return nil
}

func (s *memoryStore) reads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

// TestLiveConfigs runs forgo.LiveConfigs on a store and the Redis cache.
func TestLiveConfigs(t *testing.T) {
	cache, m := newRedis(t)
	store := &memoryStore{docs: map[string]memoryDoc{"checkout": {data: []byte(doc), version: 1}}}
	configs := forgo.NewLiveConfigs(store, cache)
	ctx := context.Background()

	// Nothing cached: the store is read, and the cache stays empty.
	got, err := configs.Load[checkout](ctx)
	want := checkout{MaxItems: 50, Budget: 18446744073709551615}
	if err != nil || got != want {
		t.Fatalf("Load = %+v, %v; want %+v", got, err, want)
	}
	if m.Exists(KeyPrefix + "checkout") {
		t.Fatalf("Load filled the cache")
	}

	// Save stores version 2, then caches it.
	if err := configs.Save(ctx, checkout{Enabled: true, MaxItems: 3}, 1); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if v := m.HGet(KeyPrefix+"checkout", "version"); v != "2" {
		t.Fatalf("cached version %q, want 2", v)
	}
	if data := m.HGet(KeyPrefix+"checkout", "data"); data != `{"enabled":true,"max_items":3,"budget":0}` {
		t.Fatalf("cached document %s", data)
	}

	// The cache serves it: the store is not read again.
	before := store.reads()
	got, err = configs.Load[checkout](ctx)
	if err != nil || got != (checkout{Enabled: true, MaxItems: 3}) {
		t.Fatalf("Load from the cache = %+v, %v", got, err)
	}
	if n := store.reads() - before; n != 0 {
		t.Fatalf("Load read the store %d times, want the cache only", n)
	}
}
