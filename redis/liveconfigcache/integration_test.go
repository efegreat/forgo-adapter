package liveconfigcache

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/efegreat/forgo-adapter/redis/internal/redistest"
)

// TestIntegrationRedis runs the cache against real Redis servers: the version guard of the Set
// script in Redis's own Lua, versions compared exactly up to 2^53, documents kept byte for byte,
// entries that never expire, and the newest of concurrent saves winning.
func TestIntegrationRedis(t *testing.T) {
	redistest.Run(t, redistest.DBLiveConfigCache, func(t *testing.T, srv redistest.Server) {
		cl := redistest.Open(t, srv)
		c := NewRedis(cl)
		ctx := context.Background()
		get := func(t *testing.T, name string) string {
			t.Helper()
			data, found, err := c.Get(ctx, name)
			if err != nil || !found {
				t.Fatalf("Get %s = %v, %v; want a cached document", name, found, err)
			}
			return string(data)
		}

		t.Run("versions", func(t *testing.T) {
			if data, found, err := c.Get(ctx, "checkout"); err != nil || found || data != nil {
				t.Fatalf("Get of a missing entry = %q, %v, %v", data, found, err)
			}
			const big = 1 << 52 // above the 32-bit integers, below 2^53
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
				{"large version replaces it", `{"label":"café ☕"}`, big + 1, `{"label":"café ☕"}`},
				{"large version one older is ignored", "late", big, `{"label":"café ☕"}`},
			}
			for _, s := range steps {
				if err := c.Set(ctx, "checkout", []byte(s.data), s.version); err != nil {
					t.Fatalf("%s: Set: %v", s.name, err)
				}
				if got := get(t, "checkout"); got != s.want {
					t.Fatalf("%s: Get = %q, want %q", s.name, got, s.want)
				}
			}
			if ttl := cl.TTL(ctx, KeyPrefix+"checkout").Val(); ttl != -1 {
				t.Fatalf("TTL = %v, want no expiry", ttl)
			}
		})

		t.Run("concurrent saves", func(t *testing.T) {
			var wg sync.WaitGroup
			for v := range uint64(16) {
				wg.Go(func() {
					if err := c.Set(ctx, "cart", []byte(strconv.FormatUint(v+1, 10)), v+1); err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()
			if got := get(t, "cart"); got != "16" {
				t.Fatalf("Get = %q, want the newest version, 16", got)
			}
		})
	})
}
