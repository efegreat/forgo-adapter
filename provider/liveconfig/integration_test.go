package liveconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/efegreat/forgo"

	"github.com/efegreat/forgo-adapter/internal/redistest"
	"github.com/efegreat/forgo-adapter/internal/sqltest"
)

// store returns the live config store of the dialect d on db.
func store(d sqltest.Dialect, db *sql.DB) forgo.LiveConfigStore {
	if d == sqltest.Postgres {
		return NewPostgres(db)
	}
	return NewMySQL(db)
}

// TestIntegrationStore runs versioned saves against a real database: the first insert, conditional
// updates, and conflicts between saves from the same version.
func TestIntegrationStore(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "live_configs")
			s := store(d, db)
			ctx := context.Background()

			if data, version, err := s.Get(ctx, "checkout"); err != nil || data != nil || version != 0 {
				t.Fatalf("Get of a missing config = %s, %d, %v; want nothing at version 0", data, version, err)
			}

			steps := []struct {
				data    string
				version uint64
				wantErr error
			}{
				{data: doc, version: 0},
				{data: `{"enabled":true}`, version: 0, wantErr: forgo.ErrLiveConfigConflict},
				{data: `{"enabled":true}`, version: 2, wantErr: forgo.ErrLiveConfigConflict},
				{data: `{"enabled":true}`, version: 1},
				// An unchanged document still moves the version.
				{data: `{"enabled":true}`, version: 2},
			}
			for i, step := range steps {
				if err := s.Set(ctx, "checkout", []byte(step.data), step.version); !errors.Is(err, step.wantErr) {
					t.Fatalf("step %d: Set at version %d = %v, want %v", i, step.version, err, step.wantErr)
				}
			}
			data, version, err := s.Get(ctx, "checkout")
			if err != nil || version != 3 {
				t.Fatalf("Get = %d, %v; want version 3", version, err)
			}
			requireJSON(t, data, `{"enabled":true}`)
		})
	}
}

// TestIntegrationConcurrentSaves checks that of several saves from one version, exactly one is
// stored and the others conflict, for the first insert as for updates.
func TestIntegrationConcurrentSaves(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "live_configs")
			s := store(d, db)

			for version := range uint64(3) {
				var (
					wg        sync.WaitGroup
					mu        sync.Mutex
					stored    int
					conflicts int
				)
				for range 8 {
					wg.Go(func() {
						err := s.Set(context.Background(), "checkout", []byte(doc), version)
						mu.Lock()
						defer mu.Unlock()
						switch {
						case err == nil:
							stored++
						case errors.Is(err, forgo.ErrLiveConfigConflict):
							conflicts++
						default:
							t.Errorf("Set at version %d: %v", version, err)
						}
					})
				}
				wg.Wait()
				if stored != 1 || conflicts != 7 {
					t.Fatalf("saves from version %d: %d stored, %d conflicts; want 1 and 7", version, stored, conflicts)
				}
			}
		})
	}
}

// TestIntegrationRedisCache runs the cache against real Redis servers: the version guard of the Set
// script in Redis's own Lua, versions compared exactly up to 2^53, documents kept byte for byte,
// entries that never expire, and the newest of concurrent saves winning.
func TestIntegrationRedisCache(t *testing.T) {
	redistest.Run(t, redistest.DBLiveConfig, func(t *testing.T, srv redistest.Server) {
		cl := redistest.Open(t, srv)
		c := NewRedisCache(cl)
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

// requireJSON compares JSON documents as values, because the databases normalize them.
func requireJSON(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("document %s: %v", got, err)
	}
	_ = json.Unmarshal([]byte(want), &w)
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("document = %s, want %s", got, want)
	}
}
