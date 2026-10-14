package liveconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/efegreat/forgo"

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
