package liveconfig

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/efegreat/forgo"
)

var (
	_ forgo.LiveConfigStore = (*Postgres)(nil)
	_ forgo.LiveConfigStore = (*MySQL)(nil)
)

const doc = `{"enabled":false,"budget":18446744073709551615}`

type checkout struct {
	Enabled  bool   `json:"enabled"`
	MaxItems int    `json:"max_items"`
	Budget   uint64 `json:"budget"`
}

func (checkout) Name() string      { return "checkout" }
func (checkout) Default() checkout { return checkout{Enabled: true, MaxItems: 50} }

func newMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return db, mock
}

func docRows(data string, version uint64) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"data", "version"}).AddRow([]byte(data), version)
}

// dialect runs the same behavior against both stores.
type dialect struct {
	name   string
	store  func(*sql.DB) forgo.LiveConfigStore
	get    string
	insert string
	update string
	// updateArgs are the arguments of the update of checkout to data from version.
	updateArgs func(data string, version uint64) []driver.Value
	// expectInsertConflict expects an insert that finds the document already stored.
	expectInsertConflict func(sqlmock.Sqlmock, string)
	// expectInsertError expects an insert that fails with err while nothing is stored.
	expectInsertError func(m sqlmock.Sqlmock, d dialect, err error)
}

var dialects = []dialect{
	{
		name:   "postgres",
		store:  func(db *sql.DB) forgo.LiveConfigStore { return NewPostgres(db) },
		get:    `SELECT data, version FROM live_configs WHERE name = \$1`,
		insert: `INSERT INTO live_configs \(\s+name,\s+data,\s+version,\s+updated_at\s+\)\s+VALUES \(\$1, \$2, 1, CURRENT_TIMESTAMP\)\s+ON CONFLICT \(name\) DO NOTHING`,
		update: `UPDATE live_configs\s+SET\s+data = \$2,\s+version = version \+ 1,\s+updated_at = CURRENT_TIMESTAMP\s+WHERE name = \$1\s+AND version = \$3`,
		updateArgs: func(data string, version uint64) []driver.Value {
			return []driver.Value{"checkout", data, version}
		},
		expectInsertConflict: func(m sqlmock.Sqlmock, insert string) {
			m.ExpectExec(insert).WithArgs("checkout", doc).WillReturnResult(sqlmock.NewResult(0, 0))
		},
		expectInsertError: func(m sqlmock.Sqlmock, d dialect, err error) {
			m.ExpectExec(d.insert).WillReturnError(err)
		},
	},
	{
		name:   "mysql",
		store:  func(db *sql.DB) forgo.LiveConfigStore { return NewMySQL(db) },
		get:    `SELECT data, version FROM live_configs WHERE name = \?`,
		insert: `INSERT INTO live_configs \(\s+name,\s+data,\s+version,\s+updated_at\s+\)\s+VALUES \(\?, \?, 1, UTC_TIMESTAMP\(6\)\)$`,
		update: `UPDATE live_configs\s+SET\s+data = \?,\s+version = version \+ 1,\s+updated_at = UTC_TIMESTAMP\(6\)\s+WHERE name = \?\s+AND version = \?`,
		updateArgs: func(data string, version uint64) []driver.Value {
			return []driver.Value{data, "checkout", version}
		},
		// The duplicate key fails the insert, and the row it found tells it apart from other failures.
		expectInsertConflict: func(m sqlmock.Sqlmock, insert string) {
			m.ExpectExec(insert).WithArgs("checkout", doc).WillReturnError(errors.New("Error 1062: Duplicate entry"))
			m.ExpectQuery(`SELECT data, version FROM live_configs WHERE name = \?`).WithArgs("checkout").WillReturnRows(docRows(doc, 1))
		},
		expectInsertError: func(m sqlmock.Sqlmock, d dialect, err error) {
			m.ExpectExec(d.insert).WillReturnError(err)
			m.ExpectQuery(d.get).WillReturnRows(sqlmock.NewRows([]string{"data", "version"}))
		},
	},
}

func TestGet(t *testing.T) {
	for _, d := range dialects {
		t.Run(d.name, func(t *testing.T) {
			db, mock := newMock(t)
			s := d.store(db)

			mock.ExpectQuery(d.get).WithArgs("checkout").WillReturnRows(docRows(doc, 3))
			data, version, err := s.Get(context.Background(), "checkout")
			if err != nil || version != 3 || string(data) != doc {
				t.Fatalf("Get = %s, %d, %v; want the document, 3, nil", data, version, err)
			}

			mock.ExpectQuery(d.get).WithArgs("checkout").WillReturnRows(sqlmock.NewRows([]string{"data", "version"}))
			if data, version, err := s.Get(context.Background(), "checkout"); err != nil || version != 0 || data != nil {
				t.Fatalf("Get of a missing document = %s, %d, %v; want nil, 0, nil", data, version, err)
			}

			mock.ExpectQuery(d.get).WillReturnError(errors.New("timeout"))
			if _, _, err := s.Get(context.Background(), "checkout"); err == nil || !strings.Contains(err.Error(), `liveconfig: get "checkout": timeout`) {
				t.Fatalf("Get err = %v", err)
			}
		})
	}
}

func TestSet(t *testing.T) {
	timeout := errors.New("timeout")
	tests := []struct {
		name    string
		version uint64
		expect  func(sqlmock.Sqlmock, dialect)
		wantErr error  // matched with errors.Is
		wantMsg string // contained in the error
	}{
		{
			name: "first version is inserted",
			expect: func(m sqlmock.Sqlmock, d dialect) {
				m.ExpectExec(d.insert).WithArgs("checkout", doc).WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "first version conflicts with a stored one",
			expect: func(m sqlmock.Sqlmock, d dialect) {
				d.expectInsertConflict(m, d.insert)
			},
			wantErr: forgo.ErrLiveConfigConflict,
		},
		{
			name: "first version fails",
			expect: func(m sqlmock.Sqlmock, d dialect) {
				d.expectInsertError(m, d, timeout)
			},
			wantErr: timeout,
			wantMsg: `liveconfig: set "checkout": timeout`,
		},
		{
			name:    "next version updates the matching one",
			version: 2,
			expect: func(m sqlmock.Sqlmock, d dialect) {
				m.ExpectExec(d.update).WithArgs(d.updateArgs(doc, 2)...).WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:    "next version conflicts with another stored version",
			version: 2,
			expect: func(m sqlmock.Sqlmock, d dialect) {
				m.ExpectExec(d.update).WithArgs(d.updateArgs(doc, 2)...).WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr: forgo.ErrLiveConfigConflict,
		},
		{
			name:    "next version fails",
			version: 2,
			expect: func(m sqlmock.Sqlmock, d dialect) {
				m.ExpectExec(d.update).WillReturnError(timeout)
			},
			wantErr: timeout,
			wantMsg: `liveconfig: set "checkout": timeout`,
		},
	}
	for _, d := range dialects {
		for _, tt := range tests {
			t.Run(d.name+"/"+tt.name, func(t *testing.T) {
				db, mock := newMock(t)
				tt.expect(mock, d)
				err := d.store(db).Set(context.Background(), "checkout", []byte(doc), tt.version)
				if tt.wantErr == nil && err != nil {
					t.Fatalf("Set: %v", err)
				}
				if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
					t.Fatalf("Set err = %v, want %v", err, tt.wantErr)
				}
				if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
					t.Fatalf("Set err = %v, want %q", err, tt.wantMsg)
				}
			})
		}
	}
}

// memoryCache is a forgo.LiveConfigCache in memory that keeps the newest version, like the Redis cache of
// the redis module's liveconfigcache, whose tests run forgo.LiveConfigs on it.
type memoryCache struct {
	mu   sync.Mutex
	docs map[string]memoryEntry
}

type memoryEntry struct {
	data    []byte
	version uint64
}

func (c *memoryCache) Get(_ context.Context, name string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.docs[name]
	return e.data, ok, nil
}

func (c *memoryCache) Set(_ context.Context, name string, data []byte, version uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.docs == nil {
		c.docs = map[string]memoryEntry{}
	}
	if e, ok := c.docs[name]; !ok || e.version < version {
		c.docs[name] = memoryEntry{data: data, version: version}
	}
	return nil
}

// version returns the version cached under name, and 0 when there is none.
func (c *memoryCache) version(name string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.docs[name].version
}

// TestLiveConfigs runs forgo.LiveConfigs on each store and a cache.
func TestLiveConfigs(t *testing.T) {
	for _, d := range dialects {
		t.Run(d.name, func(t *testing.T) {
			db, mock := newMock(t)
			cache := &memoryCache{}
			configs := forgo.NewLiveConfigs(d.store(db), cache)
			ctx := context.Background()

			// Nothing cached: the store is read, and the cache stays empty.
			mock.ExpectQuery(d.get).WithArgs("checkout").WillReturnRows(docRows(doc, 1))
			got, err := configs.Load[checkout](ctx)
			want := checkout{MaxItems: 50, Budget: 18446744073709551615}
			if err != nil || got != want {
				t.Fatalf("Load = %+v, %v; want %+v", got, err, want)
			}
			if v := cache.version("checkout"); v != 0 {
				t.Fatalf("Load filled the cache with version %d", v)
			}

			// Save stores version 2, then caches it.
			saved := `{"enabled":true,"max_items":3,"budget":0}`
			mock.ExpectExec(d.update).WithArgs(d.updateArgs(saved, 1)...).WillReturnResult(sqlmock.NewResult(0, 1))
			if err := configs.Save(ctx, checkout{Enabled: true, MaxItems: 3}, 1); err != nil {
				t.Fatalf("Save: %v", err)
			}
			if v := cache.version("checkout"); v != 2 {
				t.Fatalf("cached version %d, want 2", v)
			}

			// The cache serves it: no query is expected.
			got, err = configs.Load[checkout](ctx)
			if err != nil || got != (checkout{Enabled: true, MaxItems: 3}) {
				t.Fatalf("Load from the cache = %+v, %v", got, err)
			}
		})
	}
}
