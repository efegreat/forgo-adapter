package outbox

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/efegreat/forgo/safemap"
	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/sql/sqltx"
)

// saver is what an application requests to save its messages.
type saver interface {
	Save(ctx context.Context, msg worker.Message) error
}

var (
	_ worker.OutboxBacklogStorage = (*Postgres)(nil)
	_ worker.OutboxBacklogStorage = (*MySQL)(nil)
	_ saver                       = (*Postgres)(nil)
	_ saver                       = (*MySQL)(nil)
)

var (
	created = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	claim   = worker.Claim{ID: "claim-1", Limit: 50}
)

// claimFor returns claim as the producer builds it for ttl, on the application clock.
func claimFor(ttl time.Duration) worker.Claim {
	c := claim
	c.ExpiredBefore = time.Now().Add(-ttl)
	return c
}

// argFunc adapts a function to sqlmock.Argument.
type argFunc func(driver.Value) bool

func (f argFunc) Match(v driver.Value) bool { return f(v) }

// aged reports whether a claim age is want, allowing for the time the test took to reach the query.
func aged(age, want time.Duration) bool {
	return age >= want && age < want+5*time.Second
}

// secondsAge matches the claim age PostgreSQL takes, in seconds.
func secondsAge(want time.Duration) argFunc {
	return func(v driver.Value) bool {
		s, ok := v.(float64)
		return ok && aged(time.Duration(s*float64(time.Second)), want)
	}
}

// microsecondsAge matches the claim age MySQL takes, in microseconds.
func microsecondsAge(want time.Duration) argFunc {
	return func(v driver.Value) bool {
		us, ok := v.(int64)
		return ok && aged(time.Duration(us)*time.Microsecond, want)
	}
}

var messageColumns = []string{"id", "name", "payload", "headers", "created_at", "message_key", "destination"}

func newMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	// With one connection, a statement that bypasses an open transaction blocks instead of
	// silently running on the same mock connection.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return db, mock
}

func claimedRows() *sqlmock.Rows {
	return sqlmock.NewRows(messageColumns).
		AddRow("m1", "order.placed", []byte(`{"id":1}`), []byte(`{"tenant":"acme"}`), created, "order.placed", "orders").
		AddRow("m2", "order.cancelled", []byte(`{"id":2}`), nil, created, "", "orders")
}

func assertClaimed(t *testing.T, got []worker.Message) {
	t.Helper()
	if len(got) != 2 {
		t.Fatalf("claimed %d messages, want 2", len(got))
	}
	m := got[0]
	if m.ID != "m1" || m.Name != "order.placed" || string(m.Payload) != `{"id":1}` || !m.CreatedAt.Equal(created) ||
		m.Key != "order.placed" || m.Destination != "orders" {
		t.Fatalf("first message = %+v", m)
	}
	if v, _ := m.Headers.Get("tenant"); v != "acme" {
		t.Fatalf("tenant header = %q", v)
	}
	if got[1].ID != "m2" || got[1].Headers == nil {
		t.Fatalf("second message = %+v, want usable headers for a NULL column", got[1])
	}
	if _, ok := got[1].Headers.Get("tenant"); ok {
		t.Fatal("NULL headers decoded to a header")
	}
}

const (
	// Claims are stamped and expired on the database clock: the queries compare processing_at with
	// the database's time minus the claim age, never with a time from the application.
	// The claimed IDs are collected with ARRAY(…), so that generic plans read them by primary key.
	pgClaim = `UPDATE outbox_events\s+SET\s+processing_id = \$3,\s+processing_at = CURRENT_TIMESTAMP\s+` +
		`WHERE id = ANY\(ARRAY\(\s+SELECT id\s+FROM outbox_events.+` +
		`processing_at < CURRENT_TIMESTAMP - make_interval\(secs => \$1\).+FOR UPDATE SKIP LOCKED\s+\)\)\s+RETURNING`
	mysqlClaim = `SELECT\s+id,.+FROM outbox_events.+processing_at < UTC_TIMESTAMP\(6\) - INTERVAL \? MICROSECOND.+FOR UPDATE SKIP LOCKED`
	mysqlMark  = `UPDATE outbox_events\s+SET\s+processing_id = \?,\s+processing_at = UTC_TIMESTAMP\(6\)\s+WHERE processed_at IS NULL\s+AND id IN \(\?,\?\)`
	release    = `UPDATE outbox_events\s+SET\s+processing_id = NULL,\s+processing_at = NULL\s+WHERE processing_id = (\$1|\?)`
)

func TestPostgresClaim(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery(pgClaim).WithArgs(secondsAge(time.Minute), claim.Limit, claim.ID).WillReturnRows(claimedRows())
	mock.ExpectCommit()

	got, err := NewPostgres(db).Claim(context.Background(), claimFor(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	assertClaimed(t, got)
}

func TestMySQLClaim(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery(mysqlClaim).WithArgs(microsecondsAge(time.Minute), claim.Limit).WillReturnRows(claimedRows())
	mock.ExpectExec(mysqlMark).WithArgs(claim.ID, "m1", "m2").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	got, err := NewMySQL(db).Claim(context.Background(), claimFor(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	assertClaimed(t, got)
}

func TestMySQLClaimNothing(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery(mysqlClaim).WillReturnRows(sqlmock.NewRows(messageColumns))
	mock.ExpectCommit() // no UPDATE without messages

	got, err := NewMySQL(db).Claim(context.Background(), claim)
	if err != nil || len(got) != 0 {
		t.Fatalf("Claim = %v, %v; want no messages", got, err)
	}
}

func TestClaimFailureReleasesTheClaim(t *testing.T) {
	// A row that cannot be scanned rolls the claim back instead of holding it for ClaimTTL.
	badRow := func() *sqlmock.Rows {
		return sqlmock.NewRows(messageColumns).AddRow("m1", "n", []byte(`{}`), []byte(`not json`), created, "", "")
	}
	tests := []struct {
		name   string
		expect func(sqlmock.Sqlmock)
		claim  func(*sql.DB) error
		want   string
	}{
		{
			name: "postgres scan",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectQuery(pgClaim).WillReturnRows(badRow())
				m.ExpectRollback()
			},
			claim: func(db *sql.DB) error { _, err := NewPostgres(db).Claim(context.Background(), claim); return err },
			want:  "outbox: scan claimed message",
		},
		{
			name: "postgres query",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectQuery(pgClaim).WillReturnError(errors.New("canceling statement"))
				m.ExpectRollback()
			},
			claim: func(db *sql.DB) error { _, err := NewPostgres(db).Claim(context.Background(), claim); return err },
			want:  "outbox: claim messages: canceling statement",
		},
		{
			name: "mysql mark",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectQuery(mysqlClaim).WillReturnRows(claimedRows())
				m.ExpectExec(mysqlMark).WillReturnError(errors.New("deadlock"))
				m.ExpectRollback()
			},
			claim: func(db *sql.DB) error { _, err := NewMySQL(db).Claim(context.Background(), claim); return err },
			want:  "outbox: mark messages claimed: deadlock",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newMock(t)
			tt.expect(mock)
			if err := tt.claim(db); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Claim err = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

func TestComplete(t *testing.T) {
	type dialect struct {
		name     string
		complete func(*sql.DB, context.Context, []string) error
		markDone func(sqlmock.Sqlmock, []string)
	}
	dialects := []dialect{
		{
			name: "postgres",
			complete: func(db *sql.DB, ctx context.Context, ids []string) error {
				return NewPostgres(db).Complete(ctx, claim.ID, ids)
			},
			markDone: func(m sqlmock.Sqlmock, ids []string) {
				m.ExpectExec(`SET\s+processed_at = CURRENT_TIMESTAMP,.+WHERE processing_id = \$1.+AND id IN \(\$2, \$3\)`).
					WithArgs(claim.ID, ids[0], ids[1]).
					WillReturnResult(sqlmock.NewResult(0, 2))
			},
		},
		{
			name: "mysql",
			complete: func(db *sql.DB, ctx context.Context, ids []string) error {
				return NewMySQL(db).Complete(ctx, claim.ID, ids)
			},
			markDone: func(m sqlmock.Sqlmock, ids []string) {
				m.ExpectExec(`SET\s+processed_at = UTC_TIMESTAMP\(6\),.+WHERE processing_id = \?.+AND id IN \(\?,\?\)`).
					WithArgs(claim.ID, ids[0], ids[1]).
					WillReturnResult(sqlmock.NewResult(0, 2))
			},
		},
	}

	for _, d := range dialects {
		t.Run(d.name+"/published and failed", func(t *testing.T) {
			db, mock := newMock(t)
			mock.ExpectBegin()
			d.markDone(mock, []string{"m1", "m2"})
			mock.ExpectExec(release).WithArgs(claim.ID).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()

			if err := d.complete(db, context.Background(), []string{"m1", "m2"}); err != nil {
				t.Fatal(err)
			}
		})

		t.Run(d.name+"/nothing published", func(t *testing.T) {
			db, mock := newMock(t)
			mock.ExpectBegin()
			mock.ExpectExec(release).WithArgs(claim.ID).WillReturnResult(sqlmock.NewResult(0, 3))
			mock.ExpectCommit()

			if err := d.complete(db, context.Background(), nil); err != nil {
				t.Fatal(err)
			}
		})

		t.Run(d.name+"/release fails", func(t *testing.T) {
			db, mock := newMock(t)
			mock.ExpectBegin()
			mock.ExpectExec(release).WillReturnError(errors.New("connection lost"))
			mock.ExpectRollback()

			err := d.complete(db, context.Background(), nil)
			if err == nil || !strings.Contains(err.Error(), "outbox: release unprocessed messages: connection lost") {
				t.Fatalf("Complete err = %v", err)
			}
		})

		t.Run(d.name+"/joins the caller's transaction", func(t *testing.T) {
			db, mock := newMock(t)
			mock.ExpectBegin()
			mock.ExpectExec(release).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := sqltx.NewTransactor(db).Do(ctx, func(ctx context.Context) error {
				return d.complete(db, ctx, nil)
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDeleteProcessedBefore(t *testing.T) {
	const maxTTL = 30 * 24 * time.Hour
	before := time.Now().Add(-maxTTL)
	tests := []struct {
		name   string
		query  string
		cutoff any
		delete func(*sql.DB) (int64, error)
	}{
		{
			name:   "postgres",
			query:  `DELETE FROM outbox_events\s+WHERE id = ANY\(ARRAY\(\s+SELECT id\s+FROM outbox_events\s+WHERE processed_at IS NOT NULL.+FOR UPDATE SKIP LOCKED\s+\)\)`,
			cutoff: before,
			delete: func(db *sql.DB) (int64, error) {
				return NewPostgres(db).DeleteProcessedBefore(context.Background(), before, 100)
			},
		},
		{
			// MySQL compares an age with its own UTC clock, so the driver's loc cannot move the cutoff.
			name:   "mysql",
			query:  `DELETE FROM outbox_events\s+WHERE processed_at IS NOT NULL\s+AND processed_at < UTC_TIMESTAMP\(6\) - INTERVAL \? MICROSECOND.+LIMIT \?`,
			cutoff: microsecondsAge(maxTTL),
			delete: func(db *sql.DB) (int64, error) {
				return NewMySQL(db).DeleteProcessedBefore(context.Background(), before, 100)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newMock(t)
			mock.ExpectExec(tt.query).WithArgs(tt.cutoff, 100).WillReturnResult(sqlmock.NewResult(0, 100))
			got, err := tt.delete(db)
			if err != nil || got != 100 {
				t.Fatalf("DeleteProcessedBefore = %d, %v; want 100", got, err)
			}

			mock.ExpectExec(tt.query).WillReturnError(errors.New("disk full"))
			if _, err := tt.delete(db); err == nil || !strings.Contains(err.Error(), "outbox: delete processed messages: disk full") {
				t.Fatalf("DeleteProcessedBefore err = %v", err)
			}
		})
	}
}

// backlogSQL matches the backlog queries: both subqueries read the pending rows in ID order, the first up to
// the limit, whose placeholder is the dialect's, and the second the first one.
const backlogSQL = `SELECT\s+\(SELECT COUNT\(\*\) FROM \(\s+` +
	`SELECT 1 FROM outbox_events WHERE processed_at IS NULL ORDER BY id LIMIT (\$1|\?)\s+\) pending\),\s+` +
	`\(SELECT created_at FROM outbox_events WHERE processed_at IS NULL ORDER BY id LIMIT 1\)`

func TestBacklog(t *testing.T) {
	down := errors.New("connection refused")
	for _, tc := range []struct {
		name   string
		values []driver.Value // the row, none for a failure
		err    error
		want   worker.OutboxBacklog
	}{
		{"pending", []driver.Value{int64(42), created}, nil, worker.OutboxBacklog{Pending: 42, Oldest: created}},
		{"empty", []driver.Value{int64(0), nil}, nil, worker.OutboxBacklog{}},
		{"failure", nil, down, worker.OutboxBacklog{}},
	} {
		for _, dialect := range []string{"postgres", "mysql"} {
			t.Run(tc.name+" "+dialect, func(t *testing.T) {
				db, mock := newMock(t)
				q := mock.ExpectQuery(backlogSQL).WithArgs(10000)
				if tc.err != nil {
					q.WillReturnError(tc.err)
				} else {
					q.WillReturnRows(sqlmock.NewRows([]string{"count", "created_at"}).AddRow(tc.values...))
				}
				var s worker.OutboxBacklogStorage = NewPostgres(db)
				if dialect == "mysql" {
					s = NewMySQL(db)
				}

				got, err := s.Backlog(context.Background(), 10000)
				if !errors.Is(err, tc.err) || (err != nil && !strings.HasPrefix(err.Error(), "outbox: backlog")) {
					t.Fatalf("Backlog error = %v, want %v", err, tc.err)
				}
				if got.Pending != tc.want.Pending || !got.Oldest.Equal(tc.want.Oldest) {
					t.Fatalf("Backlog = %+v, want %+v", got, tc.want)
				}
			})
		}
	}
}

// at matches a time.Time equal to want.
func at(want time.Time) argFunc {
	return func(v driver.Value) bool {
		t, ok := v.(time.Time)
		return ok && t.Equal(want)
	}
}

// null matches SQL NULL.
var null = argFunc(func(v driver.Value) bool { return v == nil })

func TestSave(t *testing.T) {
	down := errors.New("connection refused")
	full := worker.Message{
		ID:          "m1",
		Name:        "order.placed",
		Key:         "order.placed",
		Destination: "orders",
		Payload:     []byte(`{"id":1}`),
		Headers:     safemap.NewJSONMapWithData(map[string]string{"tenant": "acme"}),
		CreatedAt:   created.Add(1500 * time.Nanosecond),
	}
	bare := worker.Message{ID: "m2", Name: "order.placed", Payload: []byte(`{}`)}

	for _, dialect := range []struct {
		name  string
		new   func(*sql.DB) saver
		query string
	}{
		{"postgres", func(db *sql.DB) saver { return NewPostgres(db) },
			`INSERT INTO outbox_events \(id, name, payload, headers, created_at, message_key, destination\)\s+` +
				`VALUES \(\$1, \$2, \$3, \$4, COALESCE\(\$5, CURRENT_TIMESTAMP\), \$6, \$7\)`},
		{"mysql", func(db *sql.DB) saver { return NewMySQL(db) },
			`INSERT INTO outbox_events \(id, name, payload, headers, created_at, message_key, destination\)\s+` +
				`VALUES \(\?, \?, \?, \?, COALESCE\(\?, UTC_TIMESTAMP\(6\)\), \?, \?\)`},
	} {
		t.Run(dialect.name, func(t *testing.T) {
			// The payload and the headers as text, CreatedAt to the microsecond, in the transaction of ctx:
			// with one connection, a statement outside it would wait for the transaction forever.
			t.Run("in a transaction", func(t *testing.T) {
				db, mock := newMock(t)
				mock.ExpectBegin()
				mock.ExpectExec(dialect.query).
					WithArgs("m1", "order.placed", `{"id":1}`, `{"tenant":"acme"}`, at(created.Add(time.Microsecond)), "order.placed", "orders").
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
				err := sqltx.Run(context.Background(), db, func(ctx context.Context) error {
					return dialect.new(db).Save(ctx, full)
				})
				if err != nil {
					t.Fatal(err)
				}
			})
			// Without CreatedAt or headers, both are NULL: the database stamps the row.
			t.Run("defaults", func(t *testing.T) {
				db, mock := newMock(t)
				mock.ExpectExec(dialect.query).
					WithArgs("m2", "order.placed", `{}`, null, null, "", "").
					WillReturnResult(sqlmock.NewResult(0, 1))
				if err := dialect.new(db).Save(context.Background(), bare); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("no ID", func(t *testing.T) {
				db, _ := newMock(t)
				noID := bare
				noID.ID = ""
				if err := dialect.new(db).Save(context.Background(), noID); !errors.Is(err, ErrNoID) {
					t.Fatalf("Save = %v, want ErrNoID", err)
				}
			})
			t.Run("failure", func(t *testing.T) {
				db, mock := newMock(t)
				mock.ExpectExec(dialect.query).WillReturnError(down)
				err := dialect.new(db).Save(context.Background(), bare)
				if !errors.Is(err, down) || !strings.HasPrefix(err.Error(), "outbox: save message m2: ") {
					t.Fatalf("Save = %v, want the failure, prefixed", err)
				}
			})
		})
	}
}
