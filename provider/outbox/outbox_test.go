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

	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/provider/sqltx"
)

var (
	_ worker.OutboxStorage = (*Postgres)(nil)
	_ worker.OutboxStorage = (*MySQL)(nil)
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
	pgClaim = `WITH claimed AS .+processing_at < CURRENT_TIMESTAMP - make_interval\(secs => \$1\).+` +
		`FOR UPDATE SKIP LOCKED.+UPDATE outbox_events AS o\s+SET\s+processing_id = \$3,\s+processing_at = CURRENT_TIMESTAMP\s+.+RETURNING`
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
			query:  `WITH deleted AS .+processed_at IS NOT NULL.+DELETE FROM outbox_events AS o`,
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
