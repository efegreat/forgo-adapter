package inbox

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/provider/sqltx"
)

var (
	_ worker.InboxStorage = (*Postgres)(nil)
	_ worker.InboxStorage = (*MySQL)(nil)
)

const (
	msgID = "0199-msg"
	sub   = "billing"

	selectStatus = `SELECT status\s+FROM inbox_events\s+WHERE message_id = (\$1|\?)\s+AND subscriber_name = (\$2|\?)`
)

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

func statusRows(status ...worker.InboxStatus) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"status"})
	for _, s := range status {
		rows.AddRow(s)
	}
	return rows
}

// dialect runs the same behavior against both storages.
type dialect struct {
	name    string
	storage func(*sql.DB) worker.InboxStorage
	// expectMark expects the conditional Waiting → target update; moved reports whether a row changed.
	expectMark func(m sqlmock.Sqlmock, target worker.InboxStatus, moved bool)
}

var dialects = []dialect{
	{
		name:    "postgres",
		storage: func(db *sql.DB) worker.InboxStorage { return NewPostgres(db) },
		expectMark: func(m sqlmock.Sqlmock, target worker.InboxStatus, moved bool) {
			rows := statusRows()
			if moved {
				rows = statusRows(target)
			}
			m.ExpectQuery(`UPDATE inbox_events\s+SET\s+status = \$3`).
				WithArgs(msgID, sub, target, worker.InboxWaiting).
				WillReturnRows(rows)
		},
	},
	{
		name:    "mysql",
		storage: func(db *sql.DB) worker.InboxStorage { return NewMySQL(db) },
		expectMark: func(m sqlmock.Sqlmock, target worker.InboxStatus, moved bool) {
			affected := int64(0)
			if moved {
				affected = 1
			}
			m.ExpectExec(`UPDATE inbox_events\s+SET\s+status = \?`).
				WithArgs(target, msgID, sub, worker.InboxWaiting).
				WillReturnResult(sqlmock.NewResult(0, affected))
		},
	},
}

func TestStatus(t *testing.T) {
	for _, d := range dialects {
		t.Run(d.name, func(t *testing.T) {
			db, mock := newMock(t)
			s := d.storage(db)

			mock.ExpectQuery(selectStatus).WithArgs(msgID, sub).WillReturnRows(statusRows(worker.InboxFailed))
			status, ok, err := s.Status(context.Background(), msgID, sub)
			if err != nil || !ok || status != worker.InboxFailed {
				t.Fatalf("Status = %v, %v, %v; want failed, true, nil", status, ok, err)
			}

			mock.ExpectQuery(selectStatus).WithArgs(msgID, sub).WillReturnRows(statusRows())
			if _, ok, err := s.Status(context.Background(), msgID, sub); err != nil || ok {
				t.Fatalf("Status of an unknown message = %v, %v; want false, nil", ok, err)
			}

			mock.ExpectQuery(selectStatus).WillReturnError(errors.New("timeout"))
			if _, _, err := s.Status(context.Background(), msgID, sub); err == nil || !strings.Contains(err.Error(), "inbox: get status: timeout") {
				t.Fatalf("Status err = %v", err)
			}
		})
	}
}

func TestMarkStatus(t *testing.T) {
	marks := []struct {
		name   string
		target worker.InboxStatus
		mark   func(worker.InboxStorage, context.Context) error
		// repeatErr is the result of moving a message into the status it already has.
		repeatErr error
	}{
		{"completed", worker.InboxCompleted, func(s worker.InboxStorage, ctx context.Context) error { return s.MarkCompleted(ctx, msgID, sub) }, ErrNotWaiting},
		{"failed", worker.InboxFailed, func(s worker.InboxStorage, ctx context.Context) error { return s.MarkFailed(ctx, msgID, sub) }, nil},
		{"rejected", worker.InboxRejected, func(s worker.InboxStorage, ctx context.Context) error { return s.MarkRejected(ctx, msgID, sub) }, nil},
	}
	cases := []struct {
		name  string
		moved bool
		// found returns the status the lookup after a failed transition finds; nil means no row.
		found   func(target worker.InboxStatus) []worker.InboxStatus
		wantErr error
	}{
		{name: "moves a waiting message", moved: true},
		{
			name:  "already in the target status",
			found: func(target worker.InboxStatus) []worker.InboxStatus { return []worker.InboxStatus{target} },
		},
		{
			name: "another terminal status",
			found: func(target worker.InboxStatus) []worker.InboxStatus {
				if target == worker.InboxRejected {
					return []worker.InboxStatus{worker.InboxCompleted}
				}
				return []worker.InboxStatus{worker.InboxRejected}
			},
			wantErr: ErrNotWaiting,
		},
		{
			name:    "unknown message",
			found:   func(worker.InboxStatus) []worker.InboxStatus { return nil },
			wantErr: sql.ErrNoRows,
		},
	}

	for _, d := range dialects {
		for _, m := range marks {
			for _, c := range cases {
				t.Run(d.name+"/"+m.name+"/"+c.name, func(t *testing.T) {
					db, mock := newMock(t)
					d.expectMark(mock, m.target, c.moved)
					if !c.moved {
						mock.ExpectQuery(selectStatus).WithArgs(msgID, sub).WillReturnRows(statusRows(c.found(m.target)...))
					}

					wantErr := c.wantErr
					if c.name == "already in the target status" {
						wantErr = m.repeatErr
					}
					err := m.mark(d.storage(db), context.Background())
					if !errors.Is(err, wantErr) || (wantErr == nil && err != nil) {
						t.Fatalf("err = %v, want %v", err, wantErr)
					}
				})
			}
		}
	}
}

func TestMarkCompletedJoinsTheHandlerTransaction(t *testing.T) {
	for _, d := range dialects {
		for _, handlerFails := range []bool{false, true} {
			name := d.name + "/commits"
			if handlerFails {
				name = d.name + "/rolls back"
			}
			t.Run(name, func(t *testing.T) {
				db, mock := newMock(t)
				mock.ExpectBegin()
				d.expectMark(mock, worker.InboxCompleted, true)
				if handlerFails {
					mock.ExpectRollback()
				} else {
					mock.ExpectCommit()
				}

				s := d.storage(db)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				err := sqltx.NewTransactor(db).Do(ctx, func(ctx context.Context) error {
					if err := s.MarkCompleted(ctx, msgID, sub); err != nil {
						return err
					}
					if handlerFails {
						return errors.New("handler failed after completing")
					}
					return nil
				})
				if handlerFails != (err != nil) {
					t.Fatalf("Do = %v", err)
				}
			})
		}
	}
}

func TestMarkCompletedRollsBackADuplicateDelivery(t *testing.T) {
	// Two deliveries of one message ran at once and the other one completed it first: this
	// transaction must not commit its handler's writes a second time.
	for _, d := range dialects {
		t.Run(d.name, func(t *testing.T) {
			db, mock := newMock(t)
			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO invoices`).WillReturnResult(sqlmock.NewResult(1, 1))
			d.expectMark(mock, worker.InboxCompleted, false)
			mock.ExpectQuery(selectStatus).WillReturnRows(statusRows(worker.InboxCompleted))
			mock.ExpectRollback()

			s := d.storage(db)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := sqltx.NewTransactor(db).Do(ctx, func(ctx context.Context) error {
				if _, err := sqltx.From(ctx, db).ExecContext(ctx, "INSERT INTO invoices VALUES (1)"); err != nil {
					return err
				}
				return s.MarkCompleted(ctx, msgID, sub)
			})
			if !errors.Is(err, ErrNotWaiting) || !strings.Contains(err.Error(), "another delivery completed it first") {
				t.Fatalf("Do = %v, want the duplicate's MarkCompleted to fail", err)
			}
		})
	}
}

func TestPostgresIncrementAttempt(t *testing.T) {
	const upsert = `INSERT INTO inbox_events .+ON CONFLICT \(message_id, subscriber_name\)`
	tests := []struct {
		name    string
		rows    *sqlmock.Rows
		err     error
		want    int
		wantErr string
	}{
		{name: "counts the attempt", rows: sqlmock.NewRows([]string{"attempts"}).AddRow(3), want: 3},
		{name: "not waiting", rows: sqlmock.NewRows([]string{"attempts"}), wantErr: ErrNotWaiting.Error()},
		{name: "query error", err: errors.New("deadlock"), wantErr: "inbox: increment attempt: deadlock"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newMock(t)
			q := mock.ExpectQuery(upsert).WithArgs(msgID, sub, worker.InboxWaiting)
			if tt.err != nil {
				q.WillReturnError(tt.err)
			} else {
				q.WillReturnRows(tt.rows)
			}

			got, err := NewPostgres(db).IncrementAttempt(context.Background(), msgID, sub)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("IncrementAttempt = %d, %v; want %d", got, err, tt.want)
			}
		})
	}
}

func TestMySQLIncrementAttempt(t *testing.T) {
	const (
		lockRow = `SELECT\s+status,\s+attempts\s+FROM inbox_events\s+WHERE message_id = \?\s+AND subscriber_name = \?\s+FOR UPDATE`
		insert  = `INSERT INTO inbox_events`
		update  = `UPDATE inbox_events\s+SET\s+attempts = \?`
	)
	attemptRows := func(status worker.InboxStatus, attempts int) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"status", "attempts"}).AddRow(status, attempts)
	}

	tests := []struct {
		name    string
		expect  func(sqlmock.Sqlmock)
		want    int
		wantErr error
	}{
		{
			name: "first attempt",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectQuery(lockRow).WithArgs(msgID, sub).WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}))
				m.ExpectExec(insert).WithArgs(msgID, sub, worker.InboxWaiting, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				m.ExpectCommit()
			},
			want: 1,
		},
		{
			name: "next attempt",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectQuery(lockRow).WillReturnRows(attemptRows(worker.InboxWaiting, 2))
				m.ExpectExec(update).WithArgs(3, msgID, sub).WillReturnResult(sqlmock.NewResult(0, 1))
				m.ExpectCommit()
			},
			want: 3,
		},
		{
			name: "not waiting",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectQuery(lockRow).WillReturnRows(attemptRows(worker.InboxCompleted, 1))
				m.ExpectRollback()
			},
			wantErr: ErrNotWaiting,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newMock(t)
			tt.expect(mock)

			got, err := NewMySQL(db).IncrementAttempt(context.Background(), msgID, sub)
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) || got != tt.want {
				t.Fatalf("IncrementAttempt = %d, %v; want %d, %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestMySQLIncrementAttemptJoinsTransaction(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT\s+status,\s+attempts`).WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}))
	mock.ExpectExec(`INSERT INTO inbox_events`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := sqltx.NewTransactor(db).Do(ctx, func(ctx context.Context) error {
		_, err := NewMySQL(db).IncrementAttempt(ctx, msgID, sub)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBefore(t *testing.T) {
	before := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	queries := map[string]string{
		"postgres": `WITH candidates AS .+DELETE FROM inbox_events AS i`,
		"mysql":    `DELETE FROM inbox_events\s+WHERE status IN \(\?, \?\)`,
	}

	for _, d := range dialects {
		t.Run(d.name, func(t *testing.T) {
			db, mock := newMock(t)
			mock.ExpectExec(queries[d.name]).
				WithArgs(worker.InboxCompleted, worker.InboxRejected, before, 100).
				WillReturnResult(sqlmock.NewResult(0, 42))

			got, err := d.storage(db).DeleteBefore(context.Background(), before, 100)
			if err != nil || got != 42 {
				t.Fatalf("DeleteBefore = %d, %v; want 42", got, err)
			}

			mock.ExpectExec(queries[d.name]).WillReturnError(errors.New("lock wait timeout"))
			if _, err := d.storage(db).DeleteBefore(context.Background(), before, 100); err == nil ||
				!strings.Contains(err.Error(), "inbox: delete old terminal messages: lock wait timeout") {
				t.Fatalf("DeleteBefore err = %v", err)
			}
		})
	}
}
