package sqltx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/efegreat/forgo/operation"
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

func TestRunCommits(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO orders").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := Run(context.Background(), db, func(ctx context.Context) error {
		if _, ok := From(ctx, db).(*sql.Tx); !ok {
			t.Error("From inside Run is not the transaction")
		}
		_, err := From(ctx, db).ExecContext(ctx, "INSERT INTO orders VALUES (1)")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := From(context.Background(), db).(*sql.DB); !ok {
		t.Fatal("From outside Run is not the database")
	}
}

func TestRunRollsBackOnError(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectRollback()

	handlerErr := operation.Error{Code: "declined", Message: "payment declined"}
	err := Run(context.Background(), db, func(context.Context) error { return handlerErr })

	var opErr operation.Error
	if !errors.As(err, &opErr) || opErr.Code != "declined" {
		t.Fatalf("Run = %v, want fn's error unchanged", err)
	}
}

func TestRunRollsBackOnPanic(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectRollback()

	defer func() {
		if r := recover(); r != "boom" {
			t.Fatalf("recovered %v, want the handler's panic", r)
		}
	}()
	_ = Run(context.Background(), db, func(context.Context) error { panic("boom") })
}

func TestRunJoinsTransaction(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE inbox_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	tr := NewTransactor(db)
	err := tr.Do(context.Background(), func(ctx context.Context) error {
		outer := From(ctx, db)
		if err := Run(ctx, db, func(ctx context.Context) error {
			if From(ctx, db) != outer {
				t.Error("nested Run began another transaction")
			}
			_, err := From(ctx, db).ExecContext(ctx, "UPDATE inbox_events SET status = 1")
			return err
		}); err != nil {
			return err
		}
		// The outer transaction fails after the nested one succeeded: both roll back.
		return errors.New("handler failed")
	})
	if err == nil || err.Error() != "handler failed" {
		t.Fatalf("Do = %v", err)
	}
}

// optsConnector records the options of the transactions begun on its connections, which sqlmock
// ignores.
type optsConnector struct{ began []driver.TxOptions }

func (c *optsConnector) Connect(context.Context) (driver.Conn, error) { return optsConn{c}, nil }
func (c *optsConnector) Driver() driver.Driver                        { return nil }

type optsConn struct{ c *optsConnector }

func (optsConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (optsConn) Close() error                        { return nil }
func (optsConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c optsConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.c.began = append(c.c.began, opts)
	return optsTx{}, nil
}

type optsTx struct{}

func (optsTx) Commit() error   { return nil }
func (optsTx) Rollback() error { return nil }

func TestRunWithBeginsWithOptions(t *testing.T) {
	c := &optsConnector{}
	db := sql.OpenDB(c)
	t.Cleanup(func() { _ = db.Close() })

	readCommitted := &sql.TxOptions{Isolation: sql.LevelReadCommitted}
	err := RunWith(context.Background(), db, readCommitted, func(ctx context.Context) error {
		// A nested call joins the transaction and ignores its own options.
		serializable := &sql.TxOptions{Isolation: sql.LevelSerializable}
		return RunWith(ctx, db, serializable, func(context.Context) error { return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), db, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	want := []driver.TxOptions{{Isolation: driver.IsolationLevel(sql.LevelReadCommitted)}, {}}
	if !slices.Equal(c.began, want) {
		t.Fatalf("began transactions with %+v, want %+v", c.began, want)
	}
}

func TestRunKeepsDatabasesApart(t *testing.T) {
	db1, mock1 := newMock(t)
	db2, mock2 := newMock(t)
	mock1.ExpectBegin()
	mock2.ExpectBegin()
	mock2.ExpectCommit()
	mock1.ExpectCommit()

	err := Run(context.Background(), db1, func(ctx context.Context) error {
		if _, ok := From(ctx, db2).(*sql.DB); !ok {
			t.Error("a transaction on db1 was used for db2")
		}
		return Run(ctx, db2, func(ctx context.Context) error {
			if From(ctx, db1) == From(ctx, db2) {
				t.Error("db1 and db2 share a transaction")
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRunErrors(t *testing.T) {
	tests := []struct {
		name    string
		expect  func(sqlmock.Sqlmock)
		wantErr string
	}{
		{
			name:    "begin",
			expect:  func(m sqlmock.Sqlmock) { m.ExpectBegin().WillReturnError(errors.New("too many connections")) },
			wantErr: "sqltx: begin: too many connections",
		},
		{
			name: "commit",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectCommit().WillReturnError(errors.New("serialization failure"))
			},
			wantErr: "sqltx: commit: serialization failure",
		},
		{
			name: "rollback",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback().WillReturnError(errors.New("connection reset"))
			},
			wantErr: "sqltx: rollback: connection reset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newMock(t)
			tt.expect(mock)
			err := Run(context.Background(), db, func(context.Context) error {
				if tt.name == "rollback" {
					return errors.New("handler failed")
				}
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Run = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestTransactorImplementsOperationTransactor(t *testing.T) {
	var _ operation.Transactor = (*Transactor)(nil)
}
