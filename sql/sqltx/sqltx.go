// Package sqltx carries database/sql transactions in the context, so every repository and storage
// called inside Transactor.Do joins the transaction without passing a *sql.Tx around.
//
// See the [sqltx] section of REFERENCE.md.
//
// [sqltx]: https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#sqltx
package sqltx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Executor runs statements. *sql.DB and *sql.Tx implement it.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// txKey keys the transaction of one database, so contexts can carry transactions on several
// databases without one being mistaken for another.
type txKey struct {
	db *sql.DB
}

// From returns the transaction on db that ctx carries, or db itself outside a transaction.
// Run every statement through it to take part in the caller's transaction:
//
//	err := sqltx.From(ctx, r.db).QueryRowContext(ctx, `SELECT …`).Scan(&v)
func From(ctx context.Context, db *sql.DB) Executor {
	if tx, ok := ctx.Value(txKey{db}).(*sql.Tx); ok {
		return tx
	}
	return db
}

// Run calls fn with a context carrying a transaction on db. It commits when fn returns nil and
// rolls back when fn returns an error or panics. fn's error is returned as is.
//
// When ctx already carries a transaction on db, fn joins it: Run neither begins nor ends a
// transaction, and the outermost Run decides whether everything commits.
func Run(ctx context.Context, db *sql.DB, fn func(ctx context.Context) error) error {
	return RunWith(ctx, db, nil, fn)
}

// RunWith is Run with the options of the transaction it begins, such as its isolation level. When
// ctx already carries a transaction on db, fn joins it and opts are ignored: the outermost call
// chose them.
func RunWith(ctx context.Context, db *sql.DB, opts *sql.TxOptions, fn func(ctx context.Context) error) (err error) {
	if _, ok := ctx.Value(txKey{db}).(*sql.Tx); ok {
		return fn(ctx)
	}

	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("sqltx: begin: %w", err)
	}
	done := false
	defer func() {
		if done {
			return
		}
		// Also reached when fn panics: the panic continues once the connection is released.
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("sqltx: rollback: %w", rbErr))
		}
	}()

	if err := fn(context.WithValue(ctx, txKey{db}, tx)); err != nil {
		return err
	}

	done = true
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqltx: commit: %w", err)
	}
	return nil
}

// Transactor is an operation.Transactor on one database. The worker consumer runs every message
// handler through it, together with the inbox update that completes the message.
type Transactor struct {
	db *sql.DB
}

// NewTransactor returns a transactor on db, for forgo.WithProviders.
func NewTransactor(db *sql.DB) *Transactor {
	return &Transactor{db: db}
}

// Do runs fn in a transaction, or in the transaction ctx already carries. See Run.
func (t *Transactor) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return Run(ctx, t.db, fn)
}
