package sqldb

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"time"
)

// The wrappers below log what passes through them and otherwise behave exactly like the driver.
// database/sql changes behavior on some optional interfaces in ways a wrapper cannot reproduce, so
// a wrapper implements those only when the driver does: SessionResetter and Validator (with both, a
// connection outlives a rollback after its context is canceled), ColumnConverter (it skips the
// conversion of arguments beyond NumInput) and RowsColumnScanner (it replaces Next). Every other
// optional interface is implemented with database/sql's own fallback when the driver lacks it.

// connector opens the connections of a logged database.
type connector struct {
	// Connector is the driver's. Its Driver method is promoted unwrapped, so code that tells
	// databases apart by driver, such as forgo's migration.DetectDialect, sees through the log.
	driver.Connector
	log *queryLog
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return wrapConn(cn, c.log), nil
}

// Close closes the driver's connector, as sql.DB.Close does for one that is an io.Closer.
func (c *connector) Close() error {
	if closer, ok := c.Connector.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// dsnConnector is the connector of a driver that does not implement driver.DriverContext, as
// sql.Open builds one.
type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (c dsnConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.Open(c.dsn)
}

func (c dsnConnector) Driver() driver.Driver {
	return c.driver
}

// conn is a logged connection. Prepare and Begin are the driver's: database/sql calls
// PrepareContext and BeginTx instead.
type conn struct {
	driver.Conn
	log *queryLog
}

func wrapConn(cn driver.Conn, log *queryLog) driver.Conn {
	c := &conn{Conn: cn, log: log}
	_, resets := cn.(driver.SessionResetter)
	_, validates := cn.(driver.Validator)
	switch {
	case resets && validates:
		return &sessionConn{c}
	case resets:
		return &resettingConn{c}
	case validates:
		return &validatingConn{c}
	}
	return c
}

type (
	resettingConn  struct{ *conn }
	validatingConn struct{ *conn }
	sessionConn    struct{ *conn }
)

func (c *resettingConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *sessionConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *validatingConn) IsValid() bool {
	return c.Conn.(driver.Validator).IsValid()
}

func (c *sessionConn) IsValid() bool {
	return c.Conn.(driver.Validator).IsValid()
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	start := time.Now()
	var res driver.Result
	var err error
	switch cn := c.Conn.(type) {
	case driver.ExecerContext:
		res, err = cn.ExecContext(ctx, query, args)
	//lint:ignore SA1019 the fallback for drivers without ExecerContext, as database/sql has
	case driver.Execer:
		var values []driver.Value
		if values, err = namedValues(args); err == nil {
			if err = ctx.Err(); err == nil {
				res, err = cn.Exec(query, values)
			}
		}
	default:
		err = driver.ErrSkip
	}
	if err == driver.ErrSkip {
		return nil, err // not run: database/sql prepares the statement instead
	}
	c.log.record(ctx, statement{query: query, args: args, start: start, err: err, result: res, rows: -1})
	return res, err
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	start := time.Now()
	var rs driver.Rows
	var err error
	switch cn := c.Conn.(type) {
	case driver.QueryerContext:
		rs, err = cn.QueryContext(ctx, query, args)
	//lint:ignore SA1019 the fallback for drivers without QueryerContext, as database/sql has
	case driver.Queryer:
		var values []driver.Value
		if values, err = namedValues(args); err == nil {
			if err = ctx.Err(); err == nil {
				rs, err = cn.Query(query, values)
			}
		}
	default:
		err = driver.ErrSkip
	}
	if err == driver.ErrSkip {
		return nil, err
	}
	if err != nil {
		c.log.record(ctx, statement{query: query, args: args, start: start, err: err, rows: -1})
		return nil, err
	}
	return wrapRows(ctx, c.log, rs, query, args, start), nil
}

// PrepareContext logs only a failure: a statement that cannot be prepared never runs, and with
// MySQL's server-side prepares this is where a syntax error shows. A prepared statement logs each
// time it runs.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	start := time.Now()
	var si driver.Stmt
	var err error
	if cn, ok := c.Conn.(driver.ConnPrepareContext); ok {
		si, err = cn.PrepareContext(ctx, query)
	} else if si, err = c.Conn.Prepare(query); err == nil {
		if err = ctx.Err(); err != nil {
			si.Close()
			si = nil
		}
	}
	if err != nil {
		c.log.record(ctx, statement{query: query, start: start, err: err, rows: -1})
		return nil, err
	}
	s := &stmt{Stmt: si, conn: c, query: query}
	//lint:ignore SA1019 database/sql still converts arguments with it, see convertingStmt
	if _, ok := si.(driver.ColumnConverter); ok {
		return &convertingStmt{s}, nil
	}
	return s, nil
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	start := time.Now()
	var t driver.Tx
	var err error
	if cn, ok := c.Conn.(driver.ConnBeginTx); ok {
		t, err = cn.BeginTx(ctx, opts)
	} else {
		t, err = beginWithoutOptions(ctx, c.Conn, opts)
	}
	c.log.record(ctx, statement{query: "BEGIN", start: start, err: err, rows: -1})
	if err != nil {
		return nil, err
	}
	return &tx{Tx: t, ctx: ctx, log: c.log}, nil
}

// beginWithoutOptions begins a transaction on a driver without driver.ConnBeginTx, as database/sql
// does.
func beginWithoutOptions(ctx context.Context, cn driver.Conn, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != 0 {
		return nil, errors.New("sql: driver does not support non-default isolation level")
	}
	if opts.ReadOnly {
		return nil, errors.New("sql: driver does not support read-only transactions")
	}
	//lint:ignore SA1019 the fallback for drivers without ConnBeginTx
	t, err := cn.Begin()
	if err == nil {
		if err = ctx.Err(); err != nil {
			t.Rollback()
			return nil, err
		}
	}
	return t, err
}

func (c *conn) Ping(ctx context.Context) error {
	if p, ok := c.Conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

// CheckNamedValue passes driver.ErrSkip on for a driver without driver.NamedValueChecker, which
// makes database/sql convert the argument itself.
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

// namedValues converts arguments for the deprecated driver interfaces, as database/sql does.
func namedValues(args []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		if arg.Name != "" {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		values[i] = arg.Value
	}
	return values, nil
}

// stmt is a logged prepared statement. Exec and Query are the driver's: database/sql calls
// ExecContext and QueryContext instead.
type stmt struct {
	driver.Stmt
	conn  *conn
	query string
}

// convertingStmt is a stmt whose driver statement implements driver.ColumnConverter.
type convertingStmt struct{ *stmt }

func (s *convertingStmt) ColumnConverter(idx int) driver.ValueConverter {
	//lint:ignore SA1019 implemented exactly when the driver statement does
	return s.Stmt.(driver.ColumnConverter).ColumnConverter(idx)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	start := time.Now()
	var res driver.Result
	var err error
	if si, ok := s.Stmt.(driver.StmtExecContext); ok {
		res, err = si.ExecContext(ctx, args)
	} else {
		var values []driver.Value
		if values, err = namedValues(args); err == nil {
			if err = ctx.Err(); err == nil {
				//lint:ignore SA1019 the fallback for drivers without StmtExecContext
				res, err = s.Stmt.Exec(values)
			}
		}
	}
	s.conn.log.record(ctx, statement{query: s.query, args: args, start: start, err: err, result: res, rows: -1})
	return res, err
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	start := time.Now()
	var rs driver.Rows
	var err error
	if si, ok := s.Stmt.(driver.StmtQueryContext); ok {
		rs, err = si.QueryContext(ctx, args)
	} else {
		var values []driver.Value
		if values, err = namedValues(args); err == nil {
			if err = ctx.Err(); err == nil {
				//lint:ignore SA1019 the fallback for drivers without StmtQueryContext
				rs, err = s.Stmt.Query(values)
			}
		}
	}
	if err != nil {
		s.conn.log.record(ctx, statement{query: s.query, args: args, start: start, err: err, rows: -1})
		return nil, err
	}
	return wrapRows(ctx, s.conn.log, rs, s.query, args, start), nil
}

// CheckNamedValue checks with the driver statement, or else with the driver connection, the order
// database/sql follows.
func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if checker, ok := s.Stmt.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(nv)
	}
	return s.conn.CheckNamedValue(nv)
}

// tx is a logged transaction. Its commit and rollback are logged with the context it began with.
type tx struct {
	driver.Tx
	ctx context.Context
	log *queryLog
}

func (t *tx) Commit() error {
	start := time.Now()
	err := t.Tx.Commit()
	t.log.record(t.ctx, statement{query: "COMMIT", start: start, err: err, rows: -1})
	return err
}

func (t *tx) Rollback() error {
	start := time.Now()
	err := t.Tx.Rollback()
	t.log.record(t.ctx, statement{query: "ROLLBACK", start: start, err: err, rows: -1})
	return err
}

// rows are the rows of a logged query. The query is logged when they close, so its duration
// includes reading them, and it counts the rows read.
type rows struct {
	driver.Rows
	ctx   context.Context
	log   *queryLog
	query string
	args  []driver.NamedValue
	start time.Time
	read  int64
	err   error // the first error reading rows
	eof   bool  // the last result set was read to its end
	done  bool  // the query was logged
}

// scanningRows are rows whose driver rows implement driver.RowsColumnScanner.
type scanningRows struct {
	rows
	scanner driver.RowsColumnScanner
}

func wrapRows(ctx context.Context, log *queryLog, rs driver.Rows, query string, args []driver.NamedValue, start time.Time) driver.Rows {
	r := rows{Rows: rs, ctx: ctx, log: log, query: query, args: args, start: start}
	if scanner, ok := rs.(driver.RowsColumnScanner); ok {
		return &scanningRows{rows: r, scanner: scanner}
	}
	return &r
}

func (r *rows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	r.count(err)
	return err
}

func (r *scanningRows) NextRow() error {
	err := r.scanner.NextRow()
	r.count(err)
	return err
}

func (r *scanningRows) ScanColumn(ctx driver.ScanContext, index int, dest any) error {
	return r.scanner.ScanColumn(ctx, index, dest)
}

func (r *rows) count(err error) {
	switch {
	case err == nil:
		r.read++
	case err == io.EOF:
		r.eof = true
	case r.err == nil:
		r.err = err
	}
}

// Close logs the query. A context canceled before the rows were read to their end is the reason
// database/sql closes them.
func (r *rows) Close() error {
	closeErr := r.Rows.Close()
	if !r.done {
		r.done = true
		err := r.err
		if err == nil && !r.eof {
			err = r.ctx.Err()
		}
		if err == nil {
			err = closeErr
		}
		r.log.record(r.ctx, statement{query: r.query, args: r.args, start: r.start, err: err, rows: r.read})
	}
	return closeErr
}

func (r *rows) HasNextResultSet() bool {
	if rs, ok := r.Rows.(driver.RowsNextResultSet); ok {
		return rs.HasNextResultSet()
	}
	return false
}

func (r *rows) NextResultSet() error {
	if rs, ok := r.Rows.(driver.RowsNextResultSet); ok {
		err := rs.NextResultSet()
		if err == nil {
			r.eof = false
		}
		return err
	}
	return io.EOF
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if rs, ok := r.Rows.(driver.RowsColumnTypeScanType); ok {
		return rs.ColumnTypeScanType(index)
	}
	return reflect.TypeFor[any]()
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if rs, ok := r.Rows.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return rs.ColumnTypeDatabaseTypeName(index)
	}
	return ""
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	if rs, is := r.Rows.(driver.RowsColumnTypeLength); is {
		return rs.ColumnTypeLength(index)
	}
	return 0, false
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if rs, is := r.Rows.(driver.RowsColumnTypeNullable); is {
		return rs.ColumnTypeNullable(index)
	}
	return false, false
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if rs, is := r.Rows.(driver.RowsColumnTypePrecisionScale); is {
		return rs.ColumnTypePrecisionScale(index)
	}
	return 0, 0, false
}
