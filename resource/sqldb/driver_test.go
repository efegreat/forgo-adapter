package sqldb

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/rs/zerolog"
)

// basicConn implements driver.Conn and nothing else, like the oldest drivers: every statement is
// prepared and runs through the deprecated Exec and Query.
type basicConn struct {
	mu    sync.Mutex
	calls []string
	// got is the last argument a statement received.
	got driver.Value
}

func (c *basicConn) note(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *basicConn) callList() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.calls, ",")
}

func (c *basicConn) Prepare(query string) (driver.Stmt, error) {
	c.note("prepare")
	if strings.Contains(query, "SYNTAX") {
		return nil, errors.New("syntax error")
	}
	return &basicStmt{conn: c}, nil
}

func (c *basicConn) Close() error { return nil }

func (c *basicConn) Begin() (driver.Tx, error) {
	c.note("begin")
	return basicTx{c}, nil
}

type basicStmt struct{ conn *basicConn }

func (s *basicStmt) Close() error  { return nil }
func (s *basicStmt) NumInput() int { return 1 }

func (s *basicStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.note("exec")
	s.conn.got = args[0]
	return driver.RowsAffected(4), nil
}

func (s *basicStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.note("query")
	s.conn.got = args[0]
	return &basicRows{left: 2}, nil
}

// convertingBasicStmt converts every argument to "converted".
type convertingBasicStmt struct{ basicStmt }

func (s *convertingBasicStmt) ColumnConverter(int) driver.ValueConverter { return converter{} }

type converter struct{}

func (converter) ConvertValue(any) (driver.Value, error) { return "converted", nil }

type basicRows struct{ left int }

func (r *basicRows) Columns() []string { return []string{"n"} }
func (r *basicRows) Close() error      { return nil }

func (r *basicRows) Next(dest []driver.Value) error {
	if r.left == 0 {
		return io.EOF
	}
	r.left--
	dest[0] = int64(r.left)
	return nil
}

// scanRows implements driver.RowsColumnScanner: database/sql never calls its Next.
type scanRows struct{ basicRows }

func (r *scanRows) Next([]driver.Value) error { panic("Next called on a RowsColumnScanner") }

func (r *scanRows) NextRow() error {
	if r.left == 0 {
		return io.EOF
	}
	r.left--
	return nil
}

func (r *scanRows) ScanColumn(_ driver.ScanContext, _ int, dest any) error {
	*dest.(*int64) = int64(r.left)
	return nil
}

type basicTx struct{ conn *basicConn }

func (t basicTx) Commit() error   { t.conn.note("commit"); return nil }
func (t basicTx) Rollback() error { t.conn.note("rollback"); return nil }

// fullConn implements every optional interface of a connection.
type fullConn struct {
	basicConn
	execErr error
	scan    bool
}

func (c *fullConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	c.note("execContext")
	if c.execErr != nil {
		return nil, c.execErr
	}
	if len(args) > 0 {
		c.got = args[0].Value
	}
	return driver.RowsAffected(2), nil
}

func (c *fullConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	c.note("queryContext")
	if c.scan {
		return &scanRows{basicRows{left: 1}}, nil
	}
	return &basicRows{left: 3}, nil
}

func (c *fullConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	c.note("prepareContext")
	return c.Prepare(query)
}

func (c *fullConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.note("beginTx")
	return basicTx{&c.basicConn}, nil
}

func (c *fullConn) Ping(context.Context) error { c.note("ping"); return nil }

func (c *fullConn) ResetSession(context.Context) error { c.note("reset"); return nil }

func (c *fullConn) IsValid() bool { c.note("valid"); return true }

// CheckNamedValue passes points as text.
func (c *fullConn) CheckNamedValue(nv *driver.NamedValue) error {
	if p, ok := nv.Value.(point); ok {
		nv.Value = p.String()
		return nil
	}
	return driver.ErrSkip
}

type point struct{ X, Y int }

func (p point) String() string { return "point" }

type (
	resetConn struct{ basicConn }
	validConn struct{ basicConn }
)

func (c *resetConn) ResetSession(context.Context) error { return nil }
func (c *validConn) IsValid() bool                      { return true }

// skipConn returns driver.ErrSkip for statements with arguments, like go-sql-driver/mysql without
// interpolateParams.
type skipConn struct{ basicConn }

func (c *skipConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	return driver.RowsAffected(0), nil
}

// convertingConn prepares statements with a ColumnConverter.
type convertingConn struct{ basicConn }

func (c *convertingConn) Prepare(string) (driver.Stmt, error) {
	return &convertingBasicStmt{basicStmt{conn: &c.basicConn}}, nil
}

type connDriver struct{ open func() driver.Conn }

func (d connDriver) Open(string) (driver.Conn, error) { return d.open(), nil }

// openFake opens a logged database on cn, whose lines are returned by lines.
func openFake(t *testing.T, cn driver.Conn, cfg LoggerConfig) (*sql.DB, context.Context, func() []map[string]any) {
	t.Helper()
	db := sql.OpenDB(&connector{
		Connector: dsnConnector{driver: connDriver{func() driver.Conn { return cn }}},
		log:       newQueryLog("main", cfg.Sanitize()),
	})
	t.Cleanup(func() { _ = db.Close() })
	ctx, lines := captureLog(t)
	return db, ctx, lines
}

// captureLog returns a context whose logger writes to a buffer, and the lines written so far.
func captureLog(t *testing.T) (context.Context, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var buf bytes.Buffer
	lg := zerolog.New(zerolog.SyncWriter(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	})))
	return logger.NewCtx(context.Background(), lg), func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		var lines []map[string]any
		for line := range bytes.Lines(buf.Bytes()) {
			var m map[string]any
			if err := json.Unmarshal(line, &m); err != nil {
				t.Fatalf("log line %q: %v", line, err)
			}
			lines = append(lines, m)
		}
		return lines
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestWrapperInterfaces(t *testing.T) {
	log := newQueryLog("main", LoggerConfig{}.Sanitize())
	for _, tc := range []struct {
		name              string
		conn              driver.Conn
		resets, validates bool
	}{
		{"basic", &basicConn{}, false, false},
		{"resetter", &resetConn{}, true, false},
		{"validator", &validConn{}, false, true},
		{"both", &fullConn{}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := wrapConn(tc.conn, log)
			if _, ok := wrapped.(driver.SessionResetter); ok != tc.resets {
				t.Errorf("SessionResetter = %v, want %v", ok, tc.resets)
			}
			if _, ok := wrapped.(driver.Validator); ok != tc.validates {
				t.Errorf("Validator = %v, want %v", ok, tc.validates)
			}
			for _, ok := range []bool{
				implements[driver.ExecerContext](wrapped), implements[driver.QueryerContext](wrapped),
				implements[driver.ConnPrepareContext](wrapped), implements[driver.ConnBeginTx](wrapped),
				implements[driver.Pinger](wrapped), implements[driver.NamedValueChecker](wrapped),
			} {
				if !ok {
					t.Fatal("wrapper lacks an interface it always implements")
				}
			}
		})
	}

	ctx := context.Background()
	si, err := wrapConn(&basicConn{}, log).(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	//lint:ignore SA1019 the wrapper must keep the deprecated interface of drivers that have it
	if implements[driver.ColumnConverter](si) {
		t.Error("statement without ColumnConverter gained one")
	}
	si, err = wrapConn(&convertingConn{}, log).(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	//lint:ignore SA1019 the wrapper must keep the deprecated interface of drivers that have it
	if !implements[driver.ColumnConverter](si) {
		t.Error("statement lost its ColumnConverter")
	}

	if implements[driver.RowsColumnScanner](wrapRows(ctx, log, &basicRows{}, "", nil, time.Time{})) {
		t.Error("rows without RowsColumnScanner gained one")
	}
	if !implements[driver.RowsColumnScanner](wrapRows(ctx, log, &scanRows{}, "", nil, time.Time{})) {
		t.Error("rows lost their RowsColumnScanner")
	}
}

func TestDriverConn(t *testing.T) {
	for _, tc := range []struct {
		name string
		conn driver.Conn
	}{
		{"basic", &basicConn{}},
		{"resetter", &resetConn{}},
		{"validator", &validConn{}},
		{"both", &fullConn{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logged, ctx, _ := openFake(t, tc.conn, LoggerConfig{})
			plain := sql.OpenDB(dsnConnector{driver: connDriver{func() driver.Conn { return tc.conn }}})
			t.Cleanup(func() { _ = plain.Close() })

			for _, db := range []*sql.DB{logged, plain} {
				c, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				err = c.Raw(func(driverConn any) error {
					if got := DriverConn(driverConn); got != tc.conn {
						t.Errorf("DriverConn(%T) = %T, want the driver's %T", driverConn, got, tc.conn)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				_ = c.Close()
			}
		})
	}
}

func implements[I any](v any) bool {
	_, ok := v.(I)
	return ok
}

func TestBasicDriver(t *testing.T) {
	cn := &basicConn{}
	db, ctx, lines := openFake(t, cn, LoggerConfig{Enabled: true, Args: true})

	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := db.ExecContext(ctx, `UPDATE t SET n = 1 WHERE id = ?`, 7)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 4 {
		t.Fatalf("RowsAffected = %d", n)
	}
	rows, err := db.QueryContext(ctx, `SELECT n FROM t WHERE id = ?`, 8)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if types[0].ScanType() != reflect.TypeFor[any]() || types[0].DatabaseTypeName() != "" {
		t.Fatalf("column type = %v %q, want database/sql's defaults", types[0].ScanType(), types[0].DatabaseTypeName())
	}
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil || n != 2 {
		t.Fatalf("read %d rows, err %v", n, err)
	}
	if rows.NextResultSet() {
		t.Fatal("NextResultSet = true without result sets")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}); err == nil || !strings.Contains(err.Error(), "isolation") {
		t.Fatalf("BeginTx with an isolation level err = %v", err)
	}
	if _, err := db.ExecContext(ctx, `SYNTAX`); err == nil {
		t.Fatal("Exec of a statement that fails to prepare succeeded")
	}

	if got := cn.callList(); got != "prepare,exec,prepare,query,begin,commit,prepare" {
		t.Fatalf("driver calls = %s", got)
	}
	want := []struct {
		query, level string
		args         string
	}{
		{`UPDATE t SET n = 1 WHERE id = ?`, "info", "[7]"},
		{`SELECT n FROM t WHERE id = ?`, "info", "[8]"},
		{"BEGIN", "info", ""},
		{"COMMIT", "info", ""},
		{"BEGIN", "error", ""},
		{`SYNTAX`, "error", ""},
	}
	got := lines()
	if len(got) != len(want) {
		t.Fatalf("logged %d lines, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i][logger.SQLQueryLogKey] != w.query || got[i]["level"] != w.level || argsOf(got[i]) != w.args {
			t.Errorf("line %d = %v, want %s at %s with args %s", i, got[i], w.query, w.level, w.args)
		}
	}
	if got[0][logger.SQLRowsAffectedLogKey] != 4.0 || got[1][logger.SQLRowsReturnedLogKey] != 2.0 {
		t.Errorf("row counts: %v, %v", got[0], got[1])
	}

	if _, err := db.ExecContext(ctx, `UPDATE t SET n = 1 WHERE id = ?`, sql.Named("id", 1)); err == nil ||
		!strings.Contains(err.Error(), "Named Parameters") {
		t.Fatalf("named argument on a driver without NamedValue support err = %v", err)
	}
}

func argsOf(line map[string]any) string {
	args, ok := line[logger.SQLArgsLogKey]
	if !ok {
		return ""
	}
	b, _ := json.Marshal(args)
	return string(b)
}

func TestFullDriver(t *testing.T) {
	cn := &fullConn{}
	db, ctx, lines := openFake(t, cn, LoggerConfig{Enabled: true, Args: true})
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE t SET p = $1`, point{1, 2}); err != nil {
		t.Fatal(err)
	}
	if cn.got != "point" {
		t.Fatalf("driver got %#v, want the value of its NamedValueChecker", cn.got)
	}
	var n int64
	if err := db.QueryRowContext(ctx, `SELECT n FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	calls := cn.callList()
	for _, call := range []string{"ping", "execContext", "queryContext", "beginTx", "rollback", "reset", "valid"} {
		if !strings.Contains(calls, call) {
			t.Errorf("driver calls %s lack %s", calls, call)
		}
	}
	if strings.Contains(calls, "prepare") {
		t.Errorf("driver calls %s prepare statements the driver runs itself", calls)
	}
	got := lines()
	if len(got) != 4 || got[0][logger.SQLRowsAffectedLogKey] != 2.0 || argsOf(got[0]) != `["point"]` ||
		got[1][logger.SQLRowsReturnedLogKey] != 1.0 || got[3][logger.SQLQueryLogKey] != "ROLLBACK" {
		t.Fatalf("lines = %v", got)
	}
}

func TestScanningRows(t *testing.T) {
	cn := &fullConn{scan: true}
	db, ctx, lines := openFake(t, cn, LoggerConfig{Enabled: true})
	var n int64 = -1
	if err := db.QueryRowContext(ctx, `SELECT n FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("scanned %d, want 0 from ScanColumn", n)
	}
	if got := lines(); len(got) != 1 || got[0][logger.SQLRowsReturnedLogKey] != 1.0 {
		t.Fatalf("lines = %v", got)
	}
}

func TestSkippedStatementLogsOnce(t *testing.T) {
	cn := &skipConn{}
	db, ctx, lines := openFake(t, cn, LoggerConfig{Enabled: true})
	if _, err := db.ExecContext(ctx, `UPDATE t SET n = ?`, 1); err != nil {
		t.Fatal(err)
	}
	if got := lines(); len(got) != 1 || got[0]["level"] != "info" {
		t.Fatalf("lines = %v, want the prepared statement only", got)
	}
}

func TestColumnConverterKept(t *testing.T) {
	cn := &convertingConn{}
	db, ctx, _ := openFake(t, cn, LoggerConfig{Enabled: true})
	if _, err := db.ExecContext(ctx, `UPDATE t SET n = ?`, 1); err != nil {
		t.Fatal(err)
	}
	if cn.got != "converted" {
		t.Fatalf("driver got %#v, want the value of the statement's ColumnConverter", cn.got)
	}
}

func TestBadConnLogsWarning(t *testing.T) {
	cn := &fullConn{execErr: driver.ErrBadConn}
	db, ctx, lines := openFake(t, cn, LoggerConfig{Enabled: true})
	if _, err := db.ExecContext(ctx, `UPDATE t SET n = 1`); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v", err)
	}
	got := lines()
	if len(got) == 0 {
		t.Fatal("nothing logged")
	}
	for _, line := range got {
		if line["level"] != "warn" || line[logger.ErrorSeverityLogKey] != 0.0 {
			t.Fatalf("bad connection logged %v, want a light warning", line)
		}
	}
}
