package sqldb_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/efegreat/forgo/logger"
	"github.com/rs/zerolog"

	"github.com/efegreat/forgo-adapter/internal/sqllog"
	"github.com/efegreat/forgo-adapter/provider/sqltx"
	"github.com/efegreat/forgo-adapter/resource/sqldb"
)

// openLogged opens a sqldb resource on sqlmock, and returns it with a context whose logger writes
// the lines that lines returns.
func openLogged(t *testing.T, cfg sqldb.LoggerConfig) (*sql.DB, sqlmock.Sqlmock, context.Context, func() []map[string]any) {
	t.Helper()
	dsn := "log_" + t.Name()
	_, mock, err := sqlmock.NewWithDSN(dsn, sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	res, err := sqldb.Config{Name: "main", DriverName: "sqlmock", DSN: dsn, Logger: cfg}.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Close() })

	var buf bytes.Buffer
	ctx := logger.NewCtx(context.Background(), zerolog.New(&buf))
	return res.Value().(*sql.DB), mock, ctx, func() []map[string]any {
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

func onlyLine(t *testing.T, lines []map[string]any) map[string]any {
	t.Helper()
	if len(lines) != 1 {
		t.Fatalf("logged %d lines, want 1: %v", len(lines), lines)
	}
	return lines[0]
}

func TestLogExec(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true})
	mock.ExpectExec(`UPDATE users SET name = $1`).WithArgs("ada").WillReturnResult(sqlmock.NewResult(0, 3))

	if _, err := db.ExecContext(ctx, `UPDATE users SET name = $1`, "ada"); err != nil {
		t.Fatal(err)
	}
	line := onlyLine(t, lines())
	for key, want := range map[string]any{
		"level":                      "info",
		"message":                    "sql query completed",
		logger.SQLDatabaseLogKey:     "main",
		logger.SQLQueryLogKey:        `UPDATE users SET name = $1`,
		logger.SQLRowsAffectedLogKey: 3.0,
	} {
		if line[key] != want {
			t.Errorf("%s = %v, want %v", key, line[key], want)
		}
	}
	if _, ok := line[logger.ElapsedTimeLogKey].(float64); !ok {
		t.Errorf("no elapsed time in %v", line)
	}
	for _, key := range []string{logger.SQLArgsLogKey, logger.SQLCallerLogKey, logger.SQLRowsReturnedLogKey, logger.ErrorSeverityLogKey} {
		if _, ok := line[key]; ok {
			t.Errorf("%s logged in %v", key, line)
		}
	}
}

func TestLogQueryAtClose(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true})
	mock.ExpectQuery(`SELECT n FROM t`).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1).AddRow(2).AddRow(3))

	rows, err := db.QueryContext(ctx, `SELECT n FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()
	if len(lines()) != 0 {
		t.Fatal("query logged before its rows closed")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if line := onlyLine(t, lines()); line[logger.SQLRowsReturnedLogKey] != 1.0 {
		t.Fatalf("%s = %v, want the 1 row read", logger.SQLRowsReturnedLogKey, line[logger.SQLRowsReturnedLogKey])
	}
}

func TestLogFailure(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true})
	mock.ExpectExec(`DELETE FROM t`).WillReturnError(errors.New("relation t does not exist"))

	_, err := db.ExecContext(ctx, `DELETE FROM t`)
	if err == nil {
		t.Fatal("no error")
	}
	line := onlyLine(t, lines())
	if line["level"] != "error" || line["message"] != "sql query failed" || line[logger.ErrorSeverityLogKey] != 2.0 ||
		line["error"] != "relation t does not exist" {
		t.Fatalf("line = %v", line)
	}
	if caller, _ := line[logger.SQLCallerLogKey].(string); !strings.Contains(caller, "log_test.go:") {
		t.Fatalf("%s = %q, want this test", logger.SQLCallerLogKey, caller)
	}
}

// TestLogCallerSkipsSqltx checks that a statement sqltx runs names the code that called sqltx.
func TestLogCallerSkipsSqltx(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true})
	mock.ExpectBegin().WillReturnError(errors.New("too many connections"))

	if err := sqltx.Run(ctx, db, func(context.Context) error { return nil }); err == nil {
		t.Fatal("no error")
	}
	line := onlyLine(t, lines())
	if caller, _ := line[logger.SQLCallerLogKey].(string); line[logger.SQLQueryLogKey] != "BEGIN" || !strings.Contains(caller, "log_test.go:") {
		t.Fatalf("line = %v, want BEGIN called from this test", line)
	}
}

func TestLogFailedRows(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true})
	mock.ExpectQuery(`SELECT n FROM t`).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1).AddRow(2).RowError(1, errors.New("lost")))

	rows, err := db.QueryContext(ctx, `SELECT n FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err == nil {
		t.Fatal("no error reading rows")
	}
	line := onlyLine(t, lines())
	if line["level"] != "error" || line["error"] != "lost" || line[logger.SQLRowsReturnedLogKey] != 1.0 {
		t.Fatalf("line = %v", line)
	}
}

func TestLogSlow(t *testing.T) {
	for _, slowOnly := range []bool{false, true} {
		t.Run("slowOnly="+map[bool]string{false: "false", true: "true"}[slowOnly], func(t *testing.T) {
			db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true, SlowThreshold: 20 * time.Millisecond, SlowOnly: slowOnly})
			mock.ExpectExec(`UPDATE fast`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(`UPDATE slow`).WillDelayFor(30 * time.Millisecond).WillReturnResult(sqlmock.NewResult(0, 1))

			for _, query := range []string{`UPDATE fast`, `UPDATE slow`} {
				if _, err := db.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			got := lines()
			if slowOnly && len(got) != 1 || !slowOnly && len(got) != 2 {
				t.Fatalf("lines = %v", got)
			}
			slow := got[len(got)-1]
			if slow["level"] != "warn" || slow["message"] != "slow sql query" || slow[logger.SQLQueryLogKey] != `UPDATE slow` {
				t.Fatalf("slow line = %v", slow)
			}
			if elapsed, _ := slow[logger.ElapsedTimeLogKey].(float64); elapsed < 0.02 {
				t.Fatalf("elapsed = %v", elapsed)
			}
			if _, ok := slow[logger.SQLCallerLogKey]; !ok {
				t.Fatalf("no caller in %v", slow)
			}
		})
	}
}

func TestLogCanceled(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true, SlowThreshold: time.Minute})
	mock.ExpectExec(`UPDATE t`).WillDelayFor(time.Second).WillReturnResult(sqlmock.NewResult(0, 1))

	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := db.ExecContext(ctx, `UPDATE t`); err == nil {
		t.Fatal("no error")
	}
	if line := onlyLine(t, lines()); line["level"] != "warn" || line[logger.ErrorSeverityLogKey] != 0.0 {
		t.Fatalf("canceled statement logged %v, want a light warning", line)
	}
}

func TestLogQuiet(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true})
	mock.ExpectExec(`UPDATE outbox_events`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE broken`).WillReturnError(errors.New("boom"))

	ctx = sqllog.Quiet(ctx)
	_, _ = db.ExecContext(ctx, `UPDATE outbox_events`)
	_, _ = db.ExecContext(ctx, `UPDATE broken`)
	got := lines()
	if len(got) != 2 || got[0]["level"] != "debug" || got[1]["level"] != "error" {
		t.Fatalf("lines = %v", got)
	}
}

func TestLogTransaction(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true})
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE t`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE t`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var queries []string
	for _, line := range lines() {
		queries = append(queries, line[logger.SQLQueryLogKey].(string))
	}
	if got := strings.Join(queries, ","); got != "BEGIN,UPDATE t,COMMIT,BEGIN,ROLLBACK" {
		t.Fatalf("logged %s", got)
	}
}

func TestLogArgs(t *testing.T) {
	when := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		fields []string
		query  string
		args   []any
		want   string
		cut    bool
	}{
		{
			name:  "values",
			query: `SELECT $1, $2, $3, $4, $5, $6, $7`,
			args:  []any{nil, 42, 1.5, true, when, "text", []byte("bytes")},
			want:  `[null,42,1.5,true,"2026-10-14T12:00:00.000000Z","text","bytes"]`,
		},
		{
			name:  "json without fields",
			query: `INSERT INTO t (data) VALUES ($1)`,
			args:  []any{`{"password": "x"}`},
			want:  `["{\"password\": \"x\"}"]`,
		},
		{
			name:   "masked",
			fields: []string{"password"},
			query:  `SELECT * FROM users WHERE email = $1 AND password = $2 LIMIT $3`,
			args:   []any{"ada@example.com", "hunter2", 20},
			want:   `["ada@example.com","[*]",20]`,
		},
		{
			name:   "unnamed",
			fields: []string{"password"},
			query:  `SELECT COALESCE($1, name) FROM t WHERE id = $2`,
			args:   []any{"x", 1},
			want:   `["[*]",1]`,
		},
		{
			name:   "named",
			fields: []string{"password"},
			query:  `SELECT * FROM t WHERE a = $1 AND b = $2`,
			args:   []any{sql.Named("password", "x"), sql.Named("email", "y")},
			want:   `["[*]","y"]`,
		},
		{
			name:   "json members",
			fields: []string{"password"},
			query:  `INSERT INTO events (name, payload) VALUES ($1, $2)`,
			args:   []any{"signup", "{\n  \"user\": {\"name\": \"ada\", \"Password\": \"x\"}\n}"},
			want:   `["signup",{"user":{"name":"ada","Password":"[*]"}}]`,
		},
		{
			name:   "json bytes",
			fields: []string{"password"},
			query:  `INSERT INTO events (payload) VALUES ($1)`,
			args:   []any{[]byte(`[{"password":"x"}]`)},
			want:   `[[{"password":"[*]"}]]`,
		},
		{
			name:   "invalid json",
			fields: []string{"password"},
			query:  `INSERT INTO events (payload) VALUES ($1)`,
			args:   []any{`{"a": 1, "password": "x" oops}`},
			want:   `["{\"a\":1,\"password\":\"[*]\""]`,
			cut:    true,
		},
		{
			name:  "long",
			query: `INSERT INTO t (data) VALUES ($1)`,
			args:  []any{strings.Repeat("é", 600)},
			want:  `["` + strings.Repeat("é", 512) + `"]`,
			cut:   true,
		},
		{
			name:   "long json",
			fields: []string{"password"},
			query:  `INSERT INTO t (data) VALUES ($1)`,
			// The cut falls inside the password, which is masked whole.
			args: []any{`{"a":"` + strings.Repeat("x", 1000) + `","password":"secretsecretsecret"}`},
			want: `["{\"a\":\"` + strings.Repeat("x", 1000) + `\",\"password\":\"[*]\""]`,
			cut:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true, Args: true, SensitiveFields: tc.fields})
			mock.ExpectExec(tc.query).WillReturnResult(sqlmock.NewResult(0, 1))
			if _, err := db.ExecContext(ctx, tc.query, tc.args...); err != nil {
				t.Fatal(err)
			}
			line := onlyLine(t, lines())
			var want any
			if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
				t.Fatal(err)
			}
			if got := line[logger.SQLArgsLogKey]; !reflect.DeepEqual(got, want) {
				t.Fatalf("%s = %v\nwant %v", logger.SQLArgsLogKey, got, want)
			}
			if cut, _ := line[logger.SQLArgsCutLogKey].(bool); cut != tc.cut {
				t.Fatalf("%s = %v, want %v", logger.SQLArgsCutLogKey, cut, tc.cut)
			}
		})
	}
}

func TestLogManyArgs(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true, Args: true})
	placeholders := make([]string, 150)
	args := make([]any, 150)
	for i := range args {
		placeholders[i], args[i] = "?", i
	}
	query := `INSERT INTO t (n) VALUES (` + strings.Join(placeholders, "), (") + `)`
	mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 150))
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
	line := onlyLine(t, lines())
	if logged, _ := line[logger.SQLArgsLogKey].([]any); len(logged) != 100 || line[logger.SQLArgsCutLogKey] != true {
		t.Fatalf("logged %d arguments, cut %v", len(logged), line[logger.SQLArgsCutLogKey])
	}
}

func TestLogLongQuery(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{Enabled: true})
	query := `SELECT ` + strings.Repeat("1 + ", 10<<10) + `1`
	mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
	line := onlyLine(t, lines())
	if logged, _ := line[logger.SQLQueryLogKey].(string); len(logged) != 32<<10 || line[logger.SQLQueryCutLogKey] != true {
		t.Fatalf("logged %d bytes of the query, cut %v", len(logged), line[logger.SQLQueryCutLogKey])
	}
}

func TestLogDisabled(t *testing.T) {
	db, mock, ctx, lines := openLogged(t, sqldb.LoggerConfig{})
	mock.ExpectExec(`UPDATE t`).WillReturnResult(sqlmock.NewResult(0, 1))
	if _, err := db.ExecContext(ctx, `UPDATE t`); err != nil {
		t.Fatal(err)
	}
	if got := lines(); len(got) != 0 {
		t.Fatalf("disabled logger logged %v", got)
	}
}

// TestLogKeepsDriver checks that the database reports the driver's own type, which forgo's
// migration.DetectDialect reads, with or without the log.
func TestLogKeepsDriver(t *testing.T) {
	_, _, err := sqlmock.NewWithDSN("log_driver")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := sql.Open("sqlmock", "log_driver")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	for _, enabled := range []bool{false, true} {
		res, err := sqldb.Config{Name: "main", DriverName: "sqlmock", DSN: "log_driver", Logger: sqldb.LoggerConfig{Enabled: enabled}}.New()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := reflect.TypeOf(res.Value().(*sql.DB).Driver()), reflect.TypeOf(plain.Driver()); got != want {
			t.Errorf("enabled %v: Driver() = %v, want %v", enabled, got, want)
		}
		_ = res.Close()
	}
}

func TestLogLevelFiltered(t *testing.T) {
	db, mock, _, _ := openLogged(t, sqldb.LoggerConfig{Enabled: true, Args: true})
	mock.ExpectExec(`UPDATE t SET a = $1`).WillReturnResult(sqlmock.NewResult(0, 1))
	var buf bytes.Buffer
	ctx := logger.NewCtx(context.Background(), zerolog.New(&buf).Level(zerolog.WarnLevel))
	if _, err := db.ExecContext(ctx, `UPDATE t SET a = $1`, 1); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("logged %s above the logger's level", buf.Bytes())
	}
}
