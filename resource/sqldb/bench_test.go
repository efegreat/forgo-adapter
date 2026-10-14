package sqldb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/rs/zerolog"
)

const benchQuery = `SELECT id, name FROM users WHERE email = $1 AND password_hash = $2 AND created_at > $3 LIMIT $4`

func benchArgs() []driver.NamedValue {
	return []driver.NamedValue{
		{Ordinal: 1, Value: "ada@example.com"},
		{Ordinal: 2, Value: "$2a$10$N9qo8uLOickgx2ZMRZoMye"},
		{Ordinal: 3, Value: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)},
		{Ordinal: 4, Value: int64(20)},
	}
}

// TestRecordDoesNotAllocate checks that a logged line allocates nothing once the plan of its
// statement is cached, arguments and masking included.
func TestRecordDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops items under the race detector")
	}
	ctx := logger.NewCtx(context.Background(), zerolog.New(io.Discard))
	args := benchArgs()
	jsonArgs := []driver.NamedValue{{Ordinal: 1, Value: "signup"}, {Ordinal: 2, Value: `{"user":{"name":"ada","password_hash":"x"}}`}}
	for _, cfg := range []LoggerConfig{
		{},
		{Args: true},
		{Args: true, SensitiveFields: []string{"password_hash"}},
	} {
		l := newQueryLog("main", cfg.Sanitize())
		for _, s := range []statement{
			{query: benchQuery, args: args, rows: 3},
			{query: `INSERT INTO events (name, payload) VALUES ($1, $2)`, args: jsonArgs, result: driver.RowsAffected(1), rows: -1},
		} {
			s.start = time.Now()
			l.record(ctx, s) // caches the plan
			if allocs := testing.AllocsPerRun(100, func() { l.record(ctx, s) }); allocs != 0 {
				t.Errorf("record(%q) with %+v allocated %v times", s.query, cfg, allocs)
			}
		}
	}
}

// BenchmarkExec runs a statement through database/sql on a driver that answers at once, so the
// difference between cases is the cost of the log.
func BenchmarkExec(b *testing.B) {
	args := []any{"ada@example.com", "$2a$10$N9qo8uLOickgx2ZMRZoMye", time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC), 20}
	for _, bc := range []struct {
		name string
		cfg  LoggerConfig
	}{
		{"off", LoggerConfig{}},
		{"slow only", LoggerConfig{Enabled: true, SlowOnly: true}},
		{"info", LoggerConfig{Enabled: true}},
		{"info args", LoggerConfig{Enabled: true, Args: true}},
		{"info masked args", LoggerConfig{Enabled: true, Args: true, SensitiveFields: []string{"password_hash", "token"}}},
	} {
		b.Run(bc.name, func(b *testing.B) {
			cn := &fullConn{}
			var db *sql.DB
			if bc.cfg.Enabled {
				db = sql.OpenDB(&connector{
					Connector: dsnConnector{driver: connDriver{func() driver.Conn { return cn }}},
					log:       newQueryLog("main", bc.cfg.Sanitize()),
				})
			} else {
				db = sql.OpenDB(dsnConnector{driver: connDriver{func() driver.Conn { return cn }}})
			}
			defer db.Close()
			ctx := logger.NewCtx(context.Background(), zerolog.New(io.Discard))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := db.ExecContext(ctx, benchQuery, args...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkParamPlan builds the plan of a statement, which happens once per statement text.
func BenchmarkParamPlan(b *testing.B) {
	l := newQueryLog("main", LoggerConfig{SensitiveFields: []string{"password_hash"}}.Sanitize())
	b.ReportAllocs()
	for b.Loop() {
		newParamPlan(benchQuery, l.fields)
	}
}
