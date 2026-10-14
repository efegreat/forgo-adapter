// Package sqltest runs the integration tests of the SQL storages against real databases. A test opens
// the database of each dialect whose DSN is set in the environment and skips the others, so that
// go test ./... needs no infrastructure:
//
//	ADAPTER_TEST_POSTGRES_DSN='postgres://postgres:forgo@127.0.0.1:5432/postgres?sslmode=disable' \
//	ADAPTER_TEST_MYSQL_DSN='root:forgo@tcp(127.0.0.1:3306)/forgo' go test ./provider/...
//
// Use disposable databases: the tests drop and create the tables of the storages they test.
package sqltest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Dialect is a database the integration tests run against.
type Dialect struct {
	// Name is the database/sql driver name.
	Name string
	env  string
}

var (
	Postgres = Dialect{Name: "postgres", env: "ADAPTER_TEST_POSTGRES_DSN"}
	MySQL    = Dialect{Name: "mysql", env: "ADAPTER_TEST_MYSQL_DSN"}

	// Dialects are the dialects every SQL storage supports.
	Dialects = []Dialect{Postgres, MySQL}
)

// sessionTimeZone is the MySQL session time zone of the tests. It is not UTC, so that a timestamp
// that follows the session time zone instead of UTC fails them.
const sessionTimeZone = "'-05:00'"

// Open opens the database of d and checks that it answers, or skips t when its DSN is not set. The
// database is closed when t ends.
//
// MySQL connections get parseTime, the driver's default loc (UTC) and a session time zone other
// than UTC.
func (d Dialect) Open(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv(d.env)
	if dsn == "" {
		t.Skip("set " + d.env + " to run against a disposable " + d.Name + " database")
	}
	if d == MySQL {
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			t.Fatalf("parse %s: %v", d.env, err)
		}
		cfg.ParseTime = true
		cfg.Loc = time.UTC
		if cfg.Params == nil {
			cfg.Params = map[string]string{}
		}
		cfg.Params["time_zone"] = sessionTimeZone
		dsn = cfg.FormatDSN()
	}

	db, err := sql.Open(d.Name, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping %s: %v", d.Name, err)
	}
	return db
}

// Reset drops table, one of inbox_events, outbox_events and live_configs, and creates it again
// empty, with the schema forgo's REFERENCE.md documents for d.
func (d Dialect) Reset(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	ddl, ok := schemas[d.Name][table]
	if !ok {
		t.Fatalf("no %s schema for %s", d.Name, table)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, stmt := range append([]string{"DROP TABLE IF EXISTS " + table}, ddl...) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

// Placeholder returns the nth parameter placeholder of d, counted from 1.
func (d Dialect) Placeholder(n int) string {
	if d == Postgres {
		return "$" + strconv.Itoa(n)
	}
	return "?"
}

// Ago returns an SQL expression for the time age before now on the database clock, in the time zone
// the storages of d stamp rows in.
func (d Dialect) Ago(age time.Duration) string {
	if d == Postgres {
		return fmt.Sprintf("CURRENT_TIMESTAMP - make_interval(secs => %d)", int64(age.Seconds()))
	}
	return fmt.Sprintf("UTC_TIMESTAMP(6) - INTERVAL %d MICROSECOND", age.Microseconds())
}

// schemas are the tables of forgo's REFERENCE.md ("Storage contracts" and "Live configs"), by
// dialect and table. Keep them identical.
var schemas = map[string]map[string][]string{
	"postgres": {
		"outbox_events": {`
CREATE TABLE outbox_events (
    id            TEXT PRIMARY KEY,
    name          TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    headers       JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    message_key   TEXT        NOT NULL DEFAULT '',
    destination   TEXT        NOT NULL DEFAULT '',
    processing_id TEXT,
    processing_at TIMESTAMPTZ,
    processed_at  TIMESTAMPTZ
)`,
			`CREATE INDEX outbox_events_pending ON outbox_events (id) WHERE processed_at IS NULL`,
			`CREATE INDEX outbox_events_claim ON outbox_events (processing_id) WHERE processing_id IS NOT NULL`,
			`CREATE INDEX outbox_events_processed ON outbox_events (processed_at, id) WHERE processed_at IS NOT NULL`,
		},
		"inbox_events": {`
CREATE TABLE inbox_events (
    message_id      TEXT        NOT NULL,
    subscriber_name TEXT        NOT NULL,
    status          SMALLINT    NOT NULL,
    attempts        INT         NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (message_id, subscriber_name)
)`,
			`CREATE INDEX inbox_events_cleanup ON inbox_events (status, updated_at)`,
		},
		"live_configs": {`
CREATE TABLE live_configs (
    name       TEXT        PRIMARY KEY,
    data       JSONB       NOT NULL,
    version    BIGINT      NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
)`},
	},
	"mysql": {
		"outbox_events": {`
CREATE TABLE outbox_events (
    id            VARCHAR(255) NOT NULL PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    payload       JSON         NOT NULL,
    headers       JSON,
    created_at    DATETIME(6)  NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    message_key   VARCHAR(255) NOT NULL DEFAULT '',
    destination   VARCHAR(255) NOT NULL DEFAULT '',
    processing_id VARCHAR(255),
    processing_at DATETIME(6),
    processed_at  DATETIME(6),
    INDEX outbox_events_processed (processed_at, id),
    INDEX outbox_events_claim (processing_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin`},
		"inbox_events": {`
CREATE TABLE inbox_events (
    message_id      VARCHAR(255) NOT NULL,
    subscriber_name VARCHAR(255) NOT NULL,
    status          TINYINT      NOT NULL,
    attempts        INT          NOT NULL,
    created_at      DATETIME(6)  NOT NULL,
    updated_at      DATETIME(6)  NOT NULL,
    PRIMARY KEY (message_id, subscriber_name),
    INDEX inbox_events_cleanup (status, updated_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin`},
		"live_configs": {`
CREATE TABLE live_configs (
    name       VARCHAR(255)    NOT NULL PRIMARY KEY,
    data       JSON            NOT NULL,
    version    BIGINT UNSIGNED NOT NULL,
    updated_at DATETIME(6)     NOT NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin`},
	},
}
