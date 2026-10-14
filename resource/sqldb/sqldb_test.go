package sqldb

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSanitize(t *testing.T) {
	got := Config{}.Sanitize()
	want := Config{
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 10 * time.Minute,
		ConnectTimeout:  5 * time.Second,
		Logger:          LoggerConfig{SlowThreshold: 200 * time.Millisecond},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Sanitize() = %+v, want %+v", got, want)
	}

	custom := Config{
		MaxOpenConns:    20,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Minute,
		ConnMaxIdleTime: time.Second,
		ConnectTimeout:  time.Second,
		Logger:          LoggerConfig{Enabled: true, SlowThreshold: time.Second, SlowOnly: true, Args: true, SensitiveFields: []string{"token"}},
	}
	if got := custom.Sanitize(); !reflect.DeepEqual(got, custom) {
		t.Fatalf("Sanitize() overrode set fields: %+v", got)
	}
}

func TestNewRejectsNegativeSlowThreshold(t *testing.T) {
	_, err := Config{Name: "main", DriverName: "sqlmock", DSN: "unused", Logger: LoggerConfig{Enabled: true, SlowThreshold: -time.Second}}.New()
	if err == nil || !strings.Contains(err.Error(), "SlowThreshold") {
		t.Fatalf("New err = %v", err)
	}
}

func TestNew(t *testing.T) {
	_, mock, err := sqlmock.NewWithDSN("sqldb_up", sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectPing()

	res, err := Config{Name: "main", DriverName: "sqlmock", DSN: "sqldb_up", MaxOpenConns: 7}.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	db, ok := res.Value().(*sql.DB)
	if !ok {
		t.Fatalf("Value() = %T, want *sql.DB", res.Value())
	}
	if res.Name() != "main" {
		t.Fatalf("Name() = %q", res.Name())
	}
	if got := db.Stats().MaxOpenConnections; got != 7 {
		t.Fatalf("MaxOpenConnections = %d, want 7", got)
	}

	mock.ExpectClose()
	if err := res.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewPingFailureClosesDB(t *testing.T) {
	_, mock, err := sqlmock.NewWithDSN("sqldb_down", sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectPing().WillReturnError(errors.New("connection refused"))
	mock.ExpectClose()

	_, err = Config{Name: "main", DriverName: "sqlmock", DSN: "sqldb_down"}.New()
	if err == nil || !strings.Contains(err.Error(), "sqldb: ping main: connection refused") {
		t.Fatalf("New err = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("database not closed after the failed ping: %v", err)
	}
}
