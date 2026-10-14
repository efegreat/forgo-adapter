// Package sqldb is a database/sql resource: a *sql.DB with pool limits, opened and verified when the
// application starts and closed when it stops.
package sqldb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/efegreat/forgo"
)

// Config is a database/sql resource. Its Value is the *sql.DB providers request.
// Import the driver for DriverName in main.
type Config struct {
	// Name identifies the resource and must be unique in the bootstrap. `boot:"name"` tags select it
	// by this name when the bootstrap has several databases. Required.
	Name string
	// DriverName is the registered database/sql driver, such as "postgres" or "mysql".
	DriverName string
	// DSN is the data source name passed to the driver.
	DSN string
	// MaxOpenConns limits open connections. Zero means unlimited.
	MaxOpenConns int
	// MaxIdleConns limits idle connections. Default: 5.
	MaxIdleConns int
	// ConnMaxLifetime closes connections older than this. Default: 60 minutes.
	ConnMaxLifetime time.Duration
	// ConnMaxIdleTime closes connections idle for longer than this. Default: 10 minutes.
	ConnMaxIdleTime time.Duration
	// ConnectTimeout bounds the ping New sends to verify the database is reachable. Default: 5 seconds.
	ConnectTimeout time.Duration
}

// Sanitize returns a copy of the config with defaults applied to its zero fields.
func (s Config) Sanitize() Config {
	if s.ConnectTimeout == 0 {
		s.ConnectTimeout = time.Second * 5
	}
	if s.MaxIdleConns == 0 {
		s.MaxIdleConns = 5
	}
	if s.ConnMaxLifetime == 0 {
		s.ConnMaxLifetime = time.Minute * 60
	}
	if s.ConnMaxIdleTime == 0 {
		s.ConnMaxIdleTime = time.Minute * 10
	}
	return s
}

// New opens the database and pings it within ConnectTimeout, so that an unreachable database or a
// wrong DSN stops the application before it serves.
func (s Config) New() (forgo.Resource, error) {
	cfg := s.Sanitize()

	db, err := sql.Open(cfg.DriverName, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("sqldb: open %s: %w", cfg.Name, err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		return nil, errors.Join(fmt.Errorf("sqldb: ping %s: %w", cfg.Name, err), db.Close())
	}

	return &resource{
		name: cfg.Name,
		db:   db,
	}, nil
}

// resource is the forgo.Resource New returns. Its Value is the *sql.DB.
type resource struct {
	name string
	db   *sql.DB
}

// Name returns Config.Name.
func (s *resource) Name() string {
	return s.name
}

// Value returns the *sql.DB.
func (s *resource) Value() any {
	return s.db
}

// Close closes the *sql.DB.
func (s *resource) Close() error {
	return s.db.Close()
}
