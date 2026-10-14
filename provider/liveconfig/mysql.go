package liveconfig

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/efegreat/forgo"

	"github.com/efegreat/forgo-adapter/internal/sqllog"
)

// MySQL is a forgo.LiveConfigStore on the live_configs table of a MySQL 8 database.
type MySQL struct {
	db *sql.DB
}

// NewMySQL returns a live config store using db, for forgo.WithProviders.
func NewMySQL(db *sql.DB) *MySQL {
	return &MySQL{db: db}
}

// Get reads the row of name. Without one, it returns version 0.
func (m *MySQL) Get(ctx context.Context, name string) ([]byte, uint64, error) {
	ctx = sqllog.Quiet(ctx)
	return get(ctx, m.db, `SELECT data, version FROM live_configs WHERE name = ?`, name)
}

// Set inserts the row of name when version is 0, and otherwise updates it only while it holds version.
// When no row changed, it returns forgo.ErrLiveConfigConflict.
func (m *MySQL) Set(ctx context.Context, name string, data []byte, version uint64) error {
	ctx = sqllog.Quiet(ctx)
	// data is sent as text: MySQL cannot create a JSON value from a []byte, whose character set is binary.
	if version == 0 {
		return m.insert(ctx, name, string(data))
	}

	res, err := m.db.ExecContext(
		ctx,
		`
		UPDATE live_configs
		SET
			data = ?,
			version = version + 1,
			updated_at = UTC_TIMESTAMP(6)
		WHERE name = ?
		  AND version = ?
		`,
		string(data),
		name,
		version,
	)
	if err != nil {
		return fmt.Errorf("liveconfig: set %q: %w", name, err)
	}
	// The update always changes version, so the affected row count is 1 on a match whether or not the
	// connection reports found rows.
	return applied(res, name)
}

// insert stores the first version of name.
func (m *MySQL) insert(ctx context.Context, name, data string) error {
	_, err := m.db.ExecContext(
		ctx,
		`
		INSERT INTO live_configs (
			name,
			data,
			version,
			updated_at
		)
		VALUES (?, ?, 1, UTC_TIMESTAMP(6))
		`,
		name,
		data,
	)
	if err == nil {
		return nil
	}
	// A duplicate key is told apart from other failures by the row it found, so that no driver error code
	// is needed. ON DUPLICATE KEY UPDATE cannot tell: with clientFoundRows, an unchanged row counts as one.
	if _, stored, getErr := m.Get(ctx, name); getErr == nil && stored != 0 {
		return forgo.ErrLiveConfigConflict
	}
	return fmt.Errorf("liveconfig: set %q: %w", name, err)
}
