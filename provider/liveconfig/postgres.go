package liveconfig

import (
	"context"
	"database/sql"
	"fmt"
)

// Postgres is a forgo.LiveConfigStore on the live_configs table of a PostgreSQL database.
type Postgres struct {
	db *sql.DB
}

// NewPostgres returns a live config store using db, for forgo.WithProviders.
func NewPostgres(db *sql.DB) *Postgres {
	return &Postgres{db: db}
}

// Get reads the row of name. Without one, it returns version 0.
func (p *Postgres) Get(ctx context.Context, name string) ([]byte, uint64, error) {
	return get(ctx, p.db, `SELECT data, version FROM live_configs WHERE name = $1`, name)
}

// Set inserts the row of name when version is 0, and otherwise updates it only while it holds version.
// When no row changed, it returns forgo.ErrLiveConfigConflict.
func (p *Postgres) Set(ctx context.Context, name string, data []byte, version uint64) error {
	var (
		res sql.Result
		err error
	)
	// data is sent as text: drivers such as lib/pq send []byte as bytea, which a JSONB column rejects.
	if version == 0 {
		res, err = p.db.ExecContext(
			ctx,
			`
			INSERT INTO live_configs (
				name,
				data,
				version,
				updated_at
			)
			VALUES ($1, $2, 1, CURRENT_TIMESTAMP)
			ON CONFLICT (name) DO NOTHING
			`,
			name,
			string(data),
		)
	} else {
		res, err = p.db.ExecContext(
			ctx,
			`
			UPDATE live_configs
			SET
				data = $2,
				version = version + 1,
				updated_at = CURRENT_TIMESTAMP
			WHERE name = $1
			  AND version = $3
			`,
			name,
			string(data),
			version,
		)
	}
	if err != nil {
		return fmt.Errorf("liveconfig: set %q: %w", name, err)
	}
	return applied(res, name)
}
