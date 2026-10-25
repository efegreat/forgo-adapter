// Package liveconfig implements forgo.LiveConfigStore on PostgreSQL and MySQL. The cache that every instance
// shares in front of them is in the redis module: liveconfigcache.NewRedis of
// github.com/efegreat/forgo-adapter/redis/liveconfigcache.
//
// The stores read and write the database directly, never through the transaction a context carries:
// forgo.LiveConfigs caches a document once its save succeeds, and the cache never expires, so a save must
// be committed when Set returns.
//
// See the [liveconfig] section of REFERENCE.md.
//
// [liveconfig]: https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#liveconfig
package liveconfig

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/efegreat/forgo"
)

// get runs query, which selects the data and version columns of the row named name.
func get(ctx context.Context, db *sql.DB, query, name string) ([]byte, uint64, error) {
	var (
		data    []byte
		version uint64
	)
	err := db.QueryRowContext(ctx, query, name).Scan(&data, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("liveconfig: get %q: %w", name, err)
	}
	return data, version, nil
}

// applied returns the outcome of a conditional write of name: forgo.ErrLiveConfigConflict when it changed
// no row.
func applied(res sql.Result, name string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("liveconfig: set %q: get affected row count: %w", name, err)
	}
	if n == 0 {
		return forgo.ErrLiveConfigConflict
	}
	return nil
}
