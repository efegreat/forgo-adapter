// Package outbox implements worker.OutboxStorage on SQL databases.
package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"

	"github.com/efegreat/forgo-adapter/internal/sqllog"
	"github.com/efegreat/forgo-adapter/provider/sqltx"
)

// claimAge returns the age at which a claim expires. ExpiredBefore is a time on the producer's
// clock, so comparing it with processing_at would let skew between that clock and the one that
// stamped the claim shift ClaimTTL. The age does not depend on the clock: the storages stamp and
// expire claims on the database clock alone, so every producer instance agrees on when a claim
// expires.
func claimAge(claim worker.Claim) time.Duration {
	return time.Since(claim.ExpiredBefore)
}

// backlogQuery selects the number of pending rows, through the index of the pending rows (a partial index on
// PostgreSQL, (processed_at, id) on MySQL), and the created_at of the first one by ID, through the same index,
// NULL when there is none. It has no parameters, so it works with either dialect.
const backlogQuery = `
	SELECT
		(SELECT COUNT(*) FROM outbox_events WHERE processed_at IS NULL),
		(SELECT created_at FROM outbox_events WHERE processed_at IS NULL ORDER BY id LIMIT 1)
	`

// backlog runs backlogQuery on db, or on the transaction of ctx.
func backlog(ctx context.Context, db *sql.DB) (worker.OutboxBacklog, error) {
	ctx = sqllog.Quiet(ctx)
	var b worker.OutboxBacklog
	var oldest sql.NullTime
	if err := sqltx.From(ctx, db).QueryRowContext(ctx, backlogQuery).Scan(&b.Pending, &oldest); err != nil {
		return worker.OutboxBacklog{}, fmt.Errorf("outbox: backlog: %w", err)
	}
	if oldest.Valid {
		b.Oldest = oldest.Time
	}
	return b, nil
}

// scanMessages reads claimed rows of id, name, payload, headers, created_at, message_key and
// destination, and closes rows.
func scanMessages(rows *sql.Rows, limit int) ([]worker.Message, error) {
	defer rows.Close()

	messages := make([]worker.Message, 0, limit)
	for rows.Next() {
		message := worker.Message{
			Headers: types.NewJSONMap[string](),
		}
		if err := rows.Scan(
			&message.ID,
			&message.Name,
			&message.Payload,
			message.Headers,
			&message.CreatedAt,
			&message.Key,
			&message.Destination,
		); err != nil {
			return nil, fmt.Errorf("outbox: scan claimed message: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: iterate claimed messages: %w", err)
	}

	// MySQL drivers need the result set closed before the next statement on the connection.
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("outbox: close claimed messages: %w", err)
	}
	return messages, nil
}
