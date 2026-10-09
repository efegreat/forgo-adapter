package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/provider/sqltx"
)

// Postgres is a worker.InboxStorage on the inbox_events table of a PostgreSQL database.
type Postgres struct {
	db *sql.DB
}

// NewPostgres returns an inbox storage using db, for forgo.WithProviders.
func NewPostgres(db *sql.DB) *Postgres {
	return &Postgres{db: db}
}

func (p *Postgres) Status(
	ctx context.Context,
	messageID string,
	subscriberName string,
) (worker.InboxStatus, bool, error) {
	var status worker.InboxStatus

	err := sqltx.From(ctx, p.db).QueryRowContext(
		ctx,
		`
		SELECT status
		FROM inbox_events
		WHERE message_id = $1
		  AND subscriber_name = $2
		`,
		messageID,
		subscriberName,
	).Scan(&status)

	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, fmt.Errorf(
			"inbox: get status: %w",
			err,
		)
	}

	return status, true, nil
}

func (p *Postgres) IncrementAttempt(
	ctx context.Context,
	messageID string,
	subscriberName string,
) (int, error) {
	var attempts int

	err := sqltx.From(ctx, p.db).QueryRowContext(
		ctx,
		`
		INSERT INTO inbox_events (
			message_id,
			subscriber_name,
			status,
			attempts,
			created_at,
			updated_at
		)
		VALUES (
			$1,
			$2,
			$3,
			1,
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP
		)
		ON CONFLICT (message_id, subscriber_name)
		DO UPDATE SET
			attempts = inbox_events.attempts + 1,
			updated_at = CURRENT_TIMESTAMP
		WHERE inbox_events.status = $3
		RETURNING attempts
		`,
		messageID,
		subscriberName,
		worker.InboxWaiting,
	).Scan(&attempts)

	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotWaiting
	}

	if err != nil {
		return 0, fmt.Errorf(
			"inbox: increment attempt: %w",
			err,
		)
	}

	return attempts, nil
}

func (p *Postgres) MarkCompleted(
	ctx context.Context,
	messageID string,
	subscriberName string,
) error {
	return p.markStatus(
		ctx,
		messageID,
		subscriberName,
		worker.InboxCompleted,
	)
}

func (p *Postgres) MarkFailed(
	ctx context.Context,
	messageID string,
	subscriberName string,
) error {
	return p.markStatus(
		ctx,
		messageID,
		subscriberName,
		worker.InboxFailed,
	)
}

func (p *Postgres) MarkRejected(
	ctx context.Context,
	messageID string,
	subscriberName string,
) error {
	return p.markStatus(
		ctx,
		messageID,
		subscriberName,
		worker.InboxRejected,
	)
}

func (p *Postgres) markStatus(
	ctx context.Context,
	messageID string,
	subscriberName string,
	target worker.InboxStatus,
) error {
	var status worker.InboxStatus

	err := sqltx.From(ctx, p.db).QueryRowContext(
		ctx,
		`
		UPDATE inbox_events
		SET
			status = $3,
			updated_at = CURRENT_TIMESTAMP
		WHERE message_id = $1
		  AND subscriber_name = $2
		  AND status = $4
		RETURNING status
		`,
		messageID,
		subscriberName,
		target,
		worker.InboxWaiting,
	).Scan(&status)

	if err == nil {
		return nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(
			"inbox: mark status %d: %w",
			target,
			err,
		)
	}

	// The Waiting -> target transition did not happen.
	//
	// Check whether this is simply an idempotent repeat of the same
	// terminal transition.
	current, exists, err := p.Status(
		ctx,
		messageID,
		subscriberName,
	)
	if err != nil {
		return err
	}

	if !exists {
		return sql.ErrNoRows
	}

	if current == target {
		return nil
	}

	return ErrNotWaiting
}

func (p *Postgres) DeleteBefore(
	ctx context.Context,
	before time.Time,
	limit int,
) (int64, error) {
	result, err := sqltx.From(ctx, p.db).ExecContext(
		ctx,
		`
		WITH candidates AS (
			SELECT message_id, subscriber_name
			FROM inbox_events
			WHERE status IN ($1, $2)
			  AND updated_at < $3
			ORDER BY updated_at
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM inbox_events AS i
		USING candidates AS c
		WHERE i.message_id = c.message_id
		  AND i.subscriber_name = c.subscriber_name
		`,
		worker.InboxCompleted,
		worker.InboxRejected,
		before,
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"inbox: delete old terminal messages: %w",
			err,
		)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf(
			"inbox: get deleted message count: %w",
			err,
		)
	}

	return count, nil
}
