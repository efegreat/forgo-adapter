package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"
)

// Postgres is a worker.OutboxStorage on the outbox_events table of a PostgreSQL database.
type Postgres struct {
	db *sql.DB
}

// NewPostgres returns an outbox storage using db, for forgo.WithProviders.
func NewPostgres(db *sql.DB) *Postgres {
	return &Postgres{db: db}
}

func (p *Postgres) Claim(
	ctx context.Context,
	claim worker.Claim,
) ([]worker.Message, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("outbox: begin claim transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.QueryContext(
		ctx,
		`
		WITH claimed AS (
			SELECT id
			FROM outbox_events
			WHERE processed_at IS NULL
			  AND (
				  processing_at IS NULL
				  OR processing_at < $1
			  )
			ORDER BY id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events AS o
		SET
			processing_id = $3,
			processing_at = CURRENT_TIMESTAMP
		FROM claimed
		WHERE o.id = claimed.id
		RETURNING
			o.id,
			o.name,
			o.payload,
			o.headers,
			o.created_at,
			o.message_key,
			o.destination
		`,
		claim.ExpiredBefore,
		claim.Limit,
		claim.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("outbox: claim messages: %w", err)
	}

	messages := make([]worker.Message, 0, claim.Limit)

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
			_ = rows.Close()

			return nil, fmt.Errorf(
				"outbox: scan claimed message: %w",
				err,
			)
		}

		messages = append(messages, message)
	}

	if err := rows.Err(); err != nil {
		_ = rows.Close()

		return nil, fmt.Errorf(
			"outbox: iterate claimed messages: %w",
			err,
		)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf(
			"outbox: close claimed messages: %w",
			err,
		)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf(
			"outbox: commit claim transaction: %w",
			err,
		)
	}

	return messages, nil
}

func (p *Postgres) Complete(
	ctx context.Context,
	claimID string,
	successfulIDs []string,
) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf(
			"outbox: begin complete transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if len(successfulIDs) > 0 {
		args := make([]any, 0, len(successfulIDs)+1)
		args = append(args, claimID)

		for _, id := range successfulIDs {
			args = append(args, id)
		}

		query := `
			UPDATE outbox_events
			SET
				processed_at = CURRENT_TIMESTAMP,
				processing_id = NULL,
				processing_at = NULL
			WHERE processing_id = $1
			  AND processed_at IS NULL
			  AND id IN (` +
			postgresPlaceholders(2, len(successfulIDs)) +
			`)`

		if _, err := tx.ExecContext(
			ctx,
			query,
			args...,
		); err != nil {
			return fmt.Errorf(
				"outbox: mark messages processed: %w",
				err,
			)
		}
	}

	// Anything still carrying this claim failed to publish.
	//
	// Successful messages already had processing_id cleared by the query
	// above, therefore this only releases the remaining messages.
	if _, err := tx.ExecContext(
		ctx,
		`
		UPDATE outbox_events
		SET
			processing_id = NULL,
			processing_at = NULL
		WHERE processing_id = $1
		  AND processed_at IS NULL
		`,
		claimID,
	); err != nil {
		return fmt.Errorf(
			"outbox: release unprocessed messages: %w",
			err,
		)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(
			"outbox: commit complete transaction: %w",
			err,
		)
	}

	return nil
}

func (p *Postgres) DeleteProcessedBefore(
	ctx context.Context,
	before time.Time,
	limit int,
) (int64, error) {
	// PostgreSQL doesn't support DELETE ... LIMIT directly, so select a
	// limited batch first and delete those rows through DELETE ... USING.
	//
	// SKIP LOCKED also means multiple outbox instances may perform cleanup
	// without waiting on one another.
	result, err := p.db.ExecContext(
		ctx,
		`
		WITH deleted AS (
			SELECT id
			FROM outbox_events
			WHERE processed_at IS NOT NULL
			  AND processed_at < $1
			ORDER BY processed_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM outbox_events AS o
		USING deleted
		WHERE o.id = deleted.id
		`,
		before,
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"outbox: delete processed messages: %w",
			err,
		)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf(
			"outbox: get deleted message count: %w",
			err,
		)
	}

	return count, nil
}

func postgresPlaceholders(start, count int) string {
	var b strings.Builder

	for i := range count {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteByte('$')
		b.WriteString(strconv.Itoa(start + i))
	}

	return b.String()
}
