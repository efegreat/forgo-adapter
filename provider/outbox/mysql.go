package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"
)

// Mysql is a worker.OutboxStorage on the outbox_events table of a MySQL 8 database.
type Mysql struct {
	db *sql.DB
}

// NewMysql returns an outbox storage using db, for forgo.WithProviders.
func NewMysql(db *sql.DB) *Mysql {
	return &Mysql{db: db}
}

func (m *Mysql) Claim(
	ctx context.Context,
	claim worker.Claim,
) ([]worker.Message, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf(
			"outbox: begin claim transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.QueryContext(
		ctx,
		`
		SELECT
			id,
			name,
			payload,
			headers,
			created_at,
			message_key,
			destination
		FROM outbox_events
		WHERE processed_at IS NULL
		  AND (
			  processing_at IS NULL
			  OR processing_at < ?
		  )
		ORDER BY id
		LIMIT ?
		FOR UPDATE SKIP LOCKED
		`,
		claim.ExpiredBefore,
		claim.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"outbox: select claimable messages: %w",
			err,
		)
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

	// Important for MySQL drivers: finish/close the result set before
	// executing another statement on this transaction/connection.
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf(
			"outbox: close claimed message rows: %w",
			err,
		)
	}

	if len(messages) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf(
				"outbox: commit empty claim transaction: %w",
				err,
			)
		}

		return messages, nil
	}

	args := make([]any, 0, len(messages)+2)

	args = append(
		args,
		claim.ID,
		time.Now().UTC(),
	)

	for i := range messages {
		args = append(args, messages[i].ID)
	}

	_, err = tx.ExecContext(
		ctx,
		`
		UPDATE outbox_events
		SET
			processing_id = ?,
			processing_at = ?
		WHERE processed_at IS NULL
		  AND id IN (`+placeholders(len(messages))+`)
		`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"outbox: mark messages claimed: %w",
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

func (m *Mysql) Complete(
	ctx context.Context,
	claimID string,
	successfulIDs []string,
) error {
	tx, err := m.db.BeginTx(ctx, nil)
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
		args := make([]any, 0, len(successfulIDs)+2)

		args = append(
			args,
			time.Now().UTC(),
			claimID,
		)

		for _, id := range successfulIDs {
			args = append(args, id)
		}

		_, err := tx.ExecContext(
			ctx,
			`
			UPDATE outbox_events
			SET
				processed_at = ?,
				processing_id = NULL,
				processing_at = NULL
			WHERE processing_id = ?
			  AND processed_at IS NULL
			  AND id IN (`+placeholders(len(successfulIDs))+`)
			`,
			args...,
		)
		if err != nil {
			return fmt.Errorf(
				"outbox: mark messages processed: %w",
				err,
			)
		}
	}

	// Successful messages already have processing_id = NULL.
	//
	// Anything still owned by this claim therefore represents a message
	// that failed to publish. Release those rows so they can immediately
	// be retried.
	_, err = tx.ExecContext(
		ctx,
		`
		UPDATE outbox_events
		SET
			processing_id = NULL,
			processing_at = NULL
		WHERE processing_id = ?
		  AND processed_at IS NULL
		`,
		claimID,
	)
	if err != nil {
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

func (m *Mysql) DeleteProcessedBefore(
	ctx context.Context,
	before time.Time,
	limit int,
) (int64, error) {
	result, err := m.db.ExecContext(
		ctx,
		`
		DELETE FROM outbox_events
		WHERE processed_at IS NOT NULL
		  AND processed_at < ?
		ORDER BY processed_at, id
		LIMIT ?
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

func placeholders(count int) string {
	return strings.TrimSuffix(
		strings.Repeat("?,", count),
		",",
	)
}
