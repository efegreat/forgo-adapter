package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/provider/sqltx"
)

// Mysql is a worker.OutboxStorage on the outbox_events table of a MySQL 8 database.
// Statements join the transaction a sqltx.Transactor put in the context.
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
	var messages []worker.Message

	err := sqltx.Run(ctx, m.db, func(ctx context.Context) error {
		tx := sqltx.From(ctx, m.db)

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
			return fmt.Errorf(
				"outbox: select claimable messages: %w",
				err,
			)
		}

		messages, err = scanMessages(rows, claim.Limit)
		if err != nil || len(messages) == 0 {
			return err
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

		if _, err := tx.ExecContext(
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
		); err != nil {
			return fmt.Errorf(
				"outbox: mark messages claimed: %w",
				err,
			)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (m *Mysql) Complete(
	ctx context.Context,
	claimID string,
	successfulIDs []string,
) error {
	return sqltx.Run(ctx, m.db, func(ctx context.Context) error {
		tx := sqltx.From(ctx, m.db)

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

			if _, err := tx.ExecContext(
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
			); err != nil {
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
		if _, err := tx.ExecContext(
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
		); err != nil {
			return fmt.Errorf(
				"outbox: release unprocessed messages: %w",
				err,
			)
		}

		return nil
	})
}

func (m *Mysql) DeleteProcessedBefore(
	ctx context.Context,
	before time.Time,
	limit int,
) (int64, error) {
	result, err := sqltx.From(ctx, m.db).ExecContext(
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
