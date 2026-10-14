package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/internal/sqllog"
	"github.com/efegreat/forgo-adapter/provider/sqltx"
)

// MySQL is a worker.OutboxStorage on the outbox_events table of a MySQL 8 database.
// Statements join the transaction a sqltx.Transactor put in the context. It stamps rows with
// UTC_TIMESTAMP(6) and ages them on the database clock, so that neither the session time zone nor
// the driver's loc moves a claim or the cleanup.
type MySQL struct {
	db *sql.DB
}

// NewMySQL returns an outbox storage using db, for forgo.WithProviders.
func NewMySQL(db *sql.DB) *MySQL {
	return &MySQL{db: db}
}

// readCommitted is the isolation of the transactions Claim and Complete begin. Under REPEATABLE READ,
// InnoDB also locks the gaps between the index records a locking statement reads, and the gap at the
// end of the unprocessed rows is where the application inserts new ones: a claim that reads up to
// it, or a release of the claim with the smallest processing_id, would block those inserts until it
// commits. A storage that joins the caller's transaction keeps the caller's isolation.
var readCommitted = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

// Claim selects the messages and stamps them in one transaction. Claims are stamped and expired on
// the database clock (see claimAge), in UTC so that neither the session time zone nor its daylight
// saving changes move them.
func (m *MySQL) Claim(
	ctx context.Context,
	claim worker.Claim,
) ([]worker.Message, error) {
	ctx = sqllog.Quiet(ctx)
	var messages []worker.Message

	err := sqltx.RunWith(ctx, m.db, readCommitted, func(ctx context.Context) error {
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
				  OR processing_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
			  )
			ORDER BY id
			LIMIT ?
			FOR UPDATE SKIP LOCKED
			`,
			claimAge(claim).Microseconds(),
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

		args := make([]any, 0, len(messages)+1)

		args = append(args, claim.ID)

		for i := range messages {
			args = append(args, messages[i].ID)
		}

		if _, err := tx.ExecContext(
			ctx,
			`
			UPDATE outbox_events
			SET
				processing_id = ?,
				processing_at = UTC_TIMESTAMP(6)
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

// Complete marks the messages of successfulIDs processed and releases the other messages of the
// claim, so that the next claim takes them again, in one transaction.
func (m *MySQL) Complete(
	ctx context.Context,
	claimID string,
	successfulIDs []string,
) error {
	ctx = sqllog.Quiet(ctx)
	return sqltx.RunWith(ctx, m.db, readCommitted, func(ctx context.Context) error {
		tx := sqltx.From(ctx, m.db)

		if len(successfulIDs) > 0 {
			args := make([]any, 0, len(successfulIDs)+1)

			args = append(args, claimID)

			for _, id := range successfulIDs {
				args = append(args, id)
			}

			if _, err := tx.ExecContext(
				ctx,
				`
				UPDATE outbox_events
				SET
					processed_at = UTC_TIMESTAMP(6),
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

// DeleteProcessedBefore deletes up to limit messages processed before before, and returns how many
// it deleted. before is turned into an age, which the database compares with its own clock.
func (m *MySQL) DeleteProcessedBefore(
	ctx context.Context,
	before time.Time,
	limit int,
) (int64, error) {
	ctx = sqllog.Quiet(ctx)
	result, err := sqltx.From(ctx, m.db).ExecContext(
		ctx,
		`
		DELETE FROM outbox_events
		WHERE processed_at IS NOT NULL
		  AND processed_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
		ORDER BY processed_at, id
		LIMIT ?
		`,
		time.Since(before).Microseconds(),
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
