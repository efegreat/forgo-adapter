package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/internal/sqllog"
	"github.com/efegreat/forgo-adapter/provider/sqltx"
)

// Postgres is a worker.OutboxStorage on the outbox_events table of a PostgreSQL database.
// Statements join the transaction a sqltx.Transactor put in the context.
type Postgres struct {
	db *sql.DB
}

// NewPostgres returns an outbox storage using db, for forgo.WithProviders.
func NewPostgres(db *sql.DB) *Postgres {
	return &Postgres{db: db}
}

// Claim claims the messages in one UPDATE … RETURNING. It runs in a transaction so a failure while
// reading the claimed rows releases them right away instead of after ClaimTTL. Claims are stamped
// and expired on the database clock (see claimAge).
//
// The IDs of the claimed rows are collected into an array before the update, which then reads them
// through the primary key. A join with them, as in WITH … UPDATE … FROM, would scan the whole table
// in a generic plan, which does not know the limit and expects many rows.
func (p *Postgres) Claim(
	ctx context.Context,
	claim worker.Claim,
) ([]worker.Message, error) {
	ctx = sqllog.Quiet(ctx)
	var messages []worker.Message

	err := sqltx.Run(ctx, p.db, func(ctx context.Context) error {
		rows, err := sqltx.From(ctx, p.db).QueryContext(
			ctx,
			`
			UPDATE outbox_events
			SET
				processing_id = $3,
				processing_at = CURRENT_TIMESTAMP
			WHERE id = ANY(ARRAY(
				SELECT id
				FROM outbox_events
				WHERE processed_at IS NULL
				  AND (
					  processing_at IS NULL
					  OR processing_at < CURRENT_TIMESTAMP - make_interval(secs => $1)
				  )
				ORDER BY id
				LIMIT $2
				FOR UPDATE SKIP LOCKED
			))
			RETURNING
				id,
				name,
				payload,
				headers,
				created_at,
				message_key,
				destination
			`,
			claimAge(claim).Seconds(),
			claim.Limit,
			claim.ID,
		)
		if err != nil {
			return fmt.Errorf("outbox: claim messages: %w", err)
		}

		messages, err = scanMessages(rows, claim.Limit)
		return err
	})
	if err != nil {
		return nil, err
	}

	return messages, nil
}

// Complete marks the messages of successfulIDs processed and releases the other messages of the
// claim, so that the next claim takes them again, in one transaction.
func (p *Postgres) Complete(
	ctx context.Context,
	claimID string,
	successfulIDs []string,
) error {
	ctx = sqllog.Quiet(ctx)
	return sqltx.Run(ctx, p.db, func(ctx context.Context) error {
		tx := sqltx.From(ctx, p.db)

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

		return nil
	})
}

// Backlog returns the number of messages not processed yet, up to limit, and the created_at of the first one
// by ID, for the producer's worker.IBacklogObserver. Both come from the outbox_events_pending partial index,
// so the count reads pending rows only, and no more than limit of them.
func (p *Postgres) Backlog(ctx context.Context, limit int) (worker.OutboxBacklog, error) {
	return backlog(ctx, p.db, postgresBacklogQuery, limit)
}

// DeleteProcessedBefore deletes up to limit messages processed before before, and returns how many
// it deleted. Rows that another cleanup holds are skipped.
func (p *Postgres) DeleteProcessedBefore(
	ctx context.Context,
	before time.Time,
	limit int,
) (int64, error) {
	ctx = sqllog.Quiet(ctx)
	// PostgreSQL has no DELETE … LIMIT, so the IDs of a limited batch are
	// collected into an array first, and the delete reads them through the
	// primary key, in generic plans too (see Claim).
	//
	// SKIP LOCKED also means multiple outbox instances may perform cleanup
	// without waiting on one another.
	result, err := sqltx.From(ctx, p.db).ExecContext(
		ctx,
		`
		DELETE FROM outbox_events
		WHERE id = ANY(ARRAY(
			SELECT id
			FROM outbox_events
			WHERE processed_at IS NOT NULL
			  AND processed_at < $1
			ORDER BY processed_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		))
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
