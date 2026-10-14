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

// MySQL is a worker.InboxStorage on the inbox_events table of a MySQL 8 database. It stamps rows
// with UTC_TIMESTAMP(6) and ages them on the database clock, so that neither the session time zone,
// its daylight saving changes nor the driver's loc moves the cleanup cutoff.
type MySQL struct {
	db *sql.DB
}

// NewMySQL returns an inbox storage using db, for forgo.WithProviders.
func NewMySQL(db *sql.DB) *MySQL {
	return &MySQL{db: db}
}

// readCommitted is the isolation of the transaction IncrementAttempt begins, as of every transaction
// the MySQL storages begin. Its upsert locks only the message's row at either isolation, but under
// REPEATABLE READ, a locking read added to the transaction would also lock gaps of the primary key,
// where consecutive UUIDv7 message IDs put the rows of new messages. A storage that joins the
// caller's transaction keeps the caller's isolation.
var readCommitted = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

// Status returns the status of the message for the subscriber, and false when the inbox has no row for it.
func (m *MySQL) Status(
	ctx context.Context,
	messageID string,
	subscriberName string,
) (worker.InboxStatus, bool, error) {
	var status worker.InboxStatus

	err := sqltx.From(ctx, m.db).QueryRowContext(
		ctx,
		`
		SELECT status
		FROM inbox_events
		WHERE message_id = ?
		  AND subscriber_name = ?
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

// IncrementAttempt counts an attempt at a waiting message and returns the count, inserting the
// message on its first attempt. It returns ErrNotWaiting once the message left waiting. The attempt
// is counted by one upsert, as in the PostgreSQL inbox: concurrent deliveries of a new message wait
// for the row the first one inserts, and then count their own attempts on it.
func (m *MySQL) IncrementAttempt(
	ctx context.Context,
	messageID string,
	subscriberName string,
) (int, error) {
	var attempts int

	err := sqltx.RunWith(ctx, m.db, readCommitted, func(ctx context.Context) error {
		tx := sqltx.From(ctx, m.db)

		// A message that left waiting keeps its row as it is.
		if _, err := tx.ExecContext(
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
			VALUES (?, ?, ?, 1, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
			ON DUPLICATE KEY UPDATE
				attempts = IF(status = ?, attempts + 1, attempts),
				updated_at = IF(status = ?, UTC_TIMESTAMP(6), updated_at)
			`,
			messageID,
			subscriberName,
			worker.InboxWaiting,
			worker.InboxWaiting,
			worker.InboxWaiting,
		); err != nil {
			return fmt.Errorf(
				"inbox: increment attempt: %w",
				err,
			)
		}

		// The upsert locked the row until the transaction ends, so this reads what it left.
		var status worker.InboxStatus
		if err := tx.QueryRowContext(
			ctx,
			`
			SELECT
				status,
				attempts
			FROM inbox_events
			WHERE message_id = ?
			  AND subscriber_name = ?
			`,
			messageID,
			subscriberName,
		).Scan(
			&status,
			&attempts,
		); err != nil {
			return fmt.Errorf(
				"inbox: read attempt: %w",
				err,
			)
		}

		if status != worker.InboxWaiting {
			return ErrNotWaiting
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return attempts, nil
}

// MarkCompleted moves the waiting message to completed. It fails unless it moved the message itself,
// as the package documentation explains.
func (m *MySQL) MarkCompleted(
	ctx context.Context,
	messageID string,
	subscriberName string,
) error {
	return m.markStatus(
		ctx,
		messageID,
		subscriberName,
		worker.InboxCompleted,
		false,
	)
}

// MarkFailed moves the waiting message to failed. Repeating it succeeds.
func (m *MySQL) MarkFailed(
	ctx context.Context,
	messageID string,
	subscriberName string,
) error {
	return m.markStatus(
		ctx,
		messageID,
		subscriberName,
		worker.InboxFailed,
		true,
	)
}

// MarkRejected moves the waiting message to rejected. Repeating it succeeds.
func (m *MySQL) MarkRejected(
	ctx context.Context,
	messageID string,
	subscriberName string,
) error {
	return m.markStatus(
		ctx,
		messageID,
		subscriberName,
		worker.InboxRejected,
		true,
	)
}

func (m *MySQL) markStatus(
	ctx context.Context,
	messageID string,
	subscriberName string,
	target worker.InboxStatus,
	repeatable bool,
) error {
	result, err := sqltx.From(ctx, m.db).ExecContext(
		ctx,
		`
		UPDATE inbox_events
		SET
			status = ?,
			updated_at = UTC_TIMESTAMP(6)
		WHERE message_id = ?
		  AND subscriber_name = ?
		  AND status = ?
		`,
		target,
		messageID,
		subscriberName,
		worker.InboxWaiting,
	)
	if err != nil {
		return fmt.Errorf(
			"inbox: mark status %d: %w",
			target,
			err,
		)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"inbox: get affected row count: %w",
			err,
		)
	}

	if affected > 0 {
		return nil
	}

	// The Waiting -> target transition did not happen.
	current, exists, err := m.Status(
		ctx,
		messageID,
		subscriberName,
	)
	if err != nil {
		return err
	}

	return unmoved(current, exists, target, repeatable)
}

// DeleteBefore deletes up to limit completed and rejected messages last updated before before, and
// returns how many it deleted. before is turned into an age, which the database compares with its
// own clock. Rows are read in the order of the inbox_events_cleanup index, (status, updated_at), so
// that a batch stops at its limit without sorting the expired rows or reading and locking the
// failed ones, which are kept.
func (m *MySQL) DeleteBefore(
	ctx context.Context,
	before time.Time,
	limit int,
) (int64, error) {
	result, err := sqltx.From(ctx, m.db).ExecContext(
		ctx,
		`
		DELETE FROM inbox_events
		WHERE status IN (?, ?)
		  AND updated_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
		ORDER BY status, updated_at
		LIMIT ?
		`,
		worker.InboxCompleted,
		worker.InboxRejected,
		time.Since(before).Microseconds(),
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
