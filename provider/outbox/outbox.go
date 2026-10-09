// Package outbox implements worker.OutboxStorage on SQL databases.
package outbox

import (
	"database/sql"
	"fmt"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"
)

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
