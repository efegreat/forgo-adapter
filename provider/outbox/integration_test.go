package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/types"

	"github.com/efegreat/forgo-adapter/internal/sqltest"
)

// storage returns the outbox of the dialect d on db.
func storage(d sqltest.Dialect, db *sql.DB) worker.OutboxStorage {
	if d == sqltest.Postgres {
		return NewPostgres(db)
	}
	return NewMySQL(db)
}

// write stores msg in the outbox, as an application does in the transaction of its change.
func write(t *testing.T, d sqltest.Dialect, db *sql.DB, msg worker.Message) {
	t.Helper()
	placeholders := make([]string, 7)
	for i := range placeholders {
		placeholders[i] = d.Placeholder(i + 1)
	}
	query := `INSERT INTO outbox_events (id, name, payload, headers, created_at, message_key, destination) VALUES (` +
		strings.Join(placeholders, ", ") + `)`
	if _, err := db.ExecContext(context.Background(), query,
		msg.ID, msg.Name, string(msg.Payload), msg.Headers, msg.CreatedAt, msg.Key, msg.Destination); err != nil {
		t.Fatal(err)
	}
}

// claimIDs claims up to limit messages under id and returns their IDs in order.
func claimIDs(t *testing.T, s worker.OutboxStorage, id string, expiredBefore time.Time, limit int) []string {
	t.Helper()
	messages, err := s.Claim(context.Background(), worker.Claim{ID: id, ExpiredBefore: expiredBefore, Limit: limit})
	if err != nil {
		t.Fatalf("Claim %s: %v", id, err)
	}
	ids := make([]string, len(messages))
	for i, m := range messages {
		ids[i] = m.ID
	}
	slices.Sort(ids)
	return ids
}

// TestIntegrationRelay runs the claims of a producer against a real database: what a claim returns,
// that claimed rows are skipped until they expire, and that Complete processes and releases them.
func TestIntegrationRelay(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "outbox_events")
			s := storage(d, db)
			ctx := context.Background()

			created := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
			var sent []worker.Message
			for i := 1; i <= 3; i++ {
				msg := worker.Message{
					ID:          fmt.Sprintf("m%d", i),
					Name:        "order.placed",
					Key:         fmt.Sprintf("order-%d", i),
					Destination: "orders",
					Payload:     []byte(fmt.Sprintf(`{"id": %d}`, i)),
					Headers:     types.NewJSONMapWithData(map[string]string{"tenant": "acme"}),
					CreatedAt:   created,
				}
				write(t, d, db, msg)
				sent = append(sent, msg)
			}
			fresh := time.Now().Add(-time.Minute) // claims younger than a minute hold

			claimed, err := s.Claim(ctx, worker.Claim{ID: "c1", ExpiredBefore: fresh, Limit: 2})
			if err != nil || len(claimed) != 2 {
				t.Fatalf("Claim = %d messages, %v; want 2", len(claimed), err)
			}
			slices.SortFunc(claimed, func(a, b worker.Message) int { return strings.Compare(a.ID, b.ID) })
			for i, got := range claimed {
				checkMessage(t, got, sent[i])
			}

			if got := claimIDs(t, s, "c2", fresh, 10); !reflect.DeepEqual(got, []string{"m3"}) {
				t.Fatalf("second claim = %v, want the row the first one left", got)
			}

			if err := s.Complete(ctx, "c1", []string{"m1"}); err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if got := claimIDs(t, s, "c3", fresh, 10); !reflect.DeepEqual(got, []string{"m2"}) {
				t.Fatalf("claim after Complete = %v, want the released m2 only", got)
			}

			// Every claim made before now has expired.
			if got := claimIDs(t, s, "c4", time.Now().Add(time.Second), 10); !reflect.DeepEqual(got, []string{"m2", "m3"}) {
				t.Fatalf("claim of expired rows = %v, want m2 and m3", got)
			}
			// The earlier claims no longer own their rows.
			if err := s.Complete(ctx, "c2", []string{"m3"}); err != nil {
				t.Fatal(err)
			}
			if err := s.Complete(ctx, "c4", []string{"m2", "m3"}); err != nil {
				t.Fatal(err)
			}
			if got := claimIDs(t, s, "c5", time.Now().Add(time.Second), 10); len(got) != 0 {
				t.Fatalf("claim after every row was processed = %v", got)
			}

			// m1 was processed long ago, m2 and m3 just now.
			age := `UPDATE outbox_events SET processed_at = ` + d.Ago(40*24*time.Hour) + ` WHERE id = 'm1'`
			if _, err := db.ExecContext(ctx, age); err != nil {
				t.Fatal(err)
			}
			if deleted, err := s.DeleteProcessedBefore(ctx, time.Now().Add(-time.Hour), 100); err != nil || deleted != 1 {
				t.Fatalf("DeleteProcessedBefore = %d, %v; want the old row", deleted, err)
			}
			if deleted, err := s.DeleteProcessedBefore(ctx, time.Now().Add(time.Minute), 1); err != nil || deleted != 1 {
				t.Fatalf("DeleteProcessedBefore with limit 1 = %d, %v", deleted, err)
			}
		})
	}
}

// TestIntegrationCompleteReleasesFailures checks that Complete without successful IDs releases
// every row of the claim at once, without waiting for the claim to expire.
func TestIntegrationCompleteReleasesFailures(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "outbox_events")
			s := storage(d, db)
			for _, id := range []string{"m1", "m2"} {
				write(t, d, db, worker.Message{ID: id, Name: "n", Payload: []byte(`{}`), Headers: types.NewJSONMap[string](), CreatedAt: time.Now()})
			}
			fresh := time.Now().Add(-time.Minute)

			if got := claimIDs(t, s, "c1", fresh, 10); len(got) != 2 {
				t.Fatalf("Claim = %v", got)
			}
			if err := s.Complete(context.Background(), "c1", nil); err != nil {
				t.Fatal(err)
			}
			if got := claimIDs(t, s, "c2", fresh, 10); !reflect.DeepEqual(got, []string{"m1", "m2"}) {
				t.Fatalf("claim after a failed publish = %v, want both rows again", got)
			}
		})
	}
}

// checkMessage compares a claimed message with the one written. The databases normalize JSON, so
// payloads are compared as values.
func checkMessage(t *testing.T, got, want worker.Message) {
	t.Helper()
	if got.ID != want.ID || got.Name != want.Name || got.Key != want.Key || got.Destination != want.Destination ||
		!got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("claimed %+v, want %+v", got, want)
	}
	var gotPayload, wantPayload any
	if err := json.Unmarshal(got.Payload, &gotPayload); err != nil {
		t.Fatalf("payload %s: %v", got.Payload, err)
	}
	_ = json.Unmarshal(want.Payload, &wantPayload)
	if !reflect.DeepEqual(gotPayload, wantPayload) {
		t.Fatalf("payload = %s, want %s", got.Payload, want.Payload)
	}
	if tenant, _ := got.Headers.Get("tenant"); tenant != "acme" {
		t.Fatalf("headers = %v", got.Headers)
	}
}
