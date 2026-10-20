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

// TestIntegrationClaimWithoutExpiry checks a claim with a zero ExpiredBefore, as forgo's forgotest makes them
// to read the outbox in tests: it takes the rows no claim holds, and never a claimed one, however old its
// claim is, since no claim was made before the zero time.
func TestIntegrationClaimWithoutExpiry(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "outbox_events")
			s := storage(d, db)
			for _, id := range []string{"m1", "m2", "m3"} {
				write(t, d, db, worker.Message{ID: id, Name: "n", Payload: []byte(`{}`), Headers: types.NewJSONMap[string](), CreatedAt: time.Now()})
			}

			if got := claimIDs(t, s, "c1", time.Time{}, 2); !reflect.DeepEqual(got, []string{"m1", "m2"}) {
				t.Fatalf("first claim = %v, want m1 and m2", got)
			}
			if got := claimIDs(t, s, "c2", time.Time{}, 10); !reflect.DeepEqual(got, []string{"m3"}) {
				t.Fatalf("second claim = %v, want the row the first one left", got)
			}
			if got := claimIDs(t, s, "c3", time.Time{}, 10); len(got) != 0 {
				t.Fatalf("claim of claimed rows = %v", got)
			}

			// Claims two centuries old, which any claim with an ExpiredBefore takes back.
			age := `UPDATE outbox_events SET processing_at = ` + d.Ago(200*365*24*time.Hour)
			if _, err := db.ExecContext(context.Background(), age); err != nil {
				t.Fatal(err)
			}
			if got := claimIDs(t, s, "c4", time.Time{}, 10); len(got) != 0 {
				t.Fatalf("claim without expiry of old claims = %v", got)
			}
			if got := claimIDs(t, s, "c5", time.Now().Add(-time.Minute), 10); !reflect.DeepEqual(got, []string{"m1", "m2", "m3"}) {
				t.Fatalf("claim of expired claims = %v, want every row", got)
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

// TestIntegrationCompleteDoesNotBlockInserts checks that a release on MySQL leaves the application's
// inserts alone. The release of the claim with the smallest processing_id reads the index entries
// right after the place where a new row's entry goes: under REPEATABLE READ, it would lock that gap
// until it commits. Another transaction holds the last row of the claim, so that the release waits
// there with its locks taken while the application inserts.
func TestIntegrationCompleteDoesNotBlockInserts(t *testing.T) {
	db := sqltest.MySQL.Open(t)
	sqltest.MySQL.Reset(t, db, "outbox_events")
	s := NewMySQL(db)
	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		write(t, sqltest.MySQL, db, worker.Message{ID: fmt.Sprintf("m%02d", i), Name: "n", Payload: []byte(`{}`),
			Headers: types.NewJSONMap[string](), CreatedAt: time.Now()})
	}
	if got := claimIDs(t, s, "c1", time.Now().Add(-time.Minute), 3); !reflect.DeepEqual(got, []string{"m01", "m02", "m03"}) {
		t.Fatalf("Claim = %v", got)
	}

	holder, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.ExecContext(ctx, `SELECT id FROM outbox_events WHERE id = 'm03' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	completed := make(chan error, 1)
	go func() { completed <- s.Complete(ctx, "c1", nil) }()
	waitForLockWait(t, db)

	app, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close() }()
	if _, err := app.ExecContext(ctx, `SET SESSION innodb_lock_wait_timeout = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecContext(ctx, `INSERT INTO outbox_events (id, name, payload) VALUES ('m99', 'n', '{}')`); err != nil {
		t.Errorf("application insert during a release: %v", err)
	}

	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-completed; err != nil {
		t.Fatalf("Complete: %v", err)
	}
}

// waitForLockWait waits until a transaction on db waits for a row lock. InnoDB refreshes
// information_schema.innodb_trx only when it was not read for 100ms, so faster polling would read the
// same snapshot forever.
func waitForLockWait(t *testing.T, db *sql.DB) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		var waiting int
		err := db.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
	}
	t.Fatal("no transaction waits for a lock")
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

// TestIntegrationBacklog checks the backlog against a real database: nothing pending, then the pending rows,
// counted up to the limit, and the first of them by ID, which processing removes.
func TestIntegrationBacklog(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "outbox_events")
			s := storage(d, db).(worker.OutboxBacklogStorage)
			ctx := context.Background()

			if b, err := s.Backlog(ctx, 10); err != nil || b.Pending != 0 || !b.Oldest.IsZero() {
				t.Fatalf("Backlog of an empty outbox = %+v, %v", b, err)
			}

			base := time.Now().Add(-time.Hour).Truncate(time.Microsecond).UTC()
			for i := 1; i <= 3; i++ {
				write(t, d, db, worker.Message{
					ID: fmt.Sprintf("m%d", i), Name: "order.placed", Payload: []byte(`{}`),
					Headers: types.NewJSONMap[string](), CreatedAt: base.Add(time.Duration(i) * time.Minute),
				})
			}
			b, err := s.Backlog(ctx, 10)
			if err != nil || b.Pending != 3 || !b.Oldest.Equal(base.Add(time.Minute)) {
				t.Fatalf("Backlog = %+v, %v; want 3 pending, the oldest created at %v", b, err, base.Add(time.Minute))
			}
			// The count stops at the limit, and the oldest stays the first row.
			b, err = s.Backlog(ctx, 2)
			if err != nil || b.Pending != 2 || !b.Oldest.Equal(base.Add(time.Minute)) {
				t.Fatalf("Backlog with limit 2 = %+v, %v; want 2 pending, the oldest created at %v", b, err, base.Add(time.Minute))
			}

			// Claimed rows are still pending; processed ones are not.
			if got := claimIDs(t, s, "c1", time.Now().Add(-time.Minute), 1); !reflect.DeepEqual(got, []string{"m1"}) {
				t.Fatalf("claimed %v", got)
			}
			if b, err := s.Backlog(ctx, 10); err != nil || b.Pending != 3 {
				t.Fatalf("Backlog with a claim = %+v, %v; want the claimed row counted", b, err)
			}
			if err := s.Complete(ctx, "c1", []string{"m1"}); err != nil {
				t.Fatal(err)
			}
			b, err = s.Backlog(ctx, 10)
			if err != nil || b.Pending != 2 || !b.Oldest.Equal(base.Add(2*time.Minute)) {
				t.Fatalf("Backlog after processing m1 = %+v, %v", b, err)
			}
		})
	}
}
