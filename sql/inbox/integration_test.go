package inbox

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/sql/internal/sqltest"
	"github.com/efegreat/forgo-adapter/sql/sqltx"
)

// storage returns the inbox of the dialect d on db.
func storage(d sqltest.Dialect, db *sql.DB) worker.InboxStorage {
	if d == sqltest.Postgres {
		return NewPostgres(db)
	}
	return NewMySQL(db)
}

// TestIntegrationTransitions runs the life of a message against a real database: attempts, the
// strict MarkCompleted, the repeatable MarkFailed and a completion that rolls back with its handler.
func TestIntegrationTransitions(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "inbox_events")
			s := storage(d, db)
			ctx := context.Background()

			if _, ok, err := s.Status(ctx, "m1", sub); err != nil || ok {
				t.Fatalf("Status of a new message = %v, %v; want false, nil", ok, err)
			}
			for want := 1; want <= 2; want++ {
				if got, err := s.IncrementAttempt(ctx, "m1", sub); err != nil || got != want {
					t.Fatalf("IncrementAttempt = %d, %v; want %d", got, err, want)
				}
			}
			// Another subscriber counts its own attempts.
			if got, err := s.IncrementAttempt(ctx, "m1", "shipping"); err != nil || got != 1 {
				t.Fatalf("IncrementAttempt of another subscriber = %d, %v; want 1", got, err)
			}

			// A completion that rolls back with its handler leaves the message waiting.
			errHandler := errors.New("handler failed")
			err := sqltx.Run(ctx, db, func(ctx context.Context) error {
				if err := s.MarkCompleted(ctx, "m1", sub); err != nil {
					return err
				}
				return errHandler
			})
			if !errors.Is(err, errHandler) {
				t.Fatalf("Run = %v, want the handler's error", err)
			}
			if status, _, err := s.Status(ctx, "m1", sub); err != nil || status != worker.InboxWaiting {
				t.Fatalf("Status after a rollback = %v, %v; want waiting", status, err)
			}

			if err := sqltx.Run(ctx, db, func(ctx context.Context) error { return s.MarkCompleted(ctx, "m1", sub) }); err != nil {
				t.Fatalf("MarkCompleted: %v", err)
			}
			if status, ok, err := s.Status(ctx, "m1", sub); err != nil || !ok || status != worker.InboxCompleted {
				t.Fatalf("Status = %v, %v, %v; want completed", status, ok, err)
			}
			if err := s.MarkCompleted(ctx, "m1", sub); !errors.Is(err, ErrNotWaiting) {
				t.Fatalf("second MarkCompleted = %v, want ErrNotWaiting", err)
			}
			if _, err := s.IncrementAttempt(ctx, "m1", sub); !errors.Is(err, ErrNotWaiting) {
				t.Fatalf("IncrementAttempt of a completed message = %v, want ErrNotWaiting", err)
			}
			if err := s.MarkFailed(ctx, "m1", sub); !errors.Is(err, ErrNotWaiting) {
				t.Fatalf("MarkFailed of a completed message = %v, want ErrNotWaiting", err)
			}

			for range 2 {
				if err := s.MarkFailed(ctx, "m1", "shipping"); err != nil {
					t.Fatalf("MarkFailed: %v", err)
				}
			}
			if err := s.MarkRejected(ctx, "unknown", sub); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("MarkRejected of an unknown message = %v, want sql.ErrNoRows", err)
			}
		})
	}
}

// TestIntegrationConcurrentFirstAttempts runs the first attempts of new messages at once, as
// consumers handling new messages concurrently do. Their UUIDv7 IDs put their rows in the same gap
// of the primary key, where transactions that read a missing row FOR UPDATE and then insert it
// deadlock under MySQL's REPEATABLE READ.
func TestIntegrationConcurrentFirstAttempts(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "inbox_events")
			s := storage(d, db)

			const workers, perWorker = 8, 25
			var failed atomic.Int64
			var firstErr sync.Once
			var wg sync.WaitGroup
			for range workers {
				wg.Go(func() {
					for range perWorker {
						id := worker.NewMessage("n", "", "", nil).ID
						if got, err := s.IncrementAttempt(context.Background(), id, sub); err != nil || got != 1 {
							failed.Add(1)
							firstErr.Do(func() { t.Errorf("IncrementAttempt = %d, %v; want 1", got, err) })
						}
					}
				})
			}
			wg.Wait()
			if n := failed.Load(); n > 0 {
				t.Fatalf("%d of %d first attempts failed", n, workers*perWorker)
			}
		})
	}
}

// TestIntegrationConcurrentDuplicateAttempts runs deliveries of the same new message at once, as a
// redelivery racing the first delivery does. Every attempt counts once: the deliveries of a message
// get the counts 1 to n between them.
func TestIntegrationConcurrentDuplicateAttempts(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "inbox_events")
			s := storage(d, db)

			const deliveries, messages = 8, 25
			want := make([]int, deliveries)
			for i := range want {
				want[i] = i + 1
			}
			for range messages {
				id := worker.NewMessage("n", "", "", nil).ID
				start := make(chan struct{})
				counts := make(chan int, deliveries)
				var wg sync.WaitGroup
				for range deliveries {
					wg.Go(func() {
						<-start
						got, err := s.IncrementAttempt(context.Background(), id, sub)
						if err != nil {
							t.Errorf("IncrementAttempt: %v", err)
						}
						counts <- got
					})
				}
				close(start)
				wg.Wait()
				close(counts)
				got := slices.Sorted(func(yield func(int) bool) {
					for c := range counts {
						if !yield(c) {
							return
						}
					}
				})
				if !slices.Equal(got, want) {
					t.Fatalf("deliveries of one message got the counts %v, want %v", got, want)
				}
			}
		})
	}
}

// TestIntegrationDeleteBefore checks the cleanup on the database clock. MySQL runs in a session time
// zone behind UTC, which deletes rows early when they are stamped in local time.
func TestIntegrationDeleteBefore(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.Open(t)
			d.Reset(t, db, "inbox_events")
			s := storage(d, db)
			ctx := context.Background()

			finish := map[string]func(ctx context.Context, id, sub string) error{
				"old-completed": s.MarkCompleted,
				"old-rejected":  s.MarkRejected,
				"old-failed":    s.MarkFailed,
				"new-completed": s.MarkCompleted,
				"old-waiting":   func(context.Context, string, string) error { return nil },
			}
			for id, mark := range finish {
				if _, err := s.IncrementAttempt(ctx, id, sub); err != nil {
					t.Fatal(err)
				}
				if err := mark(ctx, id, sub); err != nil {
					t.Fatalf("finish %s: %v", id, err)
				}
			}
			age := `UPDATE inbox_events SET updated_at = ` + d.Ago(40*24*time.Hour) + ` WHERE message_id LIKE 'old-%'`
			if _, err := db.ExecContext(ctx, age); err != nil {
				t.Fatal(err)
			}

			deleted, err := s.DeleteBefore(ctx, time.Now().Add(-time.Hour), 100)
			if err != nil || deleted != 2 {
				t.Fatalf("DeleteBefore = %d, %v; want the 2 old completed and rejected rows", deleted, err)
			}
			for id := range finish {
				_, ok, err := s.Status(ctx, id, sub)
				if err != nil {
					t.Fatal(err)
				}
				if want := id != "old-completed" && id != "old-rejected"; ok != want {
					t.Fatalf("%s kept = %v, want %v", id, ok, want)
				}
			}

			// A batch stops at limit.
			for _, id := range []string{"a", "b", "c"} {
				if _, err := s.IncrementAttempt(ctx, id, sub); err != nil {
					t.Fatal(err)
				}
				if err := s.MarkCompleted(ctx, id, sub); err != nil {
					t.Fatal(err)
				}
			}
			if deleted, err := s.DeleteBefore(ctx, time.Now().Add(time.Minute), 2); err != nil || deleted != 2 {
				t.Fatalf("DeleteBefore with limit 2 = %d, %v", deleted, err)
			}
		})
	}
}
