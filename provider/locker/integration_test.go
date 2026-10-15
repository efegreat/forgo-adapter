package locker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo-adapter/internal/redistest"
)

// TestIntegrationRedis runs the locker against real Redis servers: SET NX with a TTL, the unlock
// script, expiry on the real clock, and one holder among processes that race for a lock.
func TestIntegrationRedis(t *testing.T) {
	redistest.Run(t, redistest.DBLocker, func(t *testing.T, srv redistest.Server) {
		cl := redistest.Open(t, srv)
		ctx := context.Background()
		l, other := NewRedis(cl), NewRedis(cl)

		t.Run("lock and unlock", func(t *testing.T) {
			if ok, err := l.Lock(ctx, "job", time.Minute); err != nil || !ok {
				t.Fatalf("Lock = %v, %v; want true, nil", ok, err)
			}
			if ttl := cl.PTTL(ctx, "job").Val(); ttl <= 59*time.Second || ttl > time.Minute {
				t.Fatalf("PTTL = %v, want about 1m", ttl)
			}
			if ok, err := other.Lock(ctx, "job", time.Minute); err != nil || ok {
				t.Fatalf("Lock of another process = %v, %v; want false, nil", ok, err)
			}
			if err := other.Unlock(ctx, "job"); !errors.Is(err, ErrNotHeld) {
				t.Fatalf("Unlock of another process = %v, want ErrNotHeld", err)
			}
			if err := l.Unlock(ctx, "job"); err != nil {
				t.Fatalf("Unlock: %v", err)
			}
			if n := cl.Exists(ctx, "job").Val(); n != 0 {
				t.Fatal("Unlock left the key")
			}
		})

		t.Run("expired and taken by another process", func(t *testing.T) {
			// A TTL under a second is set in milliseconds.
			if ok, err := l.Lock(ctx, "expiring", 100*time.Millisecond); err != nil || !ok {
				t.Fatalf("Lock = %v, %v; want true, nil", ok, err)
			}
			time.Sleep(150 * time.Millisecond)
			if ok, err := other.Lock(ctx, "expiring", time.Minute); err != nil || !ok {
				t.Fatalf("Lock of another process after the TTL = %v, %v; want true, nil", ok, err)
			}
			if err := l.Unlock(ctx, "expiring"); !errors.Is(err, ErrNotHeld) {
				t.Fatalf("Unlock = %v, want ErrNotHeld", err)
			}
			if n := cl.Exists(ctx, "expiring").Val(); n != 1 {
				t.Fatal("Unlock released the lock of another process")
			}
		})

		t.Run("one holder at a time", func(t *testing.T) {
			var wg sync.WaitGroup
			var held atomic.Int32
			for range 20 {
				lk := NewRedis(cl)
				wg.Go(func() {
					ok, err := lk.Lock(ctx, "race", time.Minute)
					if err != nil {
						t.Error(err)
					}
					if ok {
						held.Add(1)
					}
				})
			}
			wg.Wait()
			if n := held.Load(); n != 1 {
				t.Fatalf("%d of 20 processes hold the lock, want 1", n)
			}
		})

		t.Run("wait for a release", func(t *testing.T) {
			if ok, err := other.Lock(ctx, "held", time.Minute); err != nil || !ok {
				t.Fatalf("Lock of another process = %v, %v", ok, err)
			}
			time.AfterFunc(50*time.Millisecond, func() { _ = other.Unlock(ctx, "held") })
			if ok, err := l.WaitLock(ctx, "held", 100, 10*time.Millisecond, time.Minute); err != nil || !ok {
				t.Fatalf("WaitLock = %v, %v; want true, nil", ok, err)
			}
		})
	})
}
