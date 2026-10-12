package locker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	m := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: m.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = cl.Close() })
	return cl, m
}

func TestRedisLockUnlock(t *testing.T) {
	ctx := context.Background()
	cl, m := newTestClient(t)
	l := NewRedis(cl)

	ok, err := l.Lock(ctx, "job", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Lock = %v, %v; want true, nil", ok, err)
	}
	if ttl := m.TTL("job"); ttl != time.Minute {
		t.Fatalf("TTL = %v, want 1m", ttl)
	}

	ok, err = l.Lock(ctx, "job", time.Minute)
	if err != nil || ok {
		t.Fatalf("second Lock = %v, %v; want false, nil", ok, err)
	}

	if err := l.Unlock(ctx, "job"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if m.Exists("job") {
		t.Fatal("Unlock left the key")
	}

	ok, err = l.Lock(ctx, "job", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Lock after Unlock = %v, %v; want true, nil", ok, err)
	}
}

func TestRedisUnlockNotHeld(t *testing.T) {
	tests := []struct {
		name string
		// after runs once the lock is acquired, before Unlock
		after       func(m *miniredis.Miniredis, other *Redis)
		keyRemains  bool
		otherHolder bool
	}{
		{
			name:  "expired",
			after: func(m *miniredis.Miniredis, _ *Redis) { m.FastForward(2 * time.Second) },
		},
		{
			name: "expired and taken by another process",
			after: func(m *miniredis.Miniredis, other *Redis) {
				m.FastForward(2 * time.Second)
				if ok, err := other.Lock(context.Background(), "job", time.Minute); err != nil || !ok {
					panic("other process could not lock")
				}
			},
			keyRemains: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cl, m := newTestClient(t)
			l, other := NewRedis(cl), NewRedis(cl)

			if ok, err := l.Lock(ctx, "job", time.Second); err != nil || !ok {
				t.Fatalf("Lock = %v, %v", ok, err)
			}
			tt.after(m, other)

			if err := l.Unlock(ctx, "job"); !errors.Is(err, ErrNotHeld) {
				t.Fatalf("Unlock err = %v, want ErrNotHeld", err)
			}
			if m.Exists("job") != tt.keyRemains {
				t.Fatalf("key exists = %v, want %v", m.Exists("job"), tt.keyRemains)
			}
		})
	}
}

// TestRedisContext checks that Lock and Unlock run on the caller's context, so that a canceled
// operation does not wait for Redis.
func TestRedisContext(t *testing.T) {
	cl, _ := newTestClient(t)
	l := NewRedis(cl)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := l.Lock(ctx, "job", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("Lock err = %v, want context.Canceled", err)
	}
	if err := l.Unlock(ctx, "job"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Unlock err = %v, want context.Canceled", err)
	}
}

func TestRedisWaitLock(t *testing.T) {
	ctx := context.Background()
	cl, _ := newTestClient(t)
	l, other := NewRedis(cl), NewRedis(cl)

	t.Run("free", func(t *testing.T) {
		ok, err := l.WaitLock(ctx, "free", 3, time.Hour, time.Minute)
		if err != nil || !ok {
			t.Fatalf("WaitLock = %v, %v; want true, nil", ok, err)
		}
	})

	if ok, err := other.Lock(ctx, "held", time.Minute); err != nil || !ok {
		t.Fatalf("other Lock = %v, %v", ok, err)
	}

	t.Run("held, no wait after the last attempt", func(t *testing.T) {
		const delay = 50 * time.Millisecond
		start := time.Now()
		ok, err := l.WaitLock(ctx, "held", 2, delay, time.Minute)
		if err != nil || ok {
			t.Fatalf("WaitLock = %v, %v; want false, nil", ok, err)
		}
		if elapsed := time.Since(start); elapsed >= 2*delay {
			t.Fatalf("WaitLock took %v, want one delay (%v) for two attempts", elapsed, delay)
		}
	})

	t.Run("zero attempts tries once", func(t *testing.T) {
		ok, err := l.WaitLock(ctx, "once", 0, time.Hour, time.Minute)
		if err != nil || !ok {
			t.Fatalf("WaitLock = %v, %v; want true, nil", ok, err)
		}
	})

	t.Run("context canceled while waiting", func(t *testing.T) {
		waitCtx, cancel := context.WithCancel(ctx)
		time.AfterFunc(20*time.Millisecond, cancel)
		ok, err := l.WaitLock(waitCtx, "held", 3, time.Hour, time.Minute)
		if ok || !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitLock = %v, %v; want false, context.Canceled", ok, err)
		}
	})

	t.Run("context canceled before", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		ok, err := l.WaitLock(canceled, "free-again", 3, time.Hour, time.Minute)
		if ok || !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitLock = %v, %v; want false, context.Canceled", ok, err)
		}
	})

	t.Run("released while waiting", func(t *testing.T) {
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = other.Unlock(ctx, "held")
		}()
		ok, err := l.WaitLock(ctx, "held", 50, 5*time.Millisecond, time.Minute)
		if err != nil || !ok {
			t.Fatalf("WaitLock = %v, %v; want true, nil", ok, err)
		}
	})
}

func TestRedisStoreError(t *testing.T) {
	ctx := context.Background()
	cl, m := newTestClient(t)
	l := NewRedis(cl)
	m.Close()

	if _, err := l.Lock(ctx, "job", time.Minute); err == nil {
		t.Fatal("Lock succeeded without Redis")
	}
	if err := l.Unlock(ctx, "job"); err == nil || errors.Is(err, ErrNotHeld) {
		t.Fatalf("Unlock err = %v, want a store error", err)
	}
}

func TestRedisImplementsILocker(t *testing.T) {
	var _ operation.ILocker = (*Redis)(nil)
}
