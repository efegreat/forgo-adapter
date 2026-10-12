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
