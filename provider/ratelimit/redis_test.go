package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/http"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLimiter(t *testing.T) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	m := miniredis.RunT(t)
	m.SetTime(time.Unix(1_700_000_000, 0))
	cl := redis.NewClient(&redis.Options{Addr: m.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = cl.Close() })
	return NewRedis(cl), m
}

func TestRedisAllow(t *testing.T) {
	type step struct {
		advance time.Duration // moves the Redis clock before the request
		want    http.RateLimitResult
	}
	tests := []struct {
		name  string
		rate  http.Rate
		steps []step
	}{
		{
			name: "burst then reject then refill",
			rate: http.Rate{Limit: 2, Period: time.Second},
			steps: []step{
				{want: http.RateLimitResult{Allowed: true, Remaining: 1, ResetAfter: 500 * time.Millisecond}},
				{want: http.RateLimitResult{Allowed: true, Remaining: 0, ResetAfter: time.Second}},
				{want: http.RateLimitResult{Allowed: false, RetryAfter: 500 * time.Millisecond, ResetAfter: time.Second}},
				{advance: 500 * time.Millisecond, want: http.RateLimitResult{Allowed: true, Remaining: 0, ResetAfter: time.Second}},
			},
		},
		{
			name: "burst smaller than limit",
			rate: http.Rate{Limit: 10, Period: time.Second, Burst: 1},
			steps: []step{
				{want: http.RateLimitResult{Allowed: true, Remaining: 0, ResetAfter: 100 * time.Millisecond}},
				{advance: 40 * time.Millisecond, want: http.RateLimitResult{Allowed: false, RetryAfter: 60 * time.Millisecond, ResetAfter: 60 * time.Millisecond}},
				{advance: 60 * time.Millisecond, want: http.RateLimitResult{Allowed: true, Remaining: 0, ResetAfter: 100 * time.Millisecond}},
			},
		},
		{
			name: "partial recovery rounds remaining down",
			rate: http.Rate{Limit: 2, Period: time.Second},
			steps: []step{
				{want: http.RateLimitResult{Allowed: true, Remaining: 1, ResetAfter: 500 * time.Millisecond}},
				{advance: 250 * time.Millisecond, want: http.RateLimitResult{Allowed: true, Remaining: 0, ResetAfter: 750 * time.Millisecond}},
			},
		},
		{
			name: "fully recovered key starts over",
			rate: http.Rate{Limit: 1, Period: time.Minute},
			steps: []step{
				{want: http.RateLimitResult{Allowed: true, Remaining: 0, ResetAfter: time.Minute}},
				{advance: 2 * time.Minute, want: http.RateLimitResult{Allowed: true, Remaining: 0, ResetAfter: time.Minute}},
			},
		},
		{
			name: "rate above one per microsecond is clamped",
			rate: http.Rate{Limit: 10_000_000, Period: time.Second, Burst: 2},
			steps: []step{
				{want: http.RateLimitResult{Allowed: true, Remaining: 1, ResetAfter: time.Microsecond}},
				{want: http.RateLimitResult{Allowed: true, Remaining: 0, ResetAfter: 2 * time.Microsecond}},
				{want: http.RateLimitResult{Allowed: false, RetryAfter: time.Microsecond, ResetAfter: 2 * time.Microsecond}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, m := newTestLimiter(t)
			now := time.Unix(1_700_000_000, 0)
			for i, s := range tt.steps {
				now = now.Add(s.advance)
				m.SetTime(now)
				got, err := l.Allow(context.Background(), "client", tt.rate)
				if err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
				if got != s.want {
					t.Fatalf("step %d: got %+v, want %+v", i, got, s.want)
				}
			}
		})
	}
}

func TestRedisAllowStoresPrefixedKeyUntilRecovered(t *testing.T) {
	l, m := newTestLimiter(t)

	if _, err := l.Allow(context.Background(), "login:1.2.3.4", http.Rate{Limit: 2, Period: time.Second}); err != nil {
		t.Fatal(err)
	}

	if !m.Exists(KeyPrefix + "login:1.2.3.4") {
		t.Fatalf("key %q not stored", KeyPrefix+"login:1.2.3.4")
	}
	if ttl := m.TTL(KeyPrefix + "login:1.2.3.4"); ttl != 500*time.Millisecond {
		t.Fatalf("TTL = %v, want the time until the burst recovers (500ms)", ttl)
	}
}

func TestRedisAllowCountsKeysSeparately(t *testing.T) {
	l, _ := newTestLimiter(t)
	rate := http.Rate{Limit: 1, Period: time.Minute}

	for _, key := range []string{"a", "b"} {
		res, err := l.Allow(context.Background(), key, rate)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Allowed {
			t.Fatalf("first request of key %q rejected", key)
		}
	}
	res, err := l.Allow(context.Background(), "a", rate)
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Fatal("second request of key a admitted")
	}
}

func TestRedisAllowInvalidRate(t *testing.T) {
	l, _ := newTestLimiter(t)
	for _, rate := range []http.Rate{
		{Limit: 0, Period: time.Second},
		{Limit: 1, Period: 0},
		{Limit: -1, Period: time.Second},
	} {
		if _, err := l.Allow(context.Background(), "k", rate); !errors.Is(err, errInvalidRate) {
			t.Fatalf("rate %+v: err = %v, want errInvalidRate", rate, err)
		}
	}
}

func TestRedisAllowStoreError(t *testing.T) {
	l, m := newTestLimiter(t)
	m.Close()

	if _, err := l.Allow(context.Background(), "k", http.Rate{Limit: 1, Period: time.Second}); err == nil {
		t.Fatal("Allow succeeded without Redis")
	}
}

func TestRedisImplementsIRateLimiter(t *testing.T) {
	var _ http.IRateLimiter = (*Redis)(nil)
}
