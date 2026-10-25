package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo/ratelimit"

	"github.com/efegreat/forgo-adapter/redis/internal/redistest"
)

// TestIntegrationRedis runs the limiter against real Redis servers: the GCRA script in Redis's own
// Lua, on the server's clock, the expiry of a key once its burst recovered, and atomic counting
// under concurrent requests.
func TestIntegrationRedis(t *testing.T) {
	redistest.Run(t, redistest.DBRateLimit, func(t *testing.T, srv redistest.Server) {
		cl := redistest.Open(t, srv)
		l := NewRedis(cl)
		ctx := context.Background()
		allow := func(t *testing.T, key string, rate ratelimit.Rate) ratelimit.Result {
			t.Helper()
			res, err := l.Allow(ctx, key, rate)
			if err != nil {
				t.Fatalf("Allow: %v", err)
			}
			return res
		}

		t.Run("burst, reject, refill", func(t *testing.T) {
			const emission = 200 * time.Millisecond
			rate := ratelimit.Rate{Limit: 2, Period: 2 * emission}
			if res := allow(t, "client", rate); !res.Allowed || res.Remaining != 1 || res.ResetAfter != emission {
				t.Fatalf("first request = %+v, want allowed with 1 remaining, reset after %s", res, emission)
			}
			if res := allow(t, "client", rate); !res.Allowed || res.Remaining != 0 || res.ResetAfter <= emission || res.ResetAfter > 2*emission {
				t.Fatalf("second request = %+v, want allowed with none remaining", res)
			}
			res := allow(t, "client", rate)
			if res.Allowed || res.RetryAfter <= 0 || res.RetryAfter > emission {
				t.Fatalf("third request = %+v, want rejected, retry within %s", res, emission)
			}
			if ttl := cl.PTTL(ctx, KeyPrefix+"client").Val(); ttl <= 0 || ttl > 2*emission {
				t.Fatalf("PTTL = %v, want the time until the burst recovers", ttl)
			}
			time.Sleep(res.RetryAfter + 10*time.Millisecond)
			if res := allow(t, "client", rate); !res.Allowed {
				t.Fatalf("request after RetryAfter = %+v, want allowed", res)
			}
		})

		t.Run("key expires once recovered", func(t *testing.T) {
			allow(t, "recovering", ratelimit.Rate{Limit: 1, Period: 100 * time.Millisecond})
			time.Sleep(150 * time.Millisecond)
			if n := cl.Exists(ctx, KeyPrefix+"recovering").Val(); n != 0 {
				t.Fatal("the key outlived its burst")
			}
		})

		t.Run("concurrent requests", func(t *testing.T) {
			rate := ratelimit.Rate{Limit: 10, Period: time.Minute}
			var wg sync.WaitGroup
			var allowed atomic.Int32
			for range 50 {
				wg.Go(func() {
					res, err := l.Allow(ctx, "busy", rate)
					if err != nil {
						t.Error(err)
					}
					if res.Allowed {
						allowed.Add(1)
					}
				})
			}
			wg.Wait()
			if n := allowed.Load(); n != 10 {
				t.Fatalf("%d of 50 concurrent requests allowed, want the limit, 10", n)
			}
		})
	})
}
