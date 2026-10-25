// Package ratelimit implements forgo's ratelimit.ILimiter on Redis, so that the limits of the servers'
// RateLimitWith middlewares hold across every instance of an application.
//
// See the [ratelimit] section of REFERENCE.md.
//
// [ratelimit]: https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#ratelimit
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/efegreat/forgo/ratelimit"

	"github.com/redis/go-redis/v9"
)

// KeyPrefix is prepended to every key the limiter stores in Redis.
const KeyPrefix = "ratelimit:"

var errInvalidRate = errors.New("ratelimit: limit and period must be positive")

// allowScript runs the generic cell rate algorithm (GCRA) atomically on the stored theoretical
// arrival time (TAT) of the next request, in microseconds. It is the algorithm of
// forgo's ratelimit.Memory, so a limit means the same with either limiter.
//
// The clock is the Redis server's TIME, so every application instance sees the same time
// regardless of clock skew between them. The key expires once its burst has fully recovered,
// because an untracked key behaves exactly the same.
//
// ARGV[1] is the emission interval and ARGV[2] the burst offset, both in microseconds.
// It returns {allowed, remaining, retry after µs, reset after µs}.
var allowScript = redis.NewScript(`
if redis.replicate_commands then
	redis.replicate_commands()
end

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])
local emission = tonumber(ARGV[1])
local burst_offset = tonumber(ARGV[2])

local tat = tonumber(redis.call('GET', KEYS[1]))
if not tat or tat < now then
	tat = now
end

local next_tat = tat + emission
local diff = now - (next_tat - burst_offset)
if diff < 0 then
	return {0, 0, -diff, tat - now}
end

local reset = next_tat - now
redis.call('SET', KEYS[1], string.format('%.0f', next_tat), 'PX', math.ceil(reset / 1000))
return {1, math.floor(diff / emission), 0, reset}
`)

// Redis is a forgo ratelimit.ILimiter that counts in Redis, so a limit holds across every instance of
// the application. Each key stores a single timestamp. Use it with the RateLimitWith middleware of the
// HTTP or the gRPC server.
type Redis struct {
	cl redis.UniversalClient
}

// NewRedis returns a rate limiter using cl, for forgo.WithProviders.
func NewRedis(cl redis.UniversalClient) *Redis {
	return &Redis{cl: cl}
}

// Allow counts a request under key at rate, in one script on Redis. When Redis fails, Allow returns the
// error, and the FailOpen of the middleware's config decides whether the request is admitted.
func (r *Redis) Allow(ctx context.Context, key string, rate ratelimit.Rate) (ratelimit.Result, error) {
	if rate.Limit <= 0 || rate.Period <= 0 {
		return ratelimit.Result{}, errInvalidRate
	}
	burst := rate.Burst
	if burst <= 0 {
		burst = rate.Limit
	}
	// Redis TIME has microsecond resolution, so rates above one request per microsecond per key
	// are counted as one per microsecond.
	emission := max(rate.Period/time.Duration(rate.Limit), time.Microsecond).Microseconds()
	burstOffset := emission * int64(burst)

	res, err := allowScript.Run(ctx, r.cl, []string{KeyPrefix + key}, emission, burstOffset).Int64Slice()
	if err != nil {
		return ratelimit.Result{}, fmt.Errorf("ratelimit: allow: %w", err)
	}
	if len(res) != 4 {
		return ratelimit.Result{}, fmt.Errorf("ratelimit: allow: unexpected reply %v", res)
	}

	return ratelimit.Result{
		Allowed:    res[0] == 1,
		Remaining:  int(res[1]),
		RetryAfter: time.Duration(res[2]) * time.Microsecond,
		ResetAfter: time.Duration(res[3]) * time.Microsecond,
	}, nil
}
