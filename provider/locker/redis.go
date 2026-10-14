// Package locker implements operation.ILocker on Redis, for the locks of cron jobs, commands and
// messages that must not run twice at once across the instances of an application.
package locker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNotHeld is returned by Unlock when the lock is no longer held by this process: its TTL
// elapsed before the operation finished, and another process may have acquired it since.
var ErrNotHeld = errors.New("locker: lock is not held")

// unlockScript deletes the lock only while it still holds this process's token, so a lock that
// expired and was acquired by another process is never released by this one.
var unlockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`)

// Redis is an operation.ILocker backed by Redis. A lock is a key set with SET NX and a TTL, so a
// crashed process never holds a lock longer than its TTL.
//
// Every lock carries a token that identifies this process. Operations of the same process that
// share a key are already excluded by the lock itself while it is held.
type Redis struct {
	cl    redis.UniversalClient
	token string
}

// NewRedis returns a locker using cl, for forgo.WithProviders.
func NewRedis(cl redis.UniversalClient) *Redis {
	return &Redis{cl: cl, token: rand.Text()}
}

// Lock tries once to acquire key for ttl and reports whether it was acquired.
func (r *Redis) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := r.cl.SetNX(ctx, key, r.token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("locker: lock %s: %w", key, err)
	}
	return ok, nil
}

// WaitLock tries to acquire key up to attempts times, waiting delay between tries but not after the
// last one. It stops early with ctx's error when ctx is done. Fewer than one attempt means one.
func (r *Redis) WaitLock(ctx context.Context, key string, attempts int, delay, ttl time.Duration) (bool, error) {
	attempts = max(attempts, 1)
	for i := range attempts {
		ok, err := r.Lock(ctx, key, ttl)
		if err != nil || ok {
			return ok, err
		}
		if i == attempts-1 {
			break
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
	return false, nil
}

// Unlock releases key. It returns ErrNotHeld when the lock expired before Unlock and leaves the key
// untouched, because another process may hold it now.
func (r *Redis) Unlock(ctx context.Context, key string) error {
	deleted, err := unlockScript.Run(ctx, r.cl, []string{key}, r.token).Int64()
	if err != nil {
		return fmt.Errorf("locker: unlock %s: %w", key, err)
	}
	if deleted == 0 {
		return fmt.Errorf("%w: %s", ErrNotHeld, key)
	}
	return nil
}
