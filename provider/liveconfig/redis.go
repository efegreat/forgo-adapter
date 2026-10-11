package liveconfig

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// KeyPrefix is prepended to the name of every live configuration cached in Redis.
const KeyPrefix = "liveconfig:"

// setScript caches a document unless the entry holds the same or a newer version, atomically. The entry is
// a hash of version and data, without expiry. ARGV[1] is the version and ARGV[2] the document. Versions are
// compared as Lua numbers, exact up to 2^53.
var setScript = redis.NewScript(`
local current = tonumber(redis.call('HGET', KEYS[1], 'version'))
if current and current >= tonumber(ARGV[1]) then
	return 0
end
redis.call('HSET', KEYS[1], 'version', ARGV[1], 'data', ARGV[2])
return 1
`)

// RedisCache is a forgo.LiveConfigCache on Redis, shared by every instance of the application.
type RedisCache struct {
	cl redis.UniversalClient
}

// NewRedisCache returns a live config cache using cl, for forgo.WithProviders.
func NewRedisCache(cl redis.UniversalClient) *RedisCache {
	return &RedisCache{cl: cl}
}

func (r *RedisCache) Get(ctx context.Context, name string) ([]byte, bool, error) {
	data, err := r.cl.HGet(ctx, KeyPrefix+name, "data").Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("liveconfig: cache get %q: %w", name, err)
	}
	return data, true, nil
}

func (r *RedisCache) Set(ctx context.Context, name string, data []byte, version uint64) error {
	if err := setScript.Run(ctx, r.cl, []string{KeyPrefix + name}, version, data).Err(); err != nil {
		return fmt.Errorf("liveconfig: cache set %q: %w", name, err)
	}
	return nil
}
