// Package redistest runs the integration tests of the Redis adapters against real Redis servers. A
// test runs once against each server whose address is in ADAPTER_TEST_REDIS_ADDRS, separated by
// commas, and skips without it, so that go test ./... needs no infrastructure:
//
//	ADAPTER_TEST_REDIS_ADDRS=127.0.0.1:6379,127.0.0.1:6380 go test ./...
//
// Use disposable servers: the tests empty the databases they run on.
package redistest

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// env names the addresses of the servers.
const env = "ADAPTER_TEST_REDIS_ADDRS"

// The databases of the packages with Redis integration tests. go test runs packages in parallel,
// and Open empties the database it opens, so every package needs one of its own.
const (
	DBRedisStream = iota + 1
	DBLocker
	DBRateLimit
	DBLiveConfigCache
)

// Server is a database on a Redis server of the integration tests.
type Server struct {
	Addr string
	DB   int
}

// Run runs test once for every server, as a subtest named after its address, on database db. It
// skips t without servers.
func Run(t *testing.T, db int, test func(t *testing.T, s Server)) {
	t.Helper()
	addrs := os.Getenv(env)
	if addrs == "" {
		t.Skip("set " + env + " to run against disposable Redis servers")
	}
	for addr := range strings.SplitSeq(addrs, ",") {
		addr = strings.TrimSpace(addr)
		t.Run(addr, func(t *testing.T) { test(t, Server{Addr: addr, DB: db}) })
	}
}

// Open connects to the database of s and empties it, before t and after it. The client is closed
// when t ends.
func Open(t *testing.T, s Server) *redis.Client {
	t.Helper()
	cl := redis.NewClient(&redis.Options{Addr: s.Addr, DB: s.DB})
	ctx := context.Background()
	if err := cl.FlushDB(ctx).Err(); err != nil {
		_ = cl.Close()
		t.Fatalf("empty database %d of %s: %v", s.DB, s.Addr, err)
	}
	t.Cleanup(func() {
		if err := cl.FlushDB(ctx).Err(); err != nil {
			t.Errorf("empty database %d of %s: %v", s.DB, s.Addr, err)
		}
		_ = cl.Close()
	})
	return cl
}

// AtLeast reports whether the server of cl runs Redis major.minor or newer.
func AtLeast(t *testing.T, cl *redis.Client, major, minor int) bool {
	t.Helper()
	info, err := cl.Info(context.Background(), "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(info) {
		v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:")
		if !ok {
			continue
		}
		parts := strings.SplitN(v, ".", 3)
		if len(parts) < 2 {
			t.Fatalf("redis_version %q", v)
		}
		var got [2]int
		for i := range got {
			if got[i], err = strconv.Atoi(parts[i]); err != nil {
				t.Fatalf("redis_version %q: %v", v, err)
			}
		}
		return got[0] > major || got[0] == major && got[1] >= minor
	}
	t.Fatal("INFO server has no redis_version")
	return false
}
