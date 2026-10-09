package redisdb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestNew(t *testing.T) {
	m := miniredis.RunT(t)

	res, err := Config{Name: "cache", Addr: m.Addr()}.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer res.Close()

	if res.Name() != "cache" {
		t.Fatalf("Name() = %q", res.Name())
	}
	// Providers request redis.UniversalClient; the bootstrap matches it by assignability.
	cl, ok := res.Value().(redis.UniversalClient)
	if !ok {
		t.Fatalf("Value() = %T, want a redis.UniversalClient", res.Value())
	}
	if err := cl.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Get("k"); got != "v" {
		t.Fatalf("stored %q, want v", got)
	}
}

func TestNewPingFailure(t *testing.T) {
	_, err := Config{Addr: "127.0.0.1:1", ConnectTimeout: time.Second}.New()
	if err == nil || !strings.HasPrefix(err.Error(), "redisdb: ping 127.0.0.1:1") {
		t.Fatalf("New err = %v", err)
	}
}

func TestSanitize(t *testing.T) {
	got := Config{}.Sanitize()
	if got.Addr != "localhost:6379" || got.ConnectTimeout != 5*time.Second {
		t.Fatalf("Sanitize() = %+v", got)
	}
}
