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

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "server", cfg: Config{Name: "cache", Addrs: []string{m.Addr()}}, want: "*redis.Client"},
		{name: "cluster", cfg: Config{Name: "cache", Addrs: []string{m.Addr()}, ClusterMode: true}, want: "*redis.ClusterClient"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tt.cfg.New()
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
			if got := typeName(cl); got != tt.want {
				t.Fatalf("Value() = %s, want %s", got, tt.want)
			}
			if err := cl.Set(context.Background(), "k", tt.name, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if got, _ := m.Get("k"); got != tt.name {
				t.Fatalf("stored %q, want %s", got, tt.name)
			}
		})
	}
}

func TestClient(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		want     string
		failover bool
	}{
		{name: "one address", cfg: Config{Addrs: []string{"a:6379"}}, want: "*redis.Client"},
		{name: "several addresses", cfg: Config{Addrs: []string{"a:6379", "b:6379"}}, want: "*redis.ClusterClient"},
		{name: "cluster mode", cfg: Config{Addrs: []string{"a:6379"}, ClusterMode: true}, want: "*redis.ClusterClient"},
		{name: "sentinel", cfg: Config{Addrs: []string{"a:26379", "b:26379"}, MasterName: "main"}, want: "*redis.Client", failover: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clients connect lazily, so building one never dials the addresses.
			cl := tt.cfg.Sanitize().client()
			defer cl.Close()

			if got := typeName(cl); got != tt.want {
				t.Fatalf("client() = %s, want %s", got, tt.want)
			}
			// go-redis names the address of a failover client's options "FailoverClient".
			if c, ok := cl.(*redis.Client); ok && (c.Options().Addr == "FailoverClient") != tt.failover {
				t.Fatalf("client() failover = %v, want %v", !tt.failover, tt.failover)
			}
		})
	}
}

func TestNewKeepsAddrs(t *testing.T) {
	m := miniredis.RunT(t)

	// Spare capacity that an append by the cluster client would write into.
	addrs := make([]string, 1, 4)
	addrs[0] = m.Addr()
	backing := addrs[:cap(addrs)]

	res, err := Config{Addrs: addrs, ClusterMode: true}.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer res.Close()
	if err := res.Value().(redis.UniversalClient).Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	for i, addr := range backing[1:] {
		if addr != "" {
			t.Fatalf("Addrs backing array [%d] = %q, want it untouched", i+1, addr)
		}
	}
}

func TestNewPingFailure(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "server", cfg: Config{Addrs: []string{"127.0.0.1:1"}}, want: "redisdb: ping 127.0.0.1:1: "},
		{name: "cluster", cfg: Config{Addrs: []string{"127.0.0.1:1", "127.0.0.1:2"}}, want: "redisdb: ping 127.0.0.1:1,127.0.0.1:2: "},
		{name: "sentinel", cfg: Config{Addrs: []string{"127.0.0.1:1"}, MasterName: "main"}, want: "redisdb: ping main via 127.0.0.1:1: "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.ConnectTimeout = time.Second
			_, err := tt.cfg.New()
			if err == nil || !strings.HasPrefix(err.Error(), tt.want) {
				t.Fatalf("New err = %v, want prefix %q", err, tt.want)
			}
		})
	}
}

func TestNewInvalid(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "empty address", cfg: Config{Addrs: []string{"a:6379", ""}}, want: "redisdb: Addrs[1] is empty"},
		{name: "cluster mode with sentinel", cfg: Config{MasterName: "main", ClusterMode: true}, want: "redisdb: ClusterMode and MasterName cannot be combined"},
		{name: "cluster mode with DB", cfg: Config{ClusterMode: true, DB: 1}, want: "redisdb: DB 1 is not supported by Redis Cluster"},
		{name: "several addresses with DB", cfg: Config{Addrs: []string{"a:6379", "b:6379"}, DB: 2}, want: "redisdb: DB 2 is not supported by Redis Cluster"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validation runs before connecting: an error from dialing would start with "redisdb: ping".
			_, err := tt.cfg.New()
			if err == nil || !strings.HasPrefix(err.Error(), tt.want) {
				t.Fatalf("New err = %v, want prefix %q", err, tt.want)
			}
		})
	}

	// A failover client selects DB on the master, so DB is valid with several Sentinels.
	if err := (Config{Addrs: []string{"a:26379", "b:26379"}, MasterName: "main", DB: 1}).Sanitize().validate(); err != nil {
		t.Fatalf("validate sentinel with DB: %v", err)
	}
}

func TestSanitize(t *testing.T) {
	got := Config{}.Sanitize()
	if len(got.Addrs) != 1 || got.Addrs[0] != "localhost:6379" || got.ConnectTimeout != 5*time.Second {
		t.Fatalf("Sanitize() = %+v", got)
	}
}

func typeName(v any) string {
	switch v.(type) {
	case *redis.Client:
		return "*redis.Client"
	case *redis.ClusterClient:
		return "*redis.ClusterClient"
	default:
		return "other"
	}
}
