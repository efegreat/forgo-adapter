package redisdb

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
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

func TestNewReadFrom(t *testing.T) {
	tests := []struct {
		readFrom ReadFrom
		readOnly bool
	}{
		{readFrom: ReadFromMaster},
		{readFrom: ReadFromReplica, readOnly: true},
		{readFrom: ReadFromNearest, readOnly: true},
		{readFrom: ReadFromAny, readOnly: true},
	}
	for _, tt := range tests {
		t.Run(string(tt.readFrom), func(t *testing.T) {
			m := miniredis.RunT(t)
			// A cluster client that reads from replicas sends READONLY on connect, which miniredis
			// lacks.
			var readonly atomic.Int32
			if err := m.Server().Register("READONLY", func(c *server.Peer, cmd string, args []string) {
				readonly.Add(1)
				c.WriteOK()
			}); err != nil {
				t.Fatal(err)
			}

			res, err := Config{Addrs: []string{m.Addr()}, ClusterMode: true, ReadFrom: tt.readFrom}.New()
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer res.Close()

			// The master is the only node of the miniredis cluster, so reads fall back to it.
			cl := res.Value().(redis.UniversalClient)
			ctx := context.Background()
			if err := cl.Set(ctx, "k", "v", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if got, err := cl.Get(ctx, "k").Result(); err != nil || got != "v" {
				t.Fatalf("Get = %q, %v, want v", got, err)
			}
			if got := readonly.Load() > 0; got != tt.readOnly {
				t.Fatalf("READONLY sent = %v, want %v", got, tt.readOnly)
			}
		})
	}
}

func TestClient(t *testing.T) {
	sentinels := []string{"a:26379", "b:26379"}
	tests := []struct {
		name string
		cfg  Config
		want shape
	}{
		{name: "one address", cfg: Config{Addrs: []string{"a:6379"}}, want: shape{kind: "server"}},
		{name: "one address ignores ReadFrom", cfg: Config{Addrs: []string{"a:6379"}, ReadFrom: ReadFromReplica}, want: shape{kind: "server"}},
		{name: "several addresses", cfg: Config{Addrs: []string{"a:6379", "b:6379"}}, want: shape{kind: "cluster"}},
		{name: "cluster mode", cfg: Config{Addrs: []string{"a:6379"}, ClusterMode: true}, want: shape{kind: "cluster"}},
		{name: "cluster from replica", cfg: Config{ClusterMode: true, ReadFrom: ReadFromReplica}, want: shape{kind: "cluster", readOnly: true}},
		{name: "cluster from nearest", cfg: Config{ClusterMode: true, ReadFrom: ReadFromNearest}, want: shape{kind: "cluster", readOnly: true, byLatency: true}},
		{name: "cluster from any", cfg: Config{ClusterMode: true, ReadFrom: ReadFromAny}, want: shape{kind: "cluster", readOnly: true, randomly: true}},
		{name: "sentinel", cfg: Config{Addrs: sentinels, MasterName: "main"}, want: shape{kind: "failover"}},
		{name: "sentinel from replica", cfg: Config{Addrs: sentinels, MasterName: "main", ReadFrom: ReadFromReplica}, want: shape{kind: "failover cluster", readOnly: true}},
		{name: "sentinel from nearest", cfg: Config{Addrs: sentinels, MasterName: "main", ReadFrom: ReadFromNearest}, want: shape{kind: "failover cluster", readOnly: true, byLatency: true}},
		{name: "sentinel from any", cfg: Config{Addrs: sentinels, MasterName: "main", ReadFrom: ReadFromAny}, want: shape{kind: "failover cluster", readOnly: true, randomly: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clients connect lazily, so building one never dials the addresses.
			cl := tt.cfg.Sanitize().client()
			defer cl.Close()

			if got := shapeOf(cl); got != tt.want {
				t.Fatalf("client() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// shape is what a test can observe of a client without connecting.
type shape struct {
	kind                          string
	readOnly, byLatency, randomly bool
}

func shapeOf(cl redis.UniversalClient) shape {
	switch c := cl.(type) {
	case *redis.Client:
		// go-redis names the address of a failover client's options "FailoverClient".
		if c.Options().Addr == "FailoverClient" {
			return shape{kind: "failover"}
		}
		return shape{kind: "server"}
	case *redis.ClusterClient:
		o := c.Options()
		kind := "cluster"
		// A failover cluster client reads its slots from the Sentinels instead of CLUSTER SLOTS.
		if o.ClusterSlots != nil {
			kind = "failover cluster"
		}
		return shape{kind: kind, readOnly: o.ReadOnly, byLatency: o.RouteByLatency, randomly: o.RouteRandomly}
	default:
		return shape{kind: fmt.Sprintf("%T", cl)}
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
		{name: "unknown ReadFrom", cfg: Config{ReadFrom: "replicas"}, want: `redisdb: unknown ReadFrom "replicas"`},
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

	// Failover clients select DB on the master, so DB is valid with several Sentinels, also when
	// reads go to replicas.
	for _, readFrom := range []ReadFrom{ReadFromMaster, ReadFromReplica} {
		cfg := Config{Addrs: []string{"a:26379", "b:26379"}, MasterName: "main", ReadFrom: readFrom, DB: 1}
		if err := cfg.Sanitize().validate(); err != nil {
			t.Fatalf("validate sentinel with DB, ReadFrom %s: %v", readFrom, err)
		}
	}
}

func TestSanitize(t *testing.T) {
	got := Config{}.Sanitize()
	if len(got.Addrs) != 1 || got.Addrs[0] != "localhost:6379" || got.ReadFrom != ReadFromMaster || got.ConnectTimeout != 5*time.Second {
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
