package redisstream

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo-adapter/resource/redisdb"
	"github.com/efegreat/forgo/server/worker"

	"github.com/redis/go-redis/v9"
)

// integrationEnv names the addresses of disposable Redis servers, separated by commas, such as
// 127.0.0.1:6379,127.0.0.1:6380. The integration tests run against each of them and empty its
// database. Without it, they skip.
const integrationEnv = "ADAPTER_TEST_REDIS_ADDRS"

// TestIntegrationBroker runs the broker against real Redis servers, for what the unit tests cannot
// check on miniredis: approximate trimming, delivery counts, consumer idle times, and claims,
// delayed retries, the heartbeat and the sweep on the real clock.
func TestIntegrationBroker(t *testing.T) {
	addrs := os.Getenv(integrationEnv)
	if addrs == "" {
		t.Skip("set " + integrationEnv + " to run against disposable Redis servers")
	}
	for addr := range strings.SplitSeq(addrs, ",") {
		t.Run(addr, func(t *testing.T) {
			integration(t, strings.TrimSpace(addr))
		})
	}
}

// live connects to the Redis server at addr and empties its database, before the test and after it.
func live(t *testing.T, addr string) (*redis.Client, Config) {
	t.Helper()
	cl := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := cl.FlushDB(ctx).Err(); err != nil {
		_ = cl.Close()
		t.Fatalf("flush %s: %v", addr, err)
	}
	t.Cleanup(func() {
		if err := cl.FlushDB(ctx).Err(); err != nil {
			t.Errorf("flush %s: %v", addr, err)
		}
		_ = cl.Close()
	})
	return cl, Config{Name: "events", Redis: redisdb.Config{Addrs: []string{addr}}}
}

// atLeast reports whether the server of cl runs Redis major.minor or newer.
func atLeast(t *testing.T, cl *redis.Client, major, minor int) bool {
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
		var got [2]int
		for i, part := range strings.SplitN(v, ".", 3)[:2] {
			if got[i], err = strconv.Atoi(part); err != nil {
				t.Fatalf("redis_version %q: %v", v, err)
			}
		}
		return got[0] > major || got[0] == major && got[1] >= minor
	}
	t.Fatal("INFO server has no redis_version")
	return false
}

func integration(t *testing.T, addr string) {
	ctx := context.Background()

	t.Run("declare", func(t *testing.T) {
		cl, cfg := live(t, addr)
		for range 2 {
			if err := cl.XAdd(ctx, &redis.XAddArgs{Stream: "orders", Values: []string{PayloadField, "{}"}}).Err(); err != nil {
				t.Fatal(err)
			}
		}
		if err := cl.XGroupCreate(ctx, "orders", "billing", "$").Err(); err != nil {
			t.Fatal(err)
		}
		cfg.Streams = []StreamConfig{
			{Name: "orders", Groups: []GroupConfig{{Name: "billing", StartID: "0"}, {Name: "audit", StartID: "0"}}},
			{Name: "payments", Groups: []GroupConfig{{Name: "ledger"}}},
		}
		open(t, cfg)

		unread := func(stream, group string) int {
			res, err := cl.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "test", Streams: []string{stream, ">"}, Block: -1}).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				t.Fatal(err)
			}
			n := 0
			for _, r := range res {
				n += len(r.Messages)
			}
			return n
		}
		// The existing group keeps its position, the new one starts at its StartID, and the stream
		// is created with its group.
		if got := [3]int{unread("orders", "billing"), unread("orders", "audit"), unread("payments", "ledger")}; got != [3]int{0, 2, 0} {
			t.Fatalf("unread entries of billing, audit and ledger = %v, want [0 2 0]", got)
		}
	})

	t.Run("trims about MaxLen", func(t *testing.T) {
		cl, cfg := live(t, addr)
		cfg.Streams = []StreamConfig{{Name: "orders", MaxLen: 10}}
		b := open(t, cfg)
		const added = 300
		for range added {
			publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
		}
		// Redis trims whole nodes of the stream, so it keeps more than MaxLen.
		if n, err := cl.XLen(ctx, "orders").Result(); err != nil || n < 10 || n >= added {
			t.Fatalf("XLEN = %d, %v; want at least 10 and fewer than %d", n, err, added)
		}
	})

	t.Run("consume and complete", func(t *testing.T) {
		cl, cfg := live(t, addr)
		b := open(t, topology(cfg, SubscriberConfig{}))
		msg := worker.NewMessage("order.placed", "order-1", "orders", []byte(`{"id":1}`))
		msg.Headers.Set("tenant", "acme")
		publish(t, b, msg)

		ch, handle := deliveries()
		consume(t, openSubscriber(t, b, "billing"), 1, handle)
		d := next(t, ch)
		got := d.Message()
		if got.ID != msg.ID || got.Name != msg.Name || string(got.Payload) != `{"id":1}` || got.Key != "order-1" || !got.CreatedAt.Equal(msg.CreatedAt) {
			t.Fatalf("message = %+v, want %+v", got, msg)
		}
		if tenant, _ := got.Headers.Get("tenant"); tenant != "acme" {
			t.Fatalf("tenant header = %q", tenant)
		}
		if err := d.Complete(ctx); err != nil {
			t.Fatal(err)
		}
		if n := len(pending(t, cl)); n != 0 {
			t.Fatalf("%d pending entries after Complete", n)
		}
	})

	t.Run("retry after the delay", func(t *testing.T) {
		cl, cfg := live(t, addr)
		const delay = 300 * time.Millisecond
		b := open(t, topology(cfg, SubscriberConfig{ClaimIdle: time.Second, Retry: RetryConfig{Initial: delay}}))
		msg := worker.NewMessage("order.placed", "", "orders", nil)
		publish(t, b, msg)

		ch, handle := deliveries()
		s := openSubscriber(t, b, "billing")
		consume(t, s, 1, handle)
		d := next(t, ch)
		retried := time.Now()
		if err := d.Retry(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if d = next(t, ch); d.Message().ID != msg.ID {
			t.Fatalf("redelivered %s, want %s", d.Message().ID, msg.ID)
		}
		// A margin for the millisecond idle times of Redis.
		if waited := time.Since(retried); waited < delay-20*time.Millisecond {
			t.Fatalf("redelivered after %s, want about %s", waited, delay)
		}
		// The read and the claim count a delivery each, parking for the retry does not.
		if p := pending(t, cl); len(p) != 1 || p[0].Consumer != s.consumer || p[0].RetryCount != 2 {
			t.Fatalf("pending = %+v, want one entry of %s delivered twice", p, s.consumer)
		}
	})

	t.Run("claim and heartbeat", func(t *testing.T) {
		cl, cfg := live(t, addr)
		b := open(t, topology(cfg, SubscriberConfig{ClaimIdle: time.Second}))
		msg := worker.NewMessage("order.placed", "", "orders", nil)
		publish(t, b, msg)
		// A consumer that crashed after reading the entry.
		if err := cl.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "billing", Consumer: "crashed", Streams: []string{"orders", ">"}, Block: -1}).Err(); err != nil {
			t.Fatal(err)
		}
		read := time.Now()

		ch, release := make(chan worker.IDelivery, 1), make(chan struct{})
		s := openSubscriber(t, b, "billing")
		consume(t, s, 1, func(ctx context.Context, d worker.IDelivery) {
			ch <- d
			<-release
			_ = d.Complete(ctx)
		})
		var once sync.Once
		free := func() { once.Do(func() { close(release) }) }
		t.Cleanup(free) // before the cleanup of consume, which waits for the handler

		if d := next(t, ch); d.Message().ID != msg.ID {
			t.Fatalf("claimed %s, want %s", d.Message().ID, msg.ID)
		}
		if waited := time.Since(read); waited < time.Second-20*time.Millisecond {
			t.Fatalf("claimed after %s, want after ClaimIdle (1s)", waited)
		}
		// The handler runs for twice ClaimIdle, and the heartbeat keeps the entry from the others.
		time.Sleep(2 * time.Second)
		claimed, _, err := cl.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: "orders", Group: "billing", MinIdle: time.Second, Start: claimStart, Count: 10, Consumer: "other"}).Result()
		if err != nil || len(claimed) != 0 {
			t.Fatalf("XAUTOCLAIM = %v, %v; want nothing", claimed, err)
		}
		// The read and the claim count a delivery each, the refreshes do not.
		if p := pending(t, cl); len(p) != 1 || p[0].Consumer != s.consumer || p[0].RetryCount != 2 {
			t.Fatalf("pending = %+v, want one entry of %s delivered twice", p, s.consumer)
		}
		free()
		eventually(t, "the acknowledgement", func() bool { return len(pending(t, cl)) == 0 })
	})

	t.Run("dead letter", func(t *testing.T) {
		cl, cfg := live(t, addr)
		b := open(t, topology(cfg, SubscriberConfig{DeadLetter: DeadLetterConfig{Enabled: true}}))
		msg := worker.NewMessage("order.placed", "", "orders", []byte("{}"))
		publish(t, b, msg)

		ch, handle := deliveries()
		consume(t, openSubscriber(t, b, "billing"), 1, handle)
		if err := next(t, ch).Reject(ctx); err != nil {
			t.Fatal(err)
		}
		if n := len(pending(t, cl)); n != 0 {
			t.Fatalf("%d pending entries after Reject", n)
		}
		dead, err := cl.XRange(ctx, "orders:billing:dlq", "-", "+").Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(dead) != 1 || dead[0].Values[worker.MessageIDHeader] != msg.ID || dead[0].Values[PayloadField] != "{}" {
			t.Fatalf("dead letters = %+v, want a copy of %s", dead, msg.ID)
		}
	})

	t.Run("sweep", func(t *testing.T) {
		cl, cfg := live(t, addr)
		b := open(t, topology(cfg, SubscriberConfig{}))
		publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
		// Consumers of instances that crashed, one of them while it held an entry. Redis records
		// both as active now.
		if err := cl.XGroupCreateConsumer(ctx, "orders", "billing", "crashed").Err(); err != nil {
			t.Fatal(err)
		}
		if err := cl.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "billing", Consumer: "holding", Streams: []string{"orders", ">"}, Block: -1}).Err(); err != nil {
			t.Fatal(err)
		}

		_, handle := deliveries()
		var subs [2]*subscriber
		for i := range subs {
			subs[i] = openSubscriber(t, b, "billing")
			subs[i].staleAfter = 500 * time.Millisecond
			consume(t, subs[i], 1, handle)
		}
		eventually(t, "the sweep", func() bool {
			_, found := consumers(t, cl)["crashed"]
			return !found
		})
		// ClaimIdle is a minute, so the entry of holding stays where it is, and holding with it.
		time.Sleep(time.Second)
		want := map[string]int64{"holding": 1}
		// Since Redis 7.2, the reads and claims of the live consumers keep them active, so no
		// sweep finds them idle. Before, only the entries they get do, and on a quiet group each
		// subscriber sweeps the consumer of the other.
		if atLeast(t, cl, 7, 2) {
			want[subs[0].consumer], want[subs[1].consumer] = 0, 0
		}
		if got := consumers(t, cl); !reflect.DeepEqual(got, want) {
			t.Fatalf("consumers = %v, want %v", got, want)
		}
		// A live consumer gets its next entry, deleted or not.
		publish(t, b, worker.NewMessage("order.placed", "", "orders", nil))
		eventually(t, "the next entry", func() bool {
			got := consumers(t, cl)
			return got[subs[0].consumer]+got[subs[1].consumer] == 1
		})
	})
}
