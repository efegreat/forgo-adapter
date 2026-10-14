package redisstream_test

import (
	"os"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/resource/broker/redisstream"
	"github.com/efegreat/forgo-adapter/resource/redisdb"
)

// consumer is a worker.ConsumerConfig whose subscriber is named "billing".
var consumer worker.ConsumerConfig

func ExampleConfig() {
	broker := redisstream.Config{
		Name:  "events",
		Redis: redisdb.Config{Addrs: []string{forgo.Env("REDIS_ADDR", "localhost:6379")}},
		Streams: []redisstream.StreamConfig{
			{
				Name:   "orders",
				MaxLen: 1_000_000, // trimmed on every publish
				Groups: []redisstream.GroupConfig{{Name: "billing"}, {Name: "shipping"}},
			},
		},
		Subscribers: []redisstream.SubscriberConfig{
			{
				Name:       "billing", // reads the group billing
				Stream:     "orders",
				Retry:      redisstream.RetryConfig{Initial: time.Second}, // delayed retries: 1s, 2s, 4s… up to ClaimIdle
				DeadLetter: redisstream.DeadLetterConfig{Enabled: true},   // orders:billing:dlq
			},
		},
	}

	app := forgo.NewApp(
		forgo.WithName("billing-worker"),
		forgo.WithResources(broker),
		forgo.WithServers(consumer),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}
