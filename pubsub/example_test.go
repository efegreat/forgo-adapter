package pubsub_test

import (
	"os"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/pubsub"
)

// consumer is a worker.ConsumerConfig whose subscriber is named "billing".
var consumer worker.ConsumerConfig

func ExampleConfig() {
	broker := pubsub.Config{
		Name:      "events",
		ProjectID: forgo.Env("GOOGLE_CLOUD_PROJECT", ""),
		Topics:    []pubsub.TopicConfig{{Name: "orders"}},
		Subscriptions: []pubsub.SubscriptionConfig{
			{
				Name:       "billing",
				Topic:      "orders",
				Retry:      pubsub.RetryConfig{Initial: time.Second}, // redeliveries back off from 1s up to 10m
				DeadLetter: pubsub.DeadLetterConfig{Enabled: true},   // billing.dlq topic and subscription
			},
		},
		Subscribers: []pubsub.SubscriberConfig{
			{Name: "billing", Subscription: "billing"}, // MaxOutstandingMessages defaults to the worker's Concurrency
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
