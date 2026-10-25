package rabbitmq_test

import (
	"os"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

// consumer is a worker.ConsumerConfig whose subscriber is named "billing".
var consumer worker.ConsumerConfig

func ExampleConfig() {
	broker := rabbitmq.Config{
		Name:     "events",
		Addr:     forgo.Env("RABBITMQ_HOST", "localhost"),
		Username: forgo.Env("RABBITMQ_USER", "guest"),
		Password: forgo.Env("RABBITMQ_PASSWORD", "guest"),
		Exchanges: []rabbitmq.ExchangeConfig{
			{Name: "orders", Kind: amqp.ExchangeTopic, Durable: true},
		},
		Queues: []rabbitmq.QueueConfig{
			{
				Name:       "billing",
				Durable:    true,
				Bindings:   []rabbitmq.BindingConfig{{Exchange: "orders", RoutingKey: "order.*"}},
				DeadLetter: rabbitmq.DeadLetterConfig{Enabled: true},
				Retry:      rabbitmq.RetryConfig{Initial: time.Second}, // delayed retries: 1s, 2s, 4s… up to 10m
			},
		},
		Subscribers: []rabbitmq.SubscriberConfig{
			{Name: "billing", Queue: "billing"}, // prefetch defaults to the worker's Concurrency
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
