# forgo-adapter

**Ready-made infrastructure for [forgo](https://github.com/efegreat/forgo): databases, brokers, storages, locks and rate limits you configure instead of write.**

[![Go Reference](https://pkg.go.dev/badge/github.com/efegreat/forgo-adapter.svg)](https://pkg.go.dev/github.com/efegreat/forgo-adapter)
[![CI](https://github.com/efegreat/forgo-adapter/actions/workflows/ci.yml/badge.svg)](https://github.com/efegreat/forgo-adapter/actions/workflows/ci.yml)
[![Go 1.27+](https://img.shields.io/badge/go-1.27%2B-00ADD8)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

[Forgo](https://github.com/efegreat/forgo) defines contracts such as `worker.IBroker`, `worker.InboxStorage`, `operation.ILocker`, `ratelimit.ILimiter`, `forgo.LiveConfigStore` and `forgo.LiveConfigCache`, and never depends on a concrete database, broker or cache. This library implements those contracts for common infrastructure. Resources are config values, and providers are constructors you hand to the bootstrap.

```go
app := forgo.NewApp(
	forgo.WithName("shop-worker"),
	forgo.WithLogger(config.Logger),
	forgo.WithResources(
		sqldb.Config{Name: "main", DriverName: "postgres", DSN: forgo.Env("DATABASE_URL", "")},    // *sql.DB
		redisdb.Config{Name: "cache", Addrs: []string{forgo.Env("REDIS_ADDR", "localhost:6379")}}, // redis.UniversalClient
		rabbitmq.Config{                                                                           // worker.IBroker
			Name:      "events",
			Exchanges: []rabbitmq.ExchangeConfig{{Name: "orders", Kind: "topic", Durable: true}},
			Queues: []rabbitmq.QueueConfig{{
				Name:       "billing",
				Durable:    true,
				Bindings:   []rabbitmq.BindingConfig{{Exchange: "orders", RoutingKey: "order.*"}},
				DeadLetter: rabbitmq.DeadLetterConfig{Enabled: true}, // billing.dlx → billing.dlq
			}},
			Subscribers: []rabbitmq.SubscriberConfig{{Name: "billing", Queue: "billing"}},
		},
	),
	forgo.WithProviders(
		sqltx.NewTransactor, // operation.Transactor: handlers and their inbox updates commit together
		inbox.NewPostgres,   // worker.InboxStorage
		outbox.NewPostgres,  // worker.OutboxStorage
		locker.NewRedis,     // operation.ILocker
		ratelimit.NewRedis,  // forgo ratelimit.ILimiter
	),
	forgo.WithServers(config.Consumer, config.Producer),
)
if err := app.Run(); err != nil { // logged; main picks the exit code
	os.Exit(1)
}
```

## What's in the box

| Package | Kind | Provides | Backed by |
| --- | --- | --- | --- |
| `resource/sqldb` | resource | `*sql.DB` | any `database/sql` driver, with an optional query log that flags slow statements and masks arguments |
| `resource/redisdb` | resource | `redis.UniversalClient` | go-redis v9: a server, Redis Cluster or Sentinel |
| `resource/broker/rabbitmq` | resource | `worker.IBroker` | amqp091-go, with automatic recovery, publisher confirms, prefetch and optional dead-letter queues |
| `resource/broker/pubsub` | resource | `worker.IBroker` | Google Cloud Pub/Sub client v2, with batched publishing, flow control, retry policies and optional dead-letter topics |
| `resource/broker/redisstream` | resource | `worker.IBroker` | Redis Streams on go-redis v9, with consumer groups, claiming of abandoned entries, delayed retries and optional dead-letter streams |
| `provider/sqltx` | provider | `operation.Transactor` | `database/sql`, transactions carried in the context |
| `provider/inbox` | provider | `worker.InboxStorage` | PostgreSQL, MySQL 8 |
| `provider/outbox` | provider | `worker.OutboxStorage` | PostgreSQL, MySQL 8 |
| `provider/locker` | provider | `operation.ILocker` | Redis |
| `provider/ratelimit` | provider | `ratelimit.ILimiter` | Redis, GCRA |
| `provider/liveconfig` | provider | `forgo.LiveConfigStore`, `forgo.LiveConfigCache` | PostgreSQL, MySQL 8; Redis |

- **Configure, don't wire.** Every resource is a plain config value whose zero fields mean "use the default". Every provider is a constructor that the bootstrap calls with the resources it needs.
- **Fail at startup.** Resources connect and verify the connection in `New`. RabbitMQ declares its whole topology there too, Pub/Sub creates and checks the topics and subscriptions you list, and Redis Streams creates its consumer groups, so a bad DSN, a conflicting queue or a changed subscription stops the app before it serves anything.
- **Nothing is lost quietly.** Handlers and their inbox updates commit in one transaction. Publishes wait for the broker's confirm before the outbox marks them sent. RabbitMQ reconnects on its own, and acknowledgements never cross a reconnect onto the wrong message.
- **Shared state is atomic.** The Redis locker and rate limiter run each check as a single Lua script, and the rate limiter uses the Redis server's clock, so every instance of your app enforces the same lock or limit.
- **Replaceable.** Each adapter implements a forgo interface. Swap one out by giving the bootstrap your own resource or provider.

## Installation

forgo-adapter requires Go 1.27 or newer.

```sh
go get github.com/efegreat/forgo-adapter
```

Import the SQL driver you use in `main`, for example `_ "github.com/lib/pq"`.

## Documentation

- **[REFERENCE.md](REFERENCE.md)** is the complete usage guide: every config field and default, how each adapter maps onto forgo, dead-letter queues, lock and rate-limit semantics.
- **[pkg.go.dev](https://pkg.go.dev/github.com/efegreat/forgo-adapter)** has the API documentation of every package, with examples.
- **[AGENTS.md](AGENTS.md)** is the guide for contributors and coding agents working on this library.
- forgo's own [REFERENCE.md](https://github.com/efegreat/forgo/blob/main/REFERENCE.md) covers the worker, the storage contracts and table schemas, locks and the rate limit middleware that these adapters plug into.

## Contributing

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md), and report vulnerabilities privately as [SECURITY.md](SECURITY.md) describes.

## License

forgo-adapter is released under the [MIT License](LICENSE).
