# forgo-adapter

**Ready-made infrastructure for [forgo](https://github.com/efegreat/forgo): databases, brokers, storages, locks and rate limits you configure instead of write.**

[Forgo](https://github.com/efegreat/forgo) defines contracts such as `worker.IBroker`, `worker.InboxStorage`, `operation.ILocker` and `http.IRateLimiter`, and never depends on a concrete database, broker or cache. This library implements those contracts for common infrastructure. Resources are config values, and providers are constructors you hand to the bootstrap.

```go
app := forgo.NewApp(
	forgo.WithName("shop-worker"),
	forgo.WithLogger(config.Logger),
	forgo.WithResources(
		sqldb.Config{Name: "main", DriverName: "postgres", DSN: forgo.Env("DATABASE_URL", "")}, // *sql.DB
		redisdb.Config{Name: "cache", Addr: forgo.Env("REDIS_ADDR", "localhost:6379")},     // *redis.Client
		rabbitmq.Config{                                                                     // worker.IBroker
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
		inbox.NewPostgres,  // worker.InboxStorage
		outbox.NewPostgres, // worker.OutboxStorage
		locker.NewRedis,    // operation.ILocker
		ratelimit.NewRedis, // http.IRateLimiter
	),
	forgo.WithServers(config.Consumer, config.Producer),
)
app.Run()
```

## What's in the box

| Package | Kind | Provides | Backed by |
| --- | --- | --- | --- |
| `resource/sqldb` | resource | `*sql.DB` | any `database/sql` driver |
| `resource/redisdb` | resource | `*redis.Client` | go-redis v9 |
| `resource/broker/rabbitmq` | resource | `worker.IBroker` | amqp091-go, with optional dead-letter queues |
| `provider/inbox` | provider | `worker.InboxStorage` | PostgreSQL, MySQL 8 |
| `provider/outbox` | provider | `worker.OutboxStorage` | PostgreSQL, MySQL 8 |
| `provider/locker` | provider | `operation.ILocker` | Redis |
| `provider/ratelimit` | provider | `http.IRateLimiter` | Redis, GCRA |

- **Configure, don't wire.** Every resource is a plain config value whose zero fields mean "use the default". Every provider is a constructor that the bootstrap calls with the resources it needs.
- **Fail at startup.** Resources connect and verify the connection in `New`. RabbitMQ declares its whole topology there too, so a bad DSN or a conflicting queue stops the app before it serves anything.
- **Shared state is atomic.** The Redis locker and rate limiter run each check as a single Lua script, and the rate limiter uses the Redis server's clock, so every instance of your app enforces the same lock or limit.
- **Replaceable.** Each adapter implements a forgo interface. Swap one out by giving the bootstrap your own resource or provider.

## Installation

forgo-adapter requires Go 1.27 or newer. forgo and forgo-adapter are private and untagged until their first release, so point your `go.mod` at local checkouts of both:

```
require (
	github.com/efegreat/forgo v0.0.0
	github.com/efegreat/forgo-adapter v0.0.0
)

replace (
	github.com/efegreat/forgo => ../forgo
	github.com/efegreat/forgo-adapter => ../forgo-adapter
)
```

Once they are released, drop the `replace` directives and run `go get github.com/efegreat/forgo-adapter`. While the repositories stay private, set `GOPRIVATE=github.com/efegreat/*` so Go fetches them with your git credentials instead of the public proxy.

Import the SQL driver you use in `main`, for example `_ "github.com/lib/pq"`.

## Documentation

- **[REFERENCE.md](REFERENCE.md)** is the complete usage guide: every config field and default, how each adapter maps onto forgo, dead-letter queues, lock and rate-limit semantics.
- **[AGENTS.md](AGENTS.md)** is the guide for contributors and coding agents working on this library.
- forgo's own [REFERENCE.md](https://github.com/efegreat/forgo/blob/main/REFERENCE.md) covers the worker, the storage contracts and table schemas, locks and the rate limit middleware that these adapters plug into.
