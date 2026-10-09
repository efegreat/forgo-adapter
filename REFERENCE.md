# forgo-adapter Reference

The complete guide to the forgo adapter library. New here? Start with the [README](README.md). The contracts these adapters implement are documented in forgo's [REFERENCE.md](https://github.com/efegreat/forgo/blob/main/REFERENCE.md).

## Contents

- [Overview](#overview)
- [Installation](#installation)
- [Resources](#resources)
  - [sqldb](#sqldb)
  - [redisdb](#redisdb)
  - [rabbitmq](#rabbitmq)
- [Providers](#providers)
  - [sqltx](#sqltx)
  - [inbox and outbox](#inbox-and-outbox)
  - [locker](#locker)
  - [ratelimit](#ratelimit)
- [A complete worker app](#a-complete-worker-app)
- [Errors](#errors)

---

## Overview

Forgo's bootstrap builds **resources** first, then calls **providers** in declaration order (see [Resources and providers](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#resources-and-providers)). Adapters fit that model:

- A **resource** is a config value implementing `forgo.ResourceFactory`. Its `New` opens the connection, verifies it and returns a `forgo.Resource` that the bootstrap closes on shutdown. Put it in `forgo.WithResources`.
- A **provider** is a constructor such as `inbox.NewPostgres(db *sql.DB) *inbox.Postgres`. The bootstrap resolves its parameters from earlier bindings. Put it in `forgo.WithProviders`, after the resources it needs.

The bootstrap matches by assignability. A provider that asks for `redis.UniversalClient` receives the `*redis.Client` of `redisdb`, and the worker servers asking for `worker.InboxStorage` receive the `*inbox.Postgres` returned by `inbox.NewPostgres`. You never name the interface yourself.

| Package | Import path | Kind | Value or result | Needs |
| --- | --- | --- | --- | --- |
| `sqldb` | `github.com/efegreat/forgo-adapter/resource/sqldb` | resource | `*sql.DB` | a `database/sql` driver |
| `redisdb` | `github.com/efegreat/forgo-adapter/resource/redisdb` | resource | `*redis.Client` | |
| `rabbitmq` | `github.com/efegreat/forgo-adapter/resource/broker/rabbitmq` | resource | `worker.IBroker` | |
| `sqltx` | `github.com/efegreat/forgo-adapter/provider/sqltx` | provider | `*Transactor` → `operation.Transactor` | `*sql.DB` |
| `inbox` | `github.com/efegreat/forgo-adapter/provider/inbox` | provider | `*Postgres`, `*Mysql` → `worker.InboxStorage` | `*sql.DB` |
| `outbox` | `github.com/efegreat/forgo-adapter/provider/outbox` | provider | `*Postgres`, `*Mysql` → `worker.OutboxStorage` | `*sql.DB` |
| `locker` | `github.com/efegreat/forgo-adapter/provider/locker` | provider | `*Redis` → `operation.ILocker` | `redis.UniversalClient` |
| `ratelimit` | `github.com/efegreat/forgo-adapter/provider/ratelimit` | provider | `*Redis` → `http.IRateLimiter` | `redis.UniversalClient` |

`redis` in this document is `github.com/redis/go-redis/v9`, and `amqp` is `github.com/rabbitmq/amqp091-go`.

---

## Installation

Go 1.27 or newer. forgo and forgo-adapter are private and untagged until their first release, so point your `go.mod` at local checkouts of both:

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

---

## Resources

### sqldb

A `database/sql` connection pool. Providers request `*sql.DB`.

```go
import (
	_ "github.com/lib/pq" // the driver, in main

	"github.com/efegreat/forgo-adapter/resource/sqldb"
)

var DB = sqldb.Config{
	Name:         "main",
	DriverName:   "postgres",
	DSN:          forgo.Env("DATABASE_URL", "postgresql://user:password@localhost/app?sslmode=disable"),
	MaxOpenConns: 20,
}
```

| Field | Default | Description |
| --- | --- | --- |
| `Name` | | The resource name, for `boot:"name"` tags. |
| `DriverName` | | The registered driver, such as `postgres` or `mysql`. |
| `DSN` | | Passed to the driver. |
| `MaxOpenConns` | unlimited | Maximum open connections. |
| `MaxIdleConns` | `5` | Maximum idle connections. |
| `ConnMaxLifetime` | `1h` | Connections older than this are closed. |
| `ConnMaxIdleTime` | `10m` | Connections idle for longer than this are closed. |
| `ConnectTimeout` | `5s` | Bounds the startup ping. |

`New` opens the pool, applies the limits and pings the database. A failed ping closes the pool and fails the bootstrap with `sqldb: ping <name>: …`.

**Several databases.** Two `sqldb` resources both bind `*sql.DB`, so a provider asking for `*sql.DB` is ambiguous. Name the one it needs with a dependency struct:

```go
forgo.WithResources(
	sqldb.Config{Name: "primary", DriverName: "postgres", DSN: primaryDSN},
	sqldb.Config{Name: "replica", DriverName: "postgres", DSN: replicaDSN},
),
forgo.WithProviders(
	func(d struct {
		DB *sql.DB `boot:"primary"`
	}) *inbox.Postgres {
		return inbox.NewPostgres(d.DB)
	},
),
```

### redisdb

A go-redis client. Its value is a `*redis.Client`. The providers of this library request `redis.UniversalClient`, which also accepts a cluster or failover client that you expose as your own resource.

```go
import "github.com/efegreat/forgo-adapter/resource/redisdb"

var Redis = redisdb.Config{
	Name:     "cache",
	Addr:     forgo.Env("REDIS_ADDR", "localhost:6379"),
	Password: forgo.Env("REDIS_PASSWORD", ""),
}
```

| Field | Default | Description |
| --- | --- | --- |
| `Name` | | The resource name, for `boot:"name"` tags. |
| `Addr` | `localhost:6379` | `host:port`. |
| `Username`, `Password` | | ACL credentials. |
| `DB` | `0` | Logical database. |
| `PoolSize` | go-redis default (10 per `GOMAXPROCS`) | Maximum socket connections. |
| `DialTimeout`, `ReadTimeout`, `WriteTimeout` | go-redis defaults (5s, 3s, 3s) | Network timeouts. |
| `TLSConfig` | plaintext | Enables TLS when set. |
| `ConnectTimeout` | `5s` | Bounds the startup ping. |

A failed ping closes the client and fails the bootstrap with `redisdb: ping <addr>: …`.

### rabbitmq

A RabbitMQ broker for forgo's [worker](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#worker). Its value is the `worker.IBroker` that both `worker.ConsumerConfig` and `worker.ProducerConfig` resolve.

```go
import "github.com/efegreat/forgo-adapter/resource/broker/rabbitmq"

var Broker = rabbitmq.Config{
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
		},
	},
	Publishers: []rabbitmq.PublisherConfig{
		{Name: "order.placed"}, // persistent by default
	},
	Subscribers: []rabbitmq.SubscriberConfig{
		{Name: "billing", Queue: "billing"}, // prefetch defaults to the worker's Concurrency
	},
}
```

#### Connection and topology

| Field | Default | Description |
| --- | --- | --- |
| `Name` | | The resource name, also shown as the connection name in the RabbitMQ management UI. |
| `Addr` | `localhost` | Broker host. |
| `Port` | `5672` | Broker port. |
| `Username`, `Password` | client default (`guest`) | PLAIN credentials. They are passed to the client directly, so special characters need no escaping. |
| `VHost` | `/` | Virtual host. |
| `Recovery` | enabled, unlimited retries every 5s | Automatic reconnection, see [Recovery](#recovery). |
| `Exchanges` | | Exchanges to declare: `Name`, `Kind`, `Durable`, `AutoDelete`, `Internal`, `NoWait`, `Args`. |
| `Queues` | | Queues to declare: `Name`, `Durable`, `AutoDelete`, `Exclusive`, `NoWait`, `Args`, `Bindings` and `DeadLetter`. |
| `Publishers` | | Named publishers, see below. |
| `Subscribers` | | Named subscribers, see below. |

`New` connects, then declares every exchange, then every queue with its bindings. A queue's dead-letter exchange and queue are declared just before the queue itself. Any failure closes the connection and fails the bootstrap. The channel the topology was declared on stays open, because the client only redeclares the topology recorded on open channels after a reconnect. Declarations are idempotent, but redeclaring an existing exchange or queue with different properties or arguments is refused by RabbitMQ with `PRECONDITION_FAILED`.

#### Publishers and subscribers

The names match forgo's worker configs:

- `worker.ProducerConfig.Publishers[i].Name` → `IBroker.NewPublisher(name)` → the `PublisherConfig` with that `Name`. The producer's default publisher uses `NewDefaultPublisher`, which publishes persistent messages and needs no config.
- `worker.SubscriberConfig.Name` → `IBroker.NewSubscriber(name)` → the `SubscriberConfig` with that `Name`, which consumes `Queue`.

An unknown name, or a subscriber whose `Queue` does not exist, fails the server at startup.

| `PublisherConfig` field | Default | Description |
| --- | --- | --- |
| `Name` | | Matches `worker.PublisherConfig.Name`. |
| `Mandatory` | `false` | Fail the publish when no queue is bound to receive the message. Without it, RabbitMQ drops unroutable messages and the publish succeeds. |
| `DeliveryMode` | `amqp.Persistent` | `amqp.Transient` for messages that may be lost on a broker restart. |

Every publisher channel is in confirm mode. `Publish` waits, up to the worker's `PublisherConfig.Timeout`, until the broker confirms it has taken responsibility for the message, which for a persistent message on a durable queue means it is written to disk. Only then does the outbox mark the message processed. A publish fails, and the message stays in the outbox for the next cycle, when:

- the broker nacks it (`rabbitmq: broker nacked the message`), for example because a queue with `x-overflow: reject-publish` is full,
- no confirm arrives in time, or the channel is lost meanwhile,
- `Mandatory` is set and no queue receives it (`rabbitmq: mandatory message is unroutable`).

A publisher sends one message at a time and waits for its confirm. Raise the worker publisher's `Concurrency` for throughput: each unit is its own channel.

| `SubscriberConfig` field | Default | Description |
| --- | --- | --- |
| `Name` | | Matches `worker.SubscriberConfig.Name`. |
| `Queue` | | The queue to consume. It must exist when the server starts. |
| `Prefetch` | the worker subscriber's `Concurrency` | How many unacknowledged messages RabbitMQ sends this consumer. The default keeps every handler slot busy and leaves the rest of the backlog in the queue for other instances. Raise it a little to hide network latency. A negative value removes the limit. At most 65535. |
| `AutoAck` | `false` | Leave it off: the worker acknowledges every message itself. |
| `Exclusive`, `NoLocal`, `NoWait`, `Args` | | AMQP consume options. |

The worker's `SubscriberConfig.Concurrency` bounds how many messages are handled at once.

#### Messages on the wire

A `worker.Message` is published to the exchange `Message.Destination` with the routing key `Message.Key`:

| AMQP | From |
| --- | --- |
| body | `Message.Payload`, `content_type: application/json` |
| `message_id` property, `x-msg-id` header | `Message.ID` |
| `type` property, `x-msg-name` header | `Message.Name` |
| `timestamp` property, `x-msg-timestamp` header (Unix nanoseconds) | `Message.CreatedAt`, when set |
| headers | `Message.Headers` |

Consuming reverses the mapping with `worker.NewMessageFromDelivery`: `Key` is the routing key and `Destination` the exchange the message was published to. Messages from other producers keep their identity when they set the `message_id` and `type` properties instead of the `x-msg-*` headers. Without either, the ID is generated on every delivery, so the consumer inbox cannot recognize a redelivery. Only string header values are kept.

The worker acknowledges each delivery:

| Worker | AMQP | Effect |
| --- | --- | --- |
| `Complete` | `basic.ack` | The message is removed. |
| `Retry` | `basic.nack`, requeue | The message goes back to the queue and is delivered again. |
| `Reject` | `basic.reject`, no requeue | The message is dropped, or dead-lettered when the queue has `DeadLetter`. |

An acknowledgement for a message whose channel was lost since it was delivered is not sent. It fails with `rabbitmq: delivery belongs to a lost channel`, because the broker already requeued the message and will deliver it again (see [Recovery](#recovery)).

#### Dead-letter queues

Messages the worker rejects are lost unless the queue has a dead-letter queue. Enable one per queue:

```go
rabbitmq.QueueConfig{
	Name:       "billing",
	Durable:    true,
	DeadLetter: rabbitmq.DeadLetterConfig{Enabled: true},
}
```

This declares a durable direct exchange `billing.dlx` and a durable queue `billing.dlq` bound to it with the key `billing`. It then declares `billing` with the arguments `x-dead-letter-exchange: billing.dlx` and `x-dead-letter-routing-key: billing`.

| `DeadLetterConfig` field | Default | Description |
| --- | --- | --- |
| `Enabled` | `false` | Declares the dead-letter exchange and queue and points the queue at them. |
| `Exchange` | `<queue>.dlx` | The dead-letter exchange. Several queues may share one. |
| `Queue` | `<queue>.dlq` | The queue that collects dead letters. |
| `RoutingKey` | `<queue>` | Binds `Queue` to `Exchange` and replaces the routing key of dead-lettered messages. |
| `Args` | | Arguments of the dead-letter queue, such as `x-message-ttl` to expire old dead letters or `x-queue-type: quorum`. |

**What ends up there.** Every message the worker rejects:

- a message that failed `MaxAttempt` times (the inbox marks it *failed*),
- a message whose payload cannot be bound (`msg_bind`, inbox *rejected*),
- a message no handler is registered for (`nf_mh`).

RabbitMQ also dead-letters messages that expire through `x-message-ttl` or overflow a queue with `x-overflow: reject-publish-dlx`. Messages that are retried are requeued and stay in the queue.

Dead letters keep their body, properties and headers. RabbitMQ adds an `x-death` header recording the source queue, the reason and a count. To replay messages, consume `<queue>.dlq` with another subscriber or use a shovel. Because a replayed message keeps its `x-msg-id`, the consumer inbox skips it as already *failed* or *rejected*. Delete its inbox row first to process it again.

**Validation.** `New` fails before connecting when a queue enables `DeadLetter` without a `Name`, or also sets `x-dead-letter-exchange` or `x-dead-letter-routing-key` in its `Args`. Use one or the other.

**Existing queues.** Queue arguments cannot change after a queue is declared. Enabling `DeadLetter` on a queue that already exists without it fails with `PRECONDITION_FAILED`. Either delete and recreate the queue, or leave `DeadLetter` off and set the dead-letter exchange through a RabbitMQ policy.

#### Recovery

The broker uses the automatic recovery of amqp091-go. When the connection or a channel is lost, the client reconnects and reopens every channel with its prefetch and confirm mode. It then redeclares the exchanges, queues, bindings and dead-letter queues, and resubscribes the consumers. Subscribers keep consuming from the same delivery channel, so the worker does not notice beyond a pause.

```go
rabbitmq.Config{
	Recovery: rabbitmq.RecoveryConfig{MaxRetries: 60, RetryInterval: 2 * time.Second},
}
```

| `RecoveryConfig` field | Default | Description |
| --- | --- | --- |
| `Disabled` | `false` | Turns recovery off. A lost connection then stops every subscriber and fails every publish until the app restarts. |
| `MaxRetries` | unlimited | Reconnection attempts before giving up. Once they are exhausted, the connection closes and subscribers stop with `rabbitmq: delivery channel closed by the broker`. |
| `RetryInterval` | `5s` | Wait before each attempt, plus up to 500ms of jitter. |

While the connection is down:

- **Publishes fail fast.** The outbox keeps the messages and retries them on its next cycle.
- **Unacknowledged deliveries are requeued by the broker.** Delivery tags start over on a recovered channel, so a late acknowledgement from a handler that was still running would settle a different message. The broker marks those deliveries stale before the channel is reopened, and their acknowledgements fail instead (`rabbitmq: delivery belongs to a lost channel`). The message is delivered again, and the inbox recognizes it by its `x-msg-id`. If the handler had completed it, the redelivery is acknowledged and skipped.
- **Unconfirmed publishes fail.** The client settles pending confirms as nacks when it reopens the channel, so the outbox retries them. A message the broker had already stored is then published twice, and the consumer inbox drops the duplicate.

The client logs its recovery through `amqp.Logger`, which discards everything by default. Call `amqp.SetLogger` in `main` to see reconnects.

---

## Providers

### sqltx

`sqltx.Transactor` is the `operation.Transactor` the worker consumer runs message handlers through. `sqltx` carries the transaction in the context, so every storage of this library and every repository of yours that runs its statements through `sqltx.From` joins it.

```go
import "github.com/efegreat/forgo-adapter/provider/sqltx"

forgo.WithProviders(
	sqltx.NewTransactor, // operation.Transactor on the *sql.DB
	inbox.NewPostgres,
	postgresrepo.NewOrderRepository,
)
```

```go
func (r *OrderRepository) Insert(ctx context.Context, o order.Order) error {
	_, err := sqltx.From(ctx, r.db).ExecContext(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, o.ID, o.Total)
	return err
}
```

| API | Behavior |
| --- | --- |
| `NewTransactor(db) *Transactor` | Provider. `Do(ctx, fn)` is `Run(ctx, db, fn)`. |
| `Run(ctx, db, fn) error` | Begins a transaction on `db` and calls `fn` with a context carrying it. Commits when `fn` returns nil. Rolls back when `fn` returns an error, which `Run` returns unchanged, or panics, after which the panic continues. |
| `From(ctx, db) Executor` | The transaction on `db` that `ctx` carries, or `db` itself outside a transaction. `Executor` has `ExecContext`, `QueryContext` and `QueryRowContext`. |

**Nesting.** A `Run` or `Do` inside another on the same database joins the outer transaction instead of starting one, and the outermost call commits or rolls back everything. Transactions are keyed by `*sql.DB`, so contexts can carry transactions on several databases at once, and `From` never hands one database's transaction to another.

**In the worker.** For each message, the consumer runs your handler and the inbox's `MarkCompleted` inside `Transactor.Do`. With `sqltx` and the SQL inbox, your writes and the completion commit together. A handler that fails rolls back both, and the message is retried. A crash after the commit leads to a redelivery that the inbox skips.

**In your services.** Write outbox rows with `sqltx.From` inside the same `Do` as the business change, as in forgo's [producer example](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#producer-outbox-relay). The row is then published only if the change commits.

### inbox and outbox

SQL implementations of forgo's [storage contracts](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#storage-contracts) for the worker consumer (inbox) and producer (outbox). They use the `inbox_events` and `outbox_events` tables whose schema is given there. Create them with your migrations, for example with forgo's `migrate` command.

```go
import (
	"github.com/efegreat/forgo-adapter/provider/inbox"
	"github.com/efegreat/forgo-adapter/provider/outbox"
)

forgo.WithProviders(
	inbox.NewPostgres,  // or inbox.NewMysql
	outbox.NewPostgres, // or outbox.NewMysql
)
```

| Constructor | Database | Notes |
| --- | --- | --- |
| `inbox.NewPostgres(*sql.DB)` | PostgreSQL | `INSERT … ON CONFLICT` increments attempts in one statement. |
| `inbox.NewMysql(*sql.DB)` | MySQL 8 | Locks the row with `SELECT … FOR UPDATE`. Timestamps are `CURRENT_TIMESTAMP(6)`, so use `DATETIME(6)` columns. |
| `outbox.NewPostgres(*sql.DB)` | PostgreSQL | Claims with `FOR UPDATE SKIP LOCKED` and stamps `processing_at` with the database clock. |
| `outbox.NewMysql(*sql.DB)` | MySQL 8 | Claims with `FOR UPDATE SKIP LOCKED` and stamps `processing_at` with the application clock. |

Status changes are idempotent. Moving a message into the state it is already in succeeds. Moving it out of a terminal state fails with `inbox.ErrNotWaiting`. Several producer instances can run against one outbox, because a claim skips the rows another claim has locked.

Every statement runs through `sqltx.From`, so a storage called inside `sqltx.Transactor.Do` joins that transaction. The operations that need several statements (MySQL `IncrementAttempt`, both `Claim`s and `Complete`s) run in `sqltx.Run`, which starts a transaction of their own or joins the caller's.

### locker

`locker.Redis` is an `operation.ILocker`. It enables `Lock` on cron jobs, CLI commands and message handlers (see [Locks](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#locks)).

```go
import "github.com/efegreat/forgo-adapter/provider/locker"

forgo.WithResources(config.Redis),
forgo.WithProviders(locker.NewRedis),
```

| Method | Behavior |
| --- | --- |
| `Lock(key, ttl)` | `SET key token NX PX ttl`. Returns whether the lock was acquired. |
| `WaitLock(ctx, key, attempts, delay, ttl)` | Tries up to `attempts` times (at least once), waiting `delay` between tries but not after the last one. Returns `ctx.Err()` when `ctx` ends first. |
| `Unlock(key)` | Deletes the key only while it still holds this process's token. Otherwise returns `locker.ErrNotHeld` and leaves the key alone. |

The token is random per `locker.Redis`, so one instance per process. A lock that expires while its operation still runs can be taken by another process. The late `Unlock` then fails with `ErrNotHeld` instead of releasing the other process's lock, and forgo's `Locker` middleware logs that failure at critical severity. A TTL that is too short for the operation shows up in your logs this way. Size `Lock.Ttl` to the longest expected run.

Keys are used as given. forgo builds them from your `Lock.Key`, for example `<Key>-<subscriber>-<messageID>` for message handlers.

### ratelimit

`ratelimit.Redis` is an `http.IRateLimiter`. Use it with `http.RateLimitWith` to enforce a rate limit across every instance of your API (see [Rate limiting](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#rate-limiting)).

```go
import "github.com/efegreat/forgo-adapter/provider/ratelimit"

forgo.WithResources(config.Redis),
forgo.WithProviders(ratelimit.NewRedis),

// on a route or the server
forgo.MiddlewareAs(http.RateLimitWith(http.RateLimitConfig{Limit: 5, Period: time.Minute, Prefix: "login:"}))
```

It implements the generic cell rate algorithm (GCRA), the algorithm of `http.MemoryRateLimiter`, so a `Rate` means the same with either limiter: `Limit` requests per `Period`, up to `Burst` at once, refilled continuously.

- **Atomic.** Each `Allow` is one Lua script that reads and writes a single key, so concurrent requests from any number of instances are counted exactly. It works on Redis Cluster.
- **One clock.** Time comes from the Redis server's `TIME`, so clock skew between app instances does not change the limit.
- **Small.** A key stores one timestamp and expires when its burst has fully recovered.
- **Keys.** Stored as `ratelimit:<Prefix><key>` (`ratelimit.KeyPrefix`). Give every `RateLimitWith` middleware its own `Prefix`.
- **Resolution.** Microseconds. Rates above one request per microsecond per key are counted as one per microsecond.
- **Failures.** A Redis error is returned to the middleware. By default the request fails with `rate_limit_unavailable` (503). With `RateLimitConfig.FailOpen` it is admitted and the failure is logged.

---

## A complete worker app

```go
// config/config.go
var (
	DB     = sqldb.Config{Name: "main", DriverName: "postgres", DSN: forgo.Env("DATABASE_URL", "")}
	Redis  = redisdb.Config{Name: "cache", Addr: forgo.Env("REDIS_ADDR", "localhost:6379")}
	Broker = rabbitmq.Config{
		Name:      "events",
		Exchanges: []rabbitmq.ExchangeConfig{{Name: "orders", Kind: amqp.ExchangeTopic, Durable: true}},
		Queues: []rabbitmq.QueueConfig{{
			Name:       "billing",
			Durable:    true,
			Bindings:   []rabbitmq.BindingConfig{{Exchange: "orders", RoutingKey: "order.*"}},
			DeadLetter: rabbitmq.DeadLetterConfig{Enabled: true, Args: map[string]any{"x-message-ttl": 7 * 24 * 3600 * 1000}},
		}},
		Publishers:  []rabbitmq.PublisherConfig{{Name: "order.placed"}},
		Subscribers: []rabbitmq.SubscriberConfig{{Name: "billing", Queue: "billing"}},
	}

	Adapters = []any{
		sqltx.NewTransactor,
		inbox.NewPostgres,
		outbox.NewPostgres,
		locker.NewRedis,
	}

	Consumer = worker.ConsumerConfig{
		Subscribers: []worker.SubscriberConfig{{
			Name:        "billing", // rabbitmq.SubscriberConfig.Name
			Concurrency: 8,
			MaxAttempt:  5, // then rejected → billing.dlq
			Handlers: []worker.HandlerConfig{{
				Name:    "order.placed", // runs in a sqltx transaction with its inbox completion
				Lock:    operation.Lock{Key: "order-placed", Ttl: time.Minute}, // locker.Redis
				Handler: worker.HandlerFor((*billing.Service).OnOrderPlaced),
			}},
		}},
	}

	Producer = worker.ProducerConfig{
		Publishers: []worker.PublisherConfig{{Name: "order.placed", Concurrency: 4}}, // rabbitmq.PublisherConfig.Name
	}
)

// cmd/worker/main.go
func main() {
	forgo.NewApp(
		forgo.WithName("shop-worker"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB, config.Redis, config.Broker),
		forgo.WithProviders(config.Adapters...),
		forgo.WithProviders(config.Services...),
		forgo.WithServers(config.Consumer, config.Producer),
	).Run()
}
```

---

## Errors

Every error is prefixed with its package and wraps its cause, so `errors.Is` and `errors.As` see the driver error.

| Prefix | Raised by |
| --- | --- |
| `sqldb:` | Opening or pinging the database. |
| `redisdb:` | Pinging Redis. |
| `rabbitmq:` | Validation, connection, topology, channels, unknown publisher or subscriber names and missing queues, a closed delivery channel, nacked and unroutable publishes, acknowledgements of deliveries from a lost channel. |
| `sqltx:` | Beginning, committing or rolling back a transaction. Errors returned by your function pass through unchanged. |
| `inbox:` | Inbox queries. `inbox.ErrNotWaiting` when a message left the waiting status. |
| `outbox:` | Outbox queries. |
| `locker:` | Redis errors. `locker.ErrNotHeld` from `Unlock`. |
| `ratelimit:` | An invalid `Rate` and Redis errors. |
