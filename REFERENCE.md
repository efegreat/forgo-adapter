# forgo-adapter Reference

The complete guide to the forgo adapter library. New here? Start with the [README](README.md). The contracts these adapters implement are documented in forgo's [REFERENCE.md](https://github.com/efegreat/forgo/blob/main/REFERENCE.md).

## Contents

- [Overview](#overview)
- [Installation](#installation)
- [Resources](#resources)
  - [sqldb](#sqldb)
  - [redisdb](#redisdb)
  - [rabbitmq](#rabbitmq)
  - [pubsub](#pubsub)
  - [redisstream](#redisstream)
- [Providers](#providers)
  - [sqltx](#sqltx)
  - [inbox and outbox](#inbox-and-outbox)
  - [locker](#locker)
  - [ratelimit](#ratelimit)
  - [liveconfig](#liveconfig)
- [Metrics (OpenTelemetry)](#metrics-opentelemetry)
  - [meter](#meter)
  - [observer](#observer)
- [A complete worker app](#a-complete-worker-app)
- [Errors](#errors)

---

## Overview

Forgo's bootstrap builds **resources** first, then calls **providers** in declaration order (see [Resources and providers](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#resources-and-providers)). Adapters fit that model:

- A **resource** is a config value implementing `forgo.ResourceFactory`. Its `New` opens the connection, verifies it and returns a `forgo.Resource` that the bootstrap closes on shutdown. Put it in `forgo.WithResources`.
- A **provider** is a constructor such as `inbox.NewPostgres(db *sql.DB) *inbox.Postgres`. The bootstrap resolves its parameters from earlier bindings. Put it in `forgo.WithProviders`, after the resources it needs.

The bootstrap matches by assignability. A provider that asks for `redis.UniversalClient` receives the `*redis.Client` or `*redis.ClusterClient` of `redisdb`, and the worker servers asking for `worker.InboxStorage` receive the `*inbox.Postgres` returned by `inbox.NewPostgres`. You never name the interface yourself.

| Package | Import path | Kind | Value or result | Needs |
| --- | --- | --- | --- | --- |
| `sqldb` | `github.com/efegreat/forgo-adapter/resource/sqldb` | resource | `*sql.DB` | a `database/sql` driver |
| `redisdb` | `github.com/efegreat/forgo-adapter/resource/redisdb` | resource | `*redis.Client` or `*redis.ClusterClient` → `redis.UniversalClient` | |
| `rabbitmq` | `github.com/efegreat/forgo-adapter/resource/broker/rabbitmq` | resource | `worker.IBroker` | |
| `pubsub` | `github.com/efegreat/forgo-adapter/resource/broker/pubsub` | resource | `worker.IBroker` | |
| `redisstream` | `github.com/efegreat/forgo-adapter/resource/broker/redisstream` | resource | `worker.IBroker` | |
| `sqltx` | `github.com/efegreat/forgo-adapter/provider/sqltx` | provider | `*Transactor` → `operation.Transactor` | `*sql.DB` |
| `inbox` | `github.com/efegreat/forgo-adapter/provider/inbox` | provider | `*Postgres`, `*MySQL` → `worker.InboxStorage` | `*sql.DB` |
| `outbox` | `github.com/efegreat/forgo-adapter/provider/outbox` | provider | `*Postgres`, `*MySQL` → `worker.OutboxStorage`, and `Save` for services | `*sql.DB` |
| `locker` | `github.com/efegreat/forgo-adapter/provider/locker` | provider | `*Redis` → `operation.ILocker` | `redis.UniversalClient` |
| `ratelimit` | `github.com/efegreat/forgo-adapter/provider/ratelimit` | provider | `*Redis` → `ratelimit.ILimiter` | `redis.UniversalClient` |
| `liveconfig` | `github.com/efegreat/forgo-adapter/provider/liveconfig` | provider | `*Postgres`, `*MySQL` → `forgo.LiveConfigStore`; `*RedisCache` → `forgo.LiveConfigCache` | `*sql.DB`; `redis.UniversalClient` |
| `meter` | `github.com/efegreat/forgo-adapter/otel/meter` (module `…/otel`) | resource | `*meter.Provider` → `metric.MeterProvider` | |
| `observer` | `github.com/efegreat/forgo-adapter/otel/observer` (module `…/otel`) | provider | `operation.IObserver` | `metric.MeterProvider` |

`redis` in this document is `github.com/redis/go-redis/v9`, `amqp` is `github.com/rabbitmq/amqp091-go`, and `option` is `google.golang.org/api/option`.

---

## Installation

Go 1.27 or newer.

```sh
go get github.com/efegreat/forgo-adapter
```

The OpenTelemetry metrics are a module of their own, `github.com/efegreat/forgo-adapter/otel`. See [Metrics](#metrics-opentelemetry).

---

## Resources

### sqldb

A `database/sql` connection pool. Providers request `*sql.DB`.

```go
import (
	_ "github.com/lib/pq" // the driver, in main

	"github.com/efegreat/forgo-adapter/resource/sqldb"
)

// DB is a function, not a var, so that tests can set DATABASE_URL before it is read.
func DB() sqldb.Config {
	return sqldb.Config{
		Name:         "main",
		DriverName:   "postgres",
		DSN:          forgo.Env("DATABASE_URL", "postgresql://user:password@localhost/app?sslmode=disable"),
		MaxOpenConns: 20,
	}
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
| `Logger` | off | Logs every statement. See [Query log](#query-log). |

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

#### Query log

`Logger` logs every statement on a line of its own, through `logger.FromCtx` of the context the statement ran with, so the line carries `op_id` and the other fields of the operation that ran it. It is off by default, and the pool then runs on the driver alone.

```go
func DB() sqldb.Config {
	return sqldb.Config{
		Name:       "main",
		DriverName: "postgres",
		DSN:        forgo.Env("DATABASE_URL", ""),
		Logger: sqldb.LoggerConfig{
			Enabled:         true,
			SlowThreshold:   500 * time.Millisecond,
			Args:            true,
			SensitiveFields: []string{"password_hash", "token"},
		},
	}
}
```

| Field | Default | Description |
| --- | --- | --- |
| `Enabled` | `false` | Turns the log on. |
| `SlowThreshold` | `200ms` | Statements that take at least this long log as slow. Negative values fail `New`. |
| `SlowOnly` | `false` | Logs only slow and failed statements. |
| `Args` | `false` | Logs the arguments of each statement. |
| `SensitiveFields` | | Masks logged arguments. See [Masking arguments](#masking-arguments). |

| Statement | Level | Message |
| --- | --- | --- |
| Completed within `SlowThreshold` | info, or debug for the [storages](#inbox-and-outbox) and [live config stores](#liveconfig) of this library | `sql query completed` |
| Completed in `SlowThreshold` or more | warn | `slow sql query` |
| Failed | error, with `err_severity` 2 (critical) | `sql query failed` |
| Canceled through its context, or failed with `driver.ErrBadConn` (database/sql retries on another connection) | warn, with `err_severity` 0 (light) | `sql query failed` |

```json
{"level":"warn","op_id":"…","sql_db":"main","sql_query":"SELECT id FROM users WHERE email = $1 AND password_hash = $2","sql_args":["ada@example.com","[*]"],"sql_returned":1,"elapsed":0.6124,"sql_caller":"/app/users/repository.go:42","message":"slow sql query"}
```

| Key | Value |
| --- | --- |
| `sql_db` | `Config.Name`. |
| `sql_query` | The statement as written, or `BEGIN`, `COMMIT` and `ROLLBACK` for transactions. Cut at 32 KiB, with `sql_query_cut: true`. |
| `sql_args` | With `Args`, the arguments in placeholder order. Strings and byte slices are cut at 1 KiB and at most 100 arguments are logged, with `sql_args_cut: true`. |
| `sql_affected` | The rows an exec affected. |
| `sql_returned` | The rows read from a query. |
| `elapsed` | Seconds, as a float. A query lasts until its rows close, so reading them counts. Waiting for a connection of the pool does not: see `db.Stats()`. |
| `sql_caller` | On slow and failed statements, the `file:line` of the code that ran the statement, past `database/sql` and `sqltx`. |
| `err_severity`, `error` | On failures. |

Every exec and query is logged, prepared statements each time they run. A failed prepare is logged too, since its statement never runs: with MySQL's server-side prepares, a syntax error shows there. Successful prepares and pings are not.

The storages of this library run their statements at debug, because the outbox relay polls every second whether or not there is work. Set the logger to the debug level to see them. Their slow and failed statements log like any other.

##### Masking arguments

Arguments are masked as `[*]` under the rules of forgo's [`SensitiveFields`](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#masking-sensitive-fields): names match case-insensitively. They reach the driver by position only, so the log reads the statement to find the column of each placeholder: the column it is compared with, assigned to or inserted into.

| Statement | Column of each placeholder |
| --- | --- |
| `email = $1`, `$1 = email`, `u.email <> ?`, `email LIKE ?`, `email IS DISTINCT FROM $1` | `email` |
| `lower(email) = lower($1)`, `email = $1::citext`, `data->>'email' = $1` | `email` |
| `id IN (?, ?)`, `id = ANY($1)`, `created_at BETWEEN $1 AND $2` | `id`, `id`, `created_at` |
| `SET password_hash = $1` | `password_hash` |
| `INSERT INTO users (email, token) VALUES (?, ?), (?, ?)` | from the column list, for every row |
| `LIMIT $1`, `OFFSET $2`, `FETCH FIRST $1 ROWS`, `INTERVAL ? SECOND` | always shown |

An argument passed with `sql.Named` is masked by its own name. Every other argument is masked when:

- its column is sensitive, or any column in the expression around it is: `hash = sha256(salt || $1)` is masked when `hash` or `salt` is sensitive;
- no column can be found for it, as in `SELECT $1`, `COALESCE(?, name)`, `INSERT … SELECT ?`, `CASE WHEN … THEN $2` and `ORDER BY $1`, or in a subquery's select list;
- it is a `$n` that appears several times, unless every appearance is shown. Tests such as `$1 IS NULL` do not count, so `($1::text IS NULL OR name = $1)` is shown.

Text in literals, quoted identifiers, comments and PostgreSQL dollar quotes is skipped, so `'a = ?'` holds no placeholder.

A string or byte slice that starts with `{` or `[` is masked like a request body: the value of every sensitive member at any depth, and it is logged as compact JSON. One that was cut at 1 KiB, or that is not valid JSON, is logged as a string up to where it stops being valid, with `sql_args_cut: true`, since the rest cannot be masked. A type the driver accepts as is is logged as `encoding/json` encodes it, and masked the same way.

Without `SensitiveFields`, every argument is logged as is.

The log reads a statement once, the first time one of its lines logs arguments, and caches the result for up to 1,024 statements per database. Past that, each line reads its statement again, which takes about a microsecond.

##### Cost

Off, nothing wraps the driver. On, a statement costs two clock reads. A line written to an output costs about 0.2 µs, or 0.3 µs with masked arguments, and allocates nothing once its statement was read. A line below the logger's level, or a fast statement with `SlowOnly`, is never built. `go test -bench Exec ./resource/sqldb` measures it.

##### Drivers

The log wraps the connections of the driver and implements exactly the optional `database/sql/driver` interfaces the driver does, so `database/sql` behaves as without it. `db.Driver()` still returns the driver, so forgo's `migration.DetectDialect` finds the dialect. The function given to `sql.Conn.Raw` receives the wrapper's connection rather than the driver's. `sqldb.DriverConn` returns the driver's, and any other value as is, so code that calls it works with the log on or off:

```go
err := conn.Raw(func(driverConn any) error {
	pg := sqldb.DriverConn(driverConn).(*stdlib.Conn).Conn() // pgx
	_, err := pg.CopyFrom(ctx, pgx.Identifier{"users"}, columns, rows)
	return err
})
```

Statements run on the driver's connection are not logged. A library that calls `Raw` itself and expects the driver's type fails with the log on, since it does not know `DriverConn`.

### redisdb

A go-redis universal client, for one Redis server, a Redis Cluster or a Sentinel-managed master. Its value is a `*redis.Client` for a server or a master, and a `*redis.ClusterClient` for a cluster or for a master whose reads go to replicas. Request `redis.UniversalClient`, as the providers of this library do, so your code runs unchanged on every deployment shape.

```go
import "github.com/efegreat/forgo-adapter/resource/redisdb"

func Redis() redisdb.Config {
	return redisdb.Config{
		Name:     "cache",
		Addrs:    strings.Split(forgo.Env("REDIS_ADDRS", "localhost:6379"), ","),
		Password: forgo.Env("REDIS_PASSWORD", ""),
	}
}
```

The client follows `redis.NewUniversalClient`:

| Config | Client |
| --- | --- |
| `MasterName` set | Failover client. `Addrs` are the Sentinels, and the client follows the master they promote. With a `ReadFrom` other than `ReadFromMaster`, a cluster client over the master and the replicas the Sentinels report. |
| Two or more `Addrs`, or `ClusterMode` | Cluster client. `Addrs` are seed nodes, and the client discovers the others. |
| Otherwise | Single-server client on `Addrs[0]`. |

| Field | Default | Description |
| --- | --- | --- |
| `Name` | | The resource name, for `boot:"name"` tags. |
| `Addrs` | `localhost:6379` | `host:port` of the server, of cluster seed nodes, or of the Sentinels of `MasterName`. |
| `MasterName` | | Sentinel master name. Makes a failover client. |
| `ClusterMode` | `false` | Makes a cluster client from a single address, such as a managed cluster's configuration endpoint. Not combinable with `MasterName`. |
| `ReadFrom` | `ReadFromMaster` | The nodes that serve read-only commands. See [Replica reads](#replica-reads). |
| `Username`, `Password` | | ACL credentials of the server, nodes or master. |
| `SentinelUsername`, `SentinelPassword` | | Credentials of the Sentinels, when they differ. |
| `DB` | `0` | Logical database. Redis Cluster only has database 0, so a cluster client rejects any other. |
| `PoolSize` | go-redis default (10 per `GOMAXPROCS`) | Maximum socket connections, per node for a cluster. |
| `DialTimeout`, `ReadTimeout`, `WriteTimeout` | go-redis defaults (5s, 3s, 3s) | Network timeouts. |
| `TLSConfig` | plaintext | Enables TLS when set. |
| `ConnectTimeout` | `5s` | Bounds the startup ping. |

`New` rejects an empty address, `ClusterMode` with `MasterName`, an unknown `ReadFrom`, and a non-zero `DB` for a cluster before connecting. A failed ping closes the client and fails the bootstrap with `redisdb: ping <addrs>: …`, or `redisdb: ping <master> via <sentinels>: …` for a failover client.

Every command and script of the Redis providers touches a single key, so they also work on a cluster.

#### Replica reads

`ReadFrom` spreads the read-only commands of a cluster or failover client over replicas. Writes and scripts always go to the master. A single-server client has one node and ignores it, so a config that sets `ReadFrom` also runs against one local Redis.

| `ReadFrom` | Read-only commands go to |
| --- | --- |
| `ReadFromMaster` (default) | The master. |
| `ReadFromReplica` | A random replica, or the master while no replica is healthy. |
| `ReadFromNearest` | The node with the lowest latency, master or replica. |
| `ReadFromAny` | A random node, master or replica. |

`ReadFrom` is a string (`master`, `replica`, `nearest`, `any`), so it can come from the environment: `ReadFrom: redisdb.ReadFrom(forgo.Env("REDIS_READ_FROM", ""))`.

Replicas lag their master, so a read from a replica can miss a write that just succeeded. The locker and the rate limiter only write, so they always use the master. The live config cache reads with `HGET`: after a save, a `Load` may still get the previous version for the replication delay, and a `MemoryTTL` keeps what it read. Give the live config cache a resource with `ReadFromMaster` if it must read its own writes.

### rabbitmq

A RabbitMQ broker for forgo's [worker](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#worker). Its value is the `worker.IBroker` that both `worker.ConsumerConfig` and `worker.ProducerConfig` resolve.

```go
import "github.com/efegreat/forgo-adapter/resource/broker/rabbitmq"

func Broker() rabbitmq.Config {
	return rabbitmq.Config{
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
				Bindings:   []rabbitmq.BindingConfig{{Exchange: "orders", RoutingKey: "order.*"}}, // Message.Key, such as order.placed
				DeadLetter: rabbitmq.DeadLetterConfig{Enabled: true},
				Retry:      rabbitmq.RetryConfig{Initial: time.Second}, // delayed retries: 1s, 2s, 4s… up to 10m
			},
		},
		Publishers: []rabbitmq.PublisherConfig{
			{Name: "order.placed"}, // persistent by default
		},
		Subscribers: []rabbitmq.SubscriberConfig{
			{Name: "billing", Queue: "billing"}, // prefetch defaults to the worker's Concurrency
		},
	}
}
```

#### Connection and topology

| Field | Default | Description |
| --- | --- | --- |
| `Name` | | The resource name. It also names the two connections in the RabbitMQ management UI: `<Name>` and `<Name>-publish`. |
| `Addr` | `localhost` | Broker host. |
| `Port` | `5672`, or `5671` with `TLSConfig` | Broker port. |
| `TLSConfig` | plaintext | Connects with TLS (`amqps`) when set. The certificate is verified against `Addr` unless it sets a `ServerName`. |
| `Username`, `Password` | client default (`guest`) | PLAIN credentials. They are passed to the client directly, so special characters need no escaping. |
| `VHost` | `/` | Virtual host. |
| `Recovery` | enabled, unlimited retries every 5s | Automatic reconnection, see [Recovery](#recovery). |
| `Exchanges` | | Exchanges to declare: `Name`, `Kind`, `Durable`, `AutoDelete`, `Internal`, `NoWait`, `Args`. |
| `Queues` | | Queues to declare: `Name`, `Durable`, `AutoDelete`, `Exclusive`, `NoWait`, `Args`, `Bindings`, `DeadLetter` and `Retry`. Set `Durable` unless the queue is `Exclusive`: RabbitMQ 4 refuses other transient queues by default. |
| `Publishers` | | Named publishers, see below. |
| `Subscribers` | | Named subscribers, see below. |

Without `TLSConfig`, the credentials and every message travel in clear text. Managed brokers usually accept only TLS, on port 5671. Their certificates are publicly trusted, so an empty config is enough: `TLSConfig: &tls.Config{}`. `New` copies the config before the client sees it, so a shared `tls.Config` stays as it is.

**Two connections.** `New` opens one connection for the topology and the subscribers, and one, `<Name>-publish`, for the publishers and for the copies the subscribers publish, of their retries and dead letters. While RabbitMQ is low on memory or disk space, it blocks every connection that publishes, with all its channels. On a connection of their own, the subscribers keep consuming meanwhile, which frees what the broker lacks.

`New` connects, then declares every exchange, then every queue with its bindings. A queue's dead-letter exchange and queue are declared just before the queue itself, and its retry queues just after it. Any failure closes the connection and fails the bootstrap. The channel the topology was declared on stays open, because the client only redeclares the topology recorded on open channels after a reconnect. Declarations are idempotent, but redeclaring an existing exchange or queue with different properties or arguments is refused by RabbitMQ with `PRECONDITION_FAILED`.

**The topology is the application's.** Which subscribers receive a message is decided here: by the exchange of its `Destination`, the bindings of the queues, and the routing key, `Message.Key` (see [Messages on the wire](#messages-on-the-wire)). Application tests on forgo's `forgotest` run no broker, so they do not cover it (see [Brokers in tests](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#brokers-in-tests)). They put a stand-in in place of this resource, with `forgotest.ReplaceResource[rabbitmq.Config](forgotest.Broker("events"))`. A binding whose routing key does not match drops the message, unless the publisher is `Mandatory`, while every test passes. Check the topology where it is declared, against a real broker, in staging or a smoke check.

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
- `Mandatory` is set and no queue receives it (`rabbitmq: mandatory message is unroutable`),
- RabbitMQ blocks publishing during a memory or disk alarm, and the publish times out waiting for it to end (`rabbitmq: the broker blocks publishing (low on memory)`). The publisher sends nothing while blocked: the client would otherwise block in the socket's write once its buffer is full, past every timeout, for as long as the alarm lasts.

A publisher sends one message at a time and waits for its confirm. Raise the worker publisher's `Concurrency` for throughput: each unit is its own channel.

| `SubscriberConfig` field | Default | Description |
| --- | --- | --- |
| `Name` | | Matches `worker.SubscriberConfig.Name`. |
| `Queue` | | The queue to consume. It must exist when the server starts. |
| `Prefetch` | the worker subscriber's `Concurrency` | How many unacknowledged messages RabbitMQ sends this consumer. The default keeps every handler slot busy and leaves the rest of the backlog in the queue for other instances. Raise it a little to hide network latency. A negative value removes the limit. At most 65535. |
| `Exclusive`, `NoLocal`, `NoWait`, `Args` | | AMQP consume options. There is no automatic acknowledgement: the worker settles every message itself. |

The worker's `SubscriberConfig.Concurrency` bounds how many messages are handled at once. A subscriber consumes under the consumer tag `forgo-<Name>-<n>`, which tells subscribers apart in the management UI.

#### Messages on the wire

A `worker.Message` is published to the exchange `Message.Destination` with the routing key `Message.Key`:

| AMQP | From |
| --- | --- |
| body | `Message.Payload`, `content_type: application/json` |
| `message_id` property, `x-msg-id` header | `Message.ID` |
| `type` property, `x-msg-name` header | `Message.Name` |
| `timestamp` property, `x-msg-timestamp` header (Unix nanoseconds) | `Message.CreatedAt`, when set |
| headers | `Message.Headers` |

Consuming reverses the mapping with `worker.NewMessageFromDelivery`: `Key` is the routing key and `Destination` the exchange the message was published to. Messages from other producers keep their identity when they set the `message_id` and `type` properties instead of the `x-msg-*` headers. Without either, the ID is generated on every delivery, so the consumer inbox cannot recognize a redelivery. Header values that are not strings are converted, so no header is lost: numbers and booleans take their text form (`2`, `true`), decimals their decimal notation, timestamps RFC 3339 in UTC, byte arrays their bytes and void an empty string. Tables and arrays, such as the `x-death` header RabbitMQ adds to dead-lettered and retried messages, are encoded as JSON, for example `[{"count":1,"queue":"orders.retry.1s","reason":"expired",…}]`. Retry copies keep the original AMQP values.

The worker acknowledges each delivery:

| Worker | AMQP | Effect |
| --- | --- | --- |
| `Complete` | `basic.ack` | The message is removed. |
| `Retry` | `basic.nack`, requeue | The message goes back to the queue and is delivered again at once. When the queue has [`Retry`](#retry-queues), a copy goes to a retry queue instead and the delivery is acknowledged: the message is delivered again after a delay. |
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

**The subscriber dead-letters what the worker rejects itself.** RabbitMQ's own dead-lettering is at most once: it drops a message whose dead-letter queue was deleted, is full, or is on a node that is down, and nothing reports it, while the inbox already marked the message *failed* or *rejected*. So instead of rejecting the delivery to RabbitMQ, the subscriber publishes a copy to `billing.dlx` with the key `billing`, waits for the broker's confirm, and then acknowledges the delivery, as it does for [retries](#retry-queues).

- **A copy that cannot be stored brings the message back,** to be rejected again from its inbox status until the copy is stored. The copy is mandatory, so a dead-letter queue that was deleted or unbound fails it, and a full one with `x-overflow: reject-publish` nacks it. When the queue has [`Retry`](#retry-queues), the message waits in its last retry queue, and `Reject` returns `rabbitmq: dead-letter <id> to <exchange>: …; parked in <retry queue> until it is rejected again`. Without `Retry`, it is requeued (`…; requeuing it`), and comes back at once, ahead of the queue's other messages: until an operator repairs the dead-letter queue, a few such messages take every handler of the subscriber. Enable `Retry` with `DeadLetter`.
- **A dead letter can be stored more than once.** A crash or a lost connection between the confirm and the acknowledgement brings the message back, to be rejected again, and a message that is in the queue twice, such as one the outbox published again, is rejected once per copy. A three-hour run with a fault every four minutes stored 1,070 dead letters for 1,065 messages, so tell them apart by `x-msg-id`.

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
- a message no handler is registered for (`msg_no_handler`).

RabbitMQ itself dead-letters messages that expire through `x-message-ttl`, overflow a queue with `x-overflow: reject-publish-dlx`, or reach the delivery limit of a quorum queue, at most once. Retried messages are not dead-lettered: they are requeued, or wait in a retry queue.

Dead letters keep their body, properties and headers, with the message ID set in `message_id` and `x-msg-id`, and they arrive from the dead-letter exchange with its routing key, as RabbitMQ's would. The ones RabbitMQ dead-letters itself carry its `x-death` header, which records the source queue, the reason and a count; the worker's copies carry only the `x-death` of the retry queues they went through. To replay messages, consume `<queue>.dlq` with another subscriber or use a shovel. Because a replayed message keeps its `x-msg-id`, the worker rejects it again, back to `<queue>.dlq`, while its inbox row is *failed* or *rejected*. To process it again, first set its inbox row back to *waiting* with zero attempts, then replay it. See forgo's [Retrying failed messages](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#retrying-failed-messages).

**Validation.** `New` fails before connecting when a queue enables `DeadLetter` without a `Name`, or also sets `x-dead-letter-exchange` or `x-dead-letter-routing-key` in its `Args`. Use one or the other.

#### Retry queues

By default, a message the worker retries is requeued and delivered again at once. During a downstream outage of a few milliseconds, that uses up every `MaxAttempt`. The same goes for the retries that do not count an attempt: a lock held by another delivery or an inbox outage then loops until it is resolved. Retry queues delay each redelivery instead, with plain queues and no plugin:

```go
rabbitmq.QueueConfig{
	Name:    "billing",
	Durable: true,
	Retry:   rabbitmq.RetryConfig{Initial: time.Second, Max: time.Minute},
}
```

The delay doubles with every failed attempt, from `Initial` up to `Max`. Each delay has its own durable queue, declared after `billing`: `billing.retry.1s`, `billing.retry.2s`, `billing.retry.4s` and so on up to `billing.retry.1m0s`. Each one has the arguments `x-message-ttl` (its delay), `x-dead-letter-exchange: ""` and `x-dead-letter-routing-key: billing`. An expired message goes back to `billing` through the default exchange, so no other queue bound to the original exchange receives it again.

Retry queues are quorum queues with `x-dead-letter-strategy: at-least-once` and `x-overflow: reject-publish`, unless `Args` set other values. RabbitMQ's own dead-lettering is otherwise at most once, and a node failure loses the messages on their way back to `billing`: in a live test on a three-node cluster, the queue's leader failing while 3,000 retried messages expired lost 189 of them with classic retry queues, and none with these. A single node runs quorum queues too.

The worker passes the attempt that failed, as counted by its inbox, to `Retry`:

| Attempt | Retry queue |
| --- | --- |
| 1, and 0 for a failure before the attempt was counted (lock conflict, inbox outage) | `Initial` |
| n | `Initial` × 2ⁿ⁻¹, at most `Max` |

To retry, the subscriber publishes a copy of the delivery to the retry queue, waits for the broker's confirm, and then acknowledges the delivery. The waiting message holds no prefetch slot and no handler. The copy keeps the body, properties and headers, with the message ID set in `message_id` and `x-msg-id`. It is persistent, and it drops `expiration` and `user_id`.

- **A failed publish falls back to an immediate requeue,** and `Retry` returns `rabbitmq: delay the retry of <id> through <retry queue>, requeuing it at once`. That happens when the subscriber's channel was lost, the retry queue no longer exists (the copy is published as mandatory), or the publishing connection stayed down or blocked for the whole `AckTimeout` of the worker's subscriber. The copy waits for it meanwhile, so that the message is not requeued, and handled again, at once and again and again.
- **A crash between the confirm and the acknowledgement** delivers the message twice: the requeued original at once, and the copy after its delay. The inbox absorbs the duplicate.
- **Retried messages fall behind newer ones,** and come back with the routing key `billing` from the default exchange.
- **Changing `Initial` or `Max`** declares new retry queues. The old ones still return what they hold, and can be deleted once empty.

| `RetryConfig` field | Default | Description |
| --- | --- | --- |
| `Initial` | disabled | Delay after the first failed attempt, at least 1ms. Zero keeps immediate requeues. |
| `Max` | `10m`, or `Initial` when longer | The longest delay. It bounds the number of retry queues: `Initial: time.Second` with the default `Max` declares 11. |
| `Args` | `x-queue-type: quorum`, `x-dead-letter-strategy: at-least-once`, `x-overflow: reject-publish` | Arguments of every retry queue. They replace the defaults they set: `x-queue-type: classic` declares classic retry queues, without the at-least-once defaults, which only quorum queues support. |

**Validation.** `New` fails before connecting when a queue enables `Retry` without a `Name`, with a negative value, an `Initial` under 1ms, a `Max` under `Initial` or over 49 days (the longest `x-message-ttl`), or `Args` that set `x-message-ttl`, `x-dead-letter-exchange` or `x-dead-letter-routing-key`.

**Existing queues.** Queue arguments cannot change after a queue is declared. Enabling `DeadLetter` on a queue that already exists without it fails with `PRECONDITION_FAILED`. Either delete and recreate the queue, or leave `DeadLetter` off and set the dead-letter exchange through a RabbitMQ policy. Retry queues declared as classic queues by an earlier version fail the same way: delete them once empty, or keep them with `Args: map[string]any{"x-queue-type": "classic"}`.

#### Recovery

The broker uses the automatic recovery of amqp091-go, on each of its two connections. When a connection or a channel is lost, the client reconnects and reopens every channel with its prefetch and confirm mode. It then redeclares the exchanges, queues, bindings and dead-letter queues, and the broker subscribes every subscriber again, so the worker does not notice beyond a pause.

```go
rabbitmq.Config{
	Recovery: rabbitmq.RecoveryConfig{MaxRetries: 60, RetryInterval: 2 * time.Second},
}
```

| `RecoveryConfig` field | Default | Description |
| --- | --- | --- |
| `Disabled` | `false` | Turns recovery off. A lost connection then stops every subscriber and fails every publish until the app restarts. |
| `MaxRetries` | unlimited | Reconnection attempts of each connection before giving up. Once they are exhausted, the connection closes. On the subscribers' connection, subscribers then stop with `rabbitmq: delivery channel closed by the broker`, or with `rabbitmq: recovery could not re-subscribe the consumer` when an attempt had reconnected. On the publishing connection, every publish fails. |
| `RetryInterval` | `5s` | Wait before each attempt, plus up to 500ms of jitter. |

During a recovery:

- **Publishes fail fast while the publishing connection is down.** The outbox keeps the messages and retries them on its next cycle. The copies of the subscribers' retries and dead letters wait for the connection instead, within the worker's `AckTimeout`, so that their deliveries are not requeued and handled again at once. While the client reopens a publisher's channel, the publish waits for it, within its context, so that no message is sent before the channel is back in confirm mode. A message sent in between would never be confirmed, and every later confirm would be counted against the wrong message.
- **Unacknowledged deliveries are requeued by the broker.** Delivery tags start over on a recovered channel, so a late acknowledgement from a handler that was still running would settle a different message. The broker marks those deliveries stale before the channel is reopened, and their acknowledgements fail instead (`rabbitmq: delivery belongs to a lost channel`). The message is delivered again, and the inbox recognizes it by its `x-msg-id`. If the handler had completed it, the redelivery is acknowledged and skipped. On a quorum queue, each such return counts toward the queue's delivery limit (`x-delivery-limit`, 20 by default since RabbitMQ 4.0), while the worker's requeues do not. RabbitMQ dead-letters a message that reaches it, or drops it without a dead-letter queue, whatever its inbox status, so set the limit above `MaxAttempt` with room for connection losses.
- **Deliveries that no handler took are dropped.** With a `Prefetch` above the worker's `Concurrency`, the client holds deliveries that wait for a handler. The broker requeued them with the channel, so the subscriber drops them and consumes them again on the reopened channel, where it subscribes on a new delivery stream.
- **Unconfirmed publishes fail.** The client settles the pending confirms of a lost channel as nacks when it reopens it, and the publish fails with `rabbitmq: publisher channel lost before the broker confirmed the message`, followed by the reason, such as the `NOT_FOUND` of a publish to an exchange that does not exist. The outbox retries them. A message the broker had already stored is then published twice, and the consumer inbox drops the duplicate.

An exchange, queue or binding that fails to recover is skipped, so the rest of the topology still recovers. An auto-delete queue (`AutoDelete`), which the broker deletes with its last consumer, is declared again with its bindings before its subscriber subscribes again. A subscriber can fail to subscribe again because its queue was deleted, or because a consumer with `Exclusive` on another connection holds the queue. It would then receive nothing, so it stops with `rabbitmq: recovery could not re-subscribe the consumer`, followed by the broker's error. The worker server then fails and the app stops. When it starts again, the subscriber consumes again, or fails at startup if its queue is gone.

**Consumer cancellations.** RabbitMQ cancels a consumer whose queue is deleted, and from 4.3 on, a consumer of a quorum queue whose delivery outlives the consumer timeout (`consumer_timeout`, a `consumer-timeout` policy, or the queue's `x-consumer-timeout`). The subscriber then cancels the consumer itself and consumes again at once, on the same channel and under a new consumer tag. A quorum queue keeps a consumer that timed out, with its tag, and can deliver to it again once its late delivery is acknowledged, while the client already dropped it: those deliveries would wait, unacknowledged, until RabbitMQ timed them out too and closed the channel (`PRECONDITION_FAILED - delivery acknowledgement on channel … timed out`). Cancelling it removes it. Then:

- **A message that timed out goes back to the queue,** and is delivered again even while its first handler still runs, so it can be handled twice at the same time. The inbox commits at most one of them, but what a handler does outside its transaction, such as calling another service, happens twice. Each delivery counts as an attempt, so a handler that keeps outliving the timeout can use up `MaxAttempt` before any of its runs commits: the message then fails and is dead-lettered, although its handler succeeds. In a long run with 25-second handlers and a 20-second timeout, that happened to one message in 36, which the outbox had published twice during an outage.
- **The other messages the cancelled consumer held stay with it,** and their handlers acknowledge them as usual.
- **A deleted queue cannot be consumed again,** and the subscriber stops with `rabbitmq: recovery could not re-subscribe the consumer`, followed by the broker's `NOT_FOUND`.

Before 4.3, RabbitMQ closes the channel of a consumer that timed out instead, and the recovery reopens it. Either way, set the consumer timeout above the longest handler, or end handlers before it with the worker's `HandlerConfig.Timeout`.

**Shutting down during an outage.** `Close` stops a recovery in progress, and waits at most 2 seconds for a broker that does not answer, for each channel and connection, such as one that hangs or that the network cut off. Neither fails the close, so a SIGTERM during an outage stops the app cleanly: RabbitMQ requeues what the connections had not acknowledged once it notices they are gone. A subscriber closes its channel and that of its copies together, and the broker its two connections, so each waits once.

The client logs its recovery through `amqp.Logger`, which discards everything by default. Call `amqp.SetLogger` in `main` to see reconnects.

### pubsub

A Google Cloud Pub/Sub broker for forgo's [worker](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#worker), on the official client `cloud.google.com/go/pubsub/v2`. Its value is the `worker.IBroker` that both `worker.ConsumerConfig` and `worker.ProducerConfig` resolve.

```go
import "github.com/efegreat/forgo-adapter/resource/broker/pubsub"

func Broker() pubsub.Config {
	return pubsub.Config{
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
}
```

#### Client and topology

| Field | Default | Description |
| --- | --- | --- |
| `Name` | | The resource name. |
| `ProjectID` | `GOOGLE_CLOUD_PROJECT`, or the project of the credentials | Project of the topics and subscriptions. Set it for the emulator. |
| `Options` | Application Default Credentials | `option.ClientOption`s of the client, such as `option.WithCredentialsFile`, or `option.WithEndpoint` for a regional endpoint. |
| `DeclareTimeout` | `30s` | Time limit for creating and checking the topics and subscriptions in `New`. |
| `Topics` | | Topics to create: `Name`. |
| `Subscriptions` | | Subscriptions to create, see below. |
| `Subscribers` | | Named subscribers, see below. |

A topic or subscription is an ID in `ProjectID`, or a full name such as `projects/shared/topics/orders` for one in another project.

`New` creates the client, then the topics, then every subscription, after its dead-letter topic and subscription. Any failure closes the client and fails the bootstrap. What already exists is kept, but an existing subscription must have the settings of its config: its topic, filter, message ordering, exactly-once delivery, retry policy and dead-letter policy. Otherwise `New` fails with `pubsub: subscription exists with other settings: projects/<project>/subscriptions/billing has another retry policy; update or recreate it`. Most of these settings cannot change after a subscription is created, and `New` never changes one behind your back. Update it yourself, for example with `gcloud pubsub subscriptions update billing --min-retry-delay=1s --max-retry-delay=600s`, or delete it and let `New` create it again. The subscriptions `New` creates never expire, whereas Pub/Sub deletes a subscription after 31 days without subscribers by default.

Creating needs `roles/pubsub.editor` on what you list. An app that only publishes and consumes, with `roles/pubsub.publisher` and `roles/pubsub.subscriber`, lists nothing in `Topics` and `Subscriptions` and uses topics and subscriptions created elsewhere, for example with Terraform. `New` then makes no call to Pub/Sub, and a missing topic or subscription shows up at the first publish or receive.

To develop against the [emulator](https://cloud.google.com/pubsub/docs/emulator), set `PUBSUB_EMULATOR_HOST`, such as `localhost:8085`, and a `ProjectID`. The client then connects without credentials, and `New` creates the topology in the empty emulator.

**The topology is the application's.** Which subscribers receive a message is decided here, or wherever the topics and subscriptions are created: by the topic of its `Destination`, and the subscriptions of that topic with their `Filter`. Application tests on forgo's `forgotest` run no broker, so they do not cover it (see [Brokers in tests](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#brokers-in-tests)). They put a stand-in in place of this resource, with `forgotest.ReplaceResource[pubsub.Config](forgotest.Broker("events"))`. A message published to a topic without the subscription a subscriber reads reaches no handler, while every test passes. Check the topology where it is declared, against Pub/Sub or the emulator, in staging or a smoke check.

#### Subscriptions

| `SubscriptionConfig` field | Default | Description |
| --- | --- | --- |
| `Name` | | The subscription. |
| `Topic` | | The topic it receives from. |
| `Filter` | | Delivers only the messages whose attributes match it, such as `attributes.tenant = "acme"`. Pub/Sub acknowledges the others itself. |
| `Ordering` | `false` | Delivers the messages that share an ordering key (`Message.Key`) one at a time, in the order Pub/Sub received them. |
| `ExactlyOnce` | `false` | Exactly-once delivery: an acknowledged message is not delivered again, and `Complete` waits until Pub/Sub confirms the acknowledgement. |
| `Retry` | disabled | The retry policy, see [Retry policy](#retry-policy). |
| `DeadLetter` | disabled | A dead-letter topic, see [Dead-letter topics](#dead-letter-topics). |

The other subscription settings keep Pub/Sub's defaults, such as a 10s ack deadline and 7 days of retention.

**Validation.** `New` fails before connecting when a topic or subscription has no `Name`, a subscription has no `Topic`, a subscriber has no `Subscription`, or `Retry` or `DeadLetter` is out of range (see below).

#### Publishers and subscribers

Publishers have no settings. `NewPublisher` accepts every `worker.PublisherConfig.Name`, and each message goes to the topic its `Destination` names. All publishers of the broker share one client publisher per topic, so the parallel publishes of the worker publisher's `Concurrency` are sent to Pub/Sub in batches. The client waits up to 10ms for other messages to share a batch: raise `Concurrency` for throughput.

`Publish` waits, up to the worker's `PublisherConfig.Timeout`, until Pub/Sub has stored the message. Only then does the outbox mark the message processed. A publish fails, and the message stays in the outbox for the next cycle, when:

- the message has no `Destination` (`pubsub: message has no Destination topic`),
- the topic does not exist, or the credentials may not publish to it,
- no answer arrives in time.

The client goes on retrying a publish that gave up waiting, for up to a minute. On shutdown, the broker waits up to five seconds for the messages still being published, then closes the client, which ends those retries. The outbox publishes such a message again, since it was never marked processed.

| `SubscriberConfig` field | Default | Description |
| --- | --- | --- |
| `Name` | | Matches `worker.SubscriberConfig.Name`. |
| `Subscription` | | The subscription to receive from. |
| `MaxOutstandingMessages` | the worker subscriber's `Concurrency` | How many unsettled messages Pub/Sub delivers to this subscriber. The default keeps every handler slot busy and leaves the rest of the backlog to other instances. Messages beyond `Concurrency` wait in the subscriber, which extends their ack deadline. A negative value removes the limit. |
| `DeadLetterTopic` | the `DeadLetter` topic of its subscription in `Subscriptions` | The topic `Reject` publishes to, for a subscription created outside `New`. |

An unknown subscriber name fails the server at startup. A missing subscription does not, because `roles/pubsub.subscriber` cannot read subscriptions. Instead, receiving fails at once with `pubsub: subscriber billing: receive from projects/<project>/subscriptions/billing`, followed by Pub/Sub's `NotFound` or `PermissionDenied`, and the worker server stops the app.

While Pub/Sub cannot be reached, the client opens its stream again by itself. It gives up when a stream ends with some errors that pass: an exceeded quota (`ResourceExhausted`), an error a proxy or load balancer sent (`Unknown`), or a server that shut down. The subscriber then receives again, after a pause that doubles from a second up to a minute with every failure in a row. Other errors, such as `NotFound` or `PermissionDenied`, stop the app as above.

The worker's `SubscriberConfig.Concurrency` bounds how many messages are handled at once. The subscriber extends the ack deadline of every message it holds for up to 60 minutes. A handler that runs longer has its message delivered again meanwhile, so bound slow handlers with `HandlerConfig.Timeout`. On shutdown, the subscriber stops pulling and waits for every running handler, and the client sends their acknowledgements before `Consume` returns. Delivered messages whose handler had not started are nacked, so they are delivered again. While Pub/Sub does not answer, the client would retry the acknowledgements for a minute, longer than most termination timeouts: the subscriber waits for them for five seconds after its last handler finished, and the messages whose acknowledgement was not sent are delivered again.

#### Messages on the wire

A `worker.Message` is published to the topic `Message.Destination`:

| Pub/Sub | From |
| --- | --- |
| data | `Message.Payload` |
| ordering key | `Message.Key` |
| `x-msg-id` attribute | `Message.ID` |
| `x-msg-name` attribute | `Message.Name` |
| `x-msg-timestamp` attribute (Unix nanoseconds) | `Message.CreatedAt`, when set |
| attributes | `Message.Headers` |

Consuming reverses the mapping with `worker.NewMessageFromDelivery`: `Key` is the ordering key, and `Destination` is empty, because Pub/Sub does not tell a subscriber which topic a message was published to. Messages from other producers without `x-msg-id` keep the message ID Pub/Sub assigned them, which stays the same across redeliveries, and those without `x-msg-timestamp` take the time Pub/Sub received them. Without `x-msg-name`, no handler receives them (`msg_no_handler`).

**Ordering keys.** Every client publisher enables message ordering, so a message with a `Key` never fails for want of it. The client then sends the messages of a key one batch at a time, and Pub/Sub accepts at most 1 MB/s per key. A failed publish pauses its key in the client, and `Publish` resumes it at once: the outbox publishes the message again on a later cycle, and does not keep the order of messages it retries anyway. Subscriptions deliver in key order only with `Ordering`. Pub/Sub keeps the order of messages published in one region, so publish them through a regional endpoint, such as `option.WithEndpoint("us-east1-pubsub.googleapis.com:443")`.

The worker settles each delivery:

| Worker | Pub/Sub | Effect |
| --- | --- | --- |
| `Complete` | ack | The message is removed. With `ExactlyOnce`, `Complete` waits until Pub/Sub confirms the ack. |
| `Retry` | nack | The message is delivered again, after the delay of the subscription's [retry policy](#retry-policy) if it has one, and at once otherwise. |
| `Reject` | ack | The message is dropped. With a [dead-letter topic](#dead-letter-topics), a copy is published there first. |

Without `ExactlyOnce`, Pub/Sub acknowledgements are best effort, so a message can be delivered again after `Complete`. The inbox recognizes it by its `x-msg-id` and skips it.

#### Retry policy

By default, a message the worker retries is delivered again at once. Pub/Sub delays redeliveries through the subscription's retry policy:

```go
pubsub.SubscriptionConfig{
	Name:  "billing",
	Topic: "orders",
	Retry: pubsub.RetryConfig{Initial: time.Second, Max: time.Minute},
}
```

This sets the policy's minimum backoff to `Initial` and its maximum to `Max`. After a nack, Pub/Sub waits `Initial` before the next delivery, and the delay grows exponentially with every further failed delivery of the message, up to `Max`. Pub/Sub counts the deliveries itself and cannot delay a single message by request. So the attempt the worker passes to `Retry` is not used, and the retries that count no attempt, such as a lock conflict or an inbox outage, are delayed too.

| `RetryConfig` field | Default | Description |
| --- | --- | --- |
| `Initial` | disabled | Minimum backoff, at most 10 minutes. Zero leaves the subscription without a retry policy. |
| `Max` | `10m` | Maximum backoff, at most 10 minutes. |

**Validation.** `New` fails before connecting when `Retry` has a negative value, a value over 10 minutes, or a `Max` under `Initial`.

#### Dead-letter topics

Messages the worker rejects are dropped unless the subscription has a dead-letter topic:

```go
pubsub.SubscriptionConfig{
	Name:       "billing",
	Topic:      "orders",
	DeadLetter: pubsub.DeadLetterConfig{Enabled: true},
}
```

This creates the topic `billing.dlq` and a subscription `billing.dlq` to it before `billing`. The subscription keeps the dead letters, because Pub/Sub drops the messages of a topic without subscriptions. When it exists, only its topic is checked, so you can change its other settings, such as a longer retention.

Pub/Sub's own dead-lettering only forwards a message after a number of failed deliveries, and the worker cannot trigger it. So to reject a message, the subscriber publishes a copy of it to the dead-letter topic, waits until Pub/Sub has stored the copy, and then acknowledges the message. The copy keeps the data, the attributes and the ordering key, with the message ID in `x-msg-id`.

| `DeadLetterConfig` field | Default | Description |
| --- | --- | --- |
| `Enabled` | `false` | Creates the dead-letter topic and subscription, and makes `Reject` publish to the topic. |
| `Topic` | `<subscription>.dlq` | The dead-letter topic. |
| `Subscription` | the ID of `Topic` | The subscription that keeps the dead letters. |
| `MaxDeliveryAttempts` | no policy | Also gives the subscription a dead-letter policy, between 5 and 100: Pub/Sub itself forwards a message to `Topic` once it was delivered that many times without an ack. That catches messages that keep failing before the worker counts an attempt, such as during a long inbox outage, so set it above the worker's `MaxAttempt` plus room for those retries. The policy needs the Pub/Sub service agent, `service-<project number>@gcp-sa-pubsub.iam.gserviceaccount.com`, to have `roles/pubsub.publisher` on `Topic` and `roles/pubsub.subscriber` on the subscription. |

For a subscription created outside `New`, set `SubscriberConfig.DeadLetterTopic` instead, and create the dead-letter topic and its subscription yourself.

**What ends up there.** Every message the worker rejects:

- a message that failed `MaxAttempt` times (the inbox marks it *failed*),
- a message whose payload cannot be bound (`msg_bind`, inbox *rejected*),
- a message no handler is registered for (`msg_no_handler`),

and, with `MaxDeliveryAttempts`, the messages Pub/Sub forwards itself.

**A copy that cannot be published.** The copy must be stored within the worker's `SubscriberConfig.AckTimeout`. If the topic is gone, the credentials may not publish to it or Pub/Sub does not answer in time, `Reject` fails with `pubsub: dead-letter <id> to <topic>, delivering it again` and nacks the message. The worker rejects it again on its next delivery, after the subscription's [retry policy](#retry-policy) delay, until the copy is published. Without a retry policy, a topic that stays unreachable redelivers such messages in a tight loop. Give the subscriber's credentials `roles/pubsub.publisher` on the dead-letter topic.

Dead letters keep their `x-msg-id`, so the worker rejects a replayed message again, back to the dead-letter topic, while its inbox row is *failed* or *rejected*. To process one again, first set its inbox row back to *waiting* with zero attempts, then publish it to the original topic again. See forgo's [Retrying failed messages](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#retrying-failed-messages).

**Validation.** `New` fails before connecting when an enabled `DeadLetter` has a `MaxDeliveryAttempts` other than zero outside 5 to 100.

### redisstream

A Redis Streams broker for forgo's [worker](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#worker), on go-redis v9 and Redis 6.2 or newer. Its value is the `worker.IBroker` that both `worker.ConsumerConfig` and `worker.ProducerConfig` resolve. A stream plays the part of a topic, and each consumer group the part of a subscription: every group receives every entry once, spread over the instances that read it.

```go
import "github.com/efegreat/forgo-adapter/resource/broker/redisstream"

func Broker() redisstream.Config {
	return redisstream.Config{
		Name:  "events",
		Redis: Redis, // a redisdb.Config: one server, a cluster or a Sentinel master
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
}
```

#### Connection and streams

| Field | Default | Description |
| --- | --- | --- |
| `Name` | | The resource name. |
| `Redis` | see [redisdb](#redisdb) | The connection. The broker opens a client of its own, so the connections its subscribers hold while they wait for entries never come from the pool of your providers. Its `Name` is not used. |
| `Streams` | | Streams to trim and groups to create, see below. |
| `Subscribers` | | Named subscribers, see below. |

| `StreamConfig` field | Default | Description |
| --- | --- | --- |
| `Name` | | The key of the stream. |
| `MaxLen` | no trimming | Trims the stream to about `MaxLen` entries on every entry the broker adds to it (`XADD MAXLEN ~`). See [Trimming](#trimming). |
| `Groups` | | Consumer groups to create: `Name`, and `StartID`, the last entry the group counts as delivered when it is created. `$` (the default) delivers only the entries added afterwards, `0` every entry the stream holds. |

`New` connects with `Redis` like the `redisdb` resource, with its validation and startup ping. It then creates every group of `Streams` that does not exist, and its stream with it (`XGROUP CREATE … MKSTREAM`). A group that exists is kept as it is, with its position, so `StartID` only applies when a group is created. While Redis is out of memory (see [Durability and memory](#durability-and-memory)), it refuses to create a group even when the group exists, so `New` then only checks that it does. Any failure closes the client and fails the bootstrap, for example `redisstream: create group billing of stream orders: WRONGTYPE …` when the key holds another type.

List the groups in the config the producer uses too. With `$`, a group created after a message was published never receives it, so the group must exist before the first publish. When both servers share the broker config, whichever starts first creates the groups.

**The topology is the application's.** Which subscribers receive a message is decided here: by the stream of its `Destination`, and the groups of that stream. Application tests on forgo's `forgotest` run no broker, so they do not cover it (see [Brokers in tests](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#brokers-in-tests)). They put a stand-in in place of this resource, with `forgotest.ReplaceResource[redisstream.Config](forgotest.Broker("events"))`. A message added to a stream without the group a subscriber reads, or before a `$` group was created, reaches no handler, while every test passes. Check the topology against a real Redis, in staging or a smoke check.

Every command touches a single key, so the broker works on Redis Cluster. A dead-letter stream is another key, which the broker writes with commands of its own, so it may hash to another slot.

**Validation.** `New` fails before connecting when a stream has no `Name` or is listed twice, `MaxLen` is negative, a group has no `Name`, a subscriber has no `Stream`, or `ClaimIdle`, `Retry` or `DeadLetter` is out of range (see below).

#### Publishers and subscribers

Publishers have no settings. `NewPublisher` accepts every `worker.PublisherConfig.Name`, and each message is added to the stream its `Destination` names, which Redis creates when it does not exist. `Publish` returns, up to the worker's `PublisherConfig.Timeout`, once Redis has added the entry, and only then does the outbox mark the message processed. A publish fails, and the message stays in the outbox for the next cycle, when:

- the message has no `Destination` (`redisstream: message has no Destination stream`),
- the key holds another type (`WRONGTYPE`), or Redis is out of memory with the `noeviction` policy (`OOM`),
- Redis cannot be reached or does not answer in time.

Redis acknowledges an entry once it is in memory, so a crash or a failover can lose entries the outbox no longer holds, and Redis refuses new entries once it is out of memory: see [Durability and memory](#durability-and-memory).

| `SubscriberConfig` field | Default | Description |
| --- | --- | --- |
| `Name` | | Matches `worker.SubscriberConfig.Name`. |
| `Stream` | | The stream to read. |
| `Group` | `Name` | The consumer group to read as. It must exist when the server starts. |
| `ClaimIdle` | `1m` | How long an entry stays pending without a sign of its consumer before another consumer claims it. At least `1s`. See [Claiming pending entries](#claiming-pending-entries). |
| `Retry` | disabled | Delays retried entries, see [Delayed retries](#delayed-retries). |
| `DeadLetter` | disabled | A dead-letter stream, see [Dead-letter streams](#dead-letter-streams). |

Each subscriber reads as a consumer of its own, named after the host with a random suffix, such as `billing-7d9f-x2k4p-UJ4K5N7QRA`, which the worker server creates when it starts (`XGROUP CREATECONSUMER`), or, while Redis is out of memory, its first read. An unknown subscriber name, or a stream or group that does not exist, fails the server at startup: `redisstream: subscriber billing: create consumer in group billing of stream orders: NOGROUP …`.

A subscriber reads no more new entries (`XREADGROUP`) than it has free handler slots, so the rest of the backlog stays in the stream for other instances, and waits up to a second for new ones. The worker's `SubscriberConfig.Concurrency` bounds how many entries are handled at once. Entries are read in the order of the stream, but handled in parallel, and retried entries come back after newer ones, so there is no ordering by `Key`.

On shutdown, the subscriber stops reading within a second and waits for every running handler. It then deletes its consumer from the group, unless the consumer still has pending entries, such as one whose acknowledgement failed: deleting it would drop them from the group.

While Redis cannot be reached, is loading its data or fails over, the subscriber keeps trying, with a pause of a second after every failure. So it does while Redis refuses commands for a while: a script runs too long (`BUSY`), a snapshot failed (`MISCONF`), a master lacks the replicas it must write to (`NOREPLICAS`), or a master that turned into a replica unblocked the reads (`UNBLOCKED`). Any other error reply stops it, and the worker server stops the app, for example `redisstream: subscriber billing: read group billing of stream orders: NOGROUP …` once the group was deleted. When the group is listed in `Streams`, the restarted app creates it again.

#### Messages on the wire

A `worker.Message` is added to the stream `Message.Destination` as one entry:

| Field | From |
| --- | --- |
| `x-msg-id` | `Message.ID` |
| `x-msg-name` | `Message.Name` |
| `x-msg-timestamp` (Unix nanoseconds) | `Message.CreatedAt`, when set |
| every header | `Message.Headers` |
| `x-msg-key` (`redisstream.KeyField`) | `Message.Key`, when set |
| `payload` (`redisstream.PayloadField`) | `Message.Payload` |

The identity fields are written from the message over any header of the same name, and headers named `payload` or `x-msg-key` are not sent. Consuming reverses the mapping with `worker.NewMessageFromDelivery`: `Key` comes from `x-msg-key`, `Destination` is the stream, and every other field is a header. Entries of other producers without `x-msg-id` take the entry ID, which stays the same across redeliveries, and those without `x-msg-timestamp` the time in the entry ID, when Redis added the entry. Without `x-msg-name`, no handler receives them (`msg_no_handler`). Another producer only needs `XADD orders * x-msg-name order.placed payload '{"id":1}'`.

The worker settles each delivery:

| Worker | Redis | Effect |
| --- | --- | --- |
| `Complete` | `XACK` | The entry leaves the group's pending entries. It stays in the stream until it is trimmed. |
| `Retry` | `XCLAIM … IDLE … JUSTID` to `forgo-retry` | A consumer of the group claims the entry again after its [delay](#delayed-retries), or at once without `Retry`. |
| `Reject` | `XACK` | The entry is dropped from the group. With a [dead-letter stream](#dead-letter-streams), a copy is added there first. |

A settlement that fails, for example while Redis cannot be reached, leaves the entry pending, and another consumer claims it once it was idle for `ClaimIdle`. The inbox recognizes it by its `x-msg-id`. Acknowledging an entry that is no longer pending, because a duplicate settled it or it was trimmed, succeeds.

#### Claiming pending entries

A consumer that stops without settling its entries, because it crashed, was killed or lost its connection, leaves them pending in the group. Every subscriber with a free handler slot claims the entries of its group that were idle for `ClaimIdle`, about once a second and before it reads new entries, and handles them. So after a crash, its messages are delivered again about `ClaimIdle` later. Redis finds the entries that are due (`XPENDING … IDLE`), in one round trip however many entries wait for a retry, and the subscriber claims them (`XCLAIM`), so a large retry backlog never holds back new entries.

While a handler runs, its subscriber resets the idle time of the entry every `ClaimIdle`/3 (`XCLAIM … JUSTID`, which counts no delivery). A handler may therefore run longer than `ClaimIdle`, and only an instance that cannot reach Redis for `ClaimIdle`, for example because of a network partition, loses its entries to another consumer. Both then handle the message: the inbox completes it once, and a `HandlerConfig.Lock` keeps them from running at the same time.

`ClaimIdle` is also the longest retry delay. Raise it for longer delays, at the price of a slower recovery after a crash. Every subscriber of a group needs the same `ClaimIdle`.

An instance that crashed cannot delete its consumer on shutdown, so the consumer stays in the group once its entries are claimed. Every subscriber therefore sweeps its group about every five minutes (`XINFO CONSUMERS`), and deletes the consumers that have no pending entries and were idle for ten times `ClaimIdle` (ten minutes by default). It keeps its own consumer and `forgo-retry`. Since Redis 7.2, a live consumer is never that idle, since it reads about once a second or holds the entries it handles. Before 7.2, a consumer's idle time only resets when it gets entries, so the sweep also deletes the consumers of live instances while their group is quiet. Either way, a consumer that is deleted while it is alive loses nothing, since it has no pending entries, and Redis creates it again with the next entry it reads or claims. A sweep that fails, for example because the ACL user may not run `XINFO`, is left to the next one.

#### Delayed retries

By default, a retried entry is claimed again at once, within about a second. Without a delay, a downstream outage of a few seconds uses up every `MaxAttempt`, and a lock conflict or an inbox outage redelivers every second until it is resolved. `Retry` delays each redelivery:

```go
redisstream.SubscriberConfig{
	Name:   "billing",
	Stream: "orders",
	Retry:  redisstream.RetryConfig{Initial: time.Second, Max: 30 * time.Second},
}
```

The delay doubles with every failed attempt, from `Initial` up to `Max`, from the attempt the worker passes to `Retry`:

| Attempt | Delay |
| --- | --- |
| 1, and 0 for a failure before the attempt was counted (lock conflict, inbox outage) | `Initial` |
| n | `Initial` × 2ⁿ⁻¹, at most `Max` |

Redis Streams have no delays of their own. To retry, the subscriber hands the entry to the consumer `forgo-retry` of the group and sets its idle time to `ClaimIdle` minus the delay, so the consumers of the group claim it once the delay has passed. The waiting entry holds no handler slot and stays in the stream, and `XPENDING orders billing - + 10 forgo-retry` lists the entries waiting for a retry. A retry comes back up to about a second after its delay, and no delay is longer than `ClaimIdle`. If the hand-over fails, the entry is claimed again once it was idle for `ClaimIdle`.

| `RetryConfig` field | Default | Description |
| --- | --- | --- |
| `Initial` | disabled | Delay after the first failed attempt, at least 1ms. Zero claims retried entries again at once. |
| `Max` | `ClaimIdle` | The longest delay, at most `ClaimIdle`. |

**Validation.** `New` fails before connecting when `Retry` has a negative value, an `Initial` under 1ms, a value over `ClaimIdle`, or a `Max` under `Initial`.

#### Dead-letter streams

Entries the worker rejects are acknowledged and dropped from the group unless the subscriber has a dead-letter stream:

```go
redisstream.SubscriberConfig{
	Name:       "billing",
	Stream:     "orders",
	DeadLetter: redisstream.DeadLetterConfig{Enabled: true},
}
```

To reject an entry, the subscriber adds a copy of it to `orders:billing:dlq`, then acknowledges the entry. The copy keeps every field, with the message ID in `x-msg-id`. Redis creates the stream with the first dead letter. List it in `Streams` with a `MaxLen` to trim it.

| `DeadLetterConfig` field | Default | Description |
| --- | --- | --- |
| `Enabled` | `false` | Makes `Reject` copy entries to `Stream`. |
| `Stream` | `<stream>:<group>:dlq` | The dead-letter stream. Several subscribers may share one. |

**What ends up there.** Every message the worker rejects:

- a message that failed `MaxAttempt` times (the inbox marks it *failed*),
- a message whose payload cannot be bound (`msg_bind`, inbox *rejected*),
- a message no handler is registered for (`msg_no_handler`).

**A copy that cannot be added.** When the key holds another type, Redis is out of memory or does not answer within the worker's `SubscriberConfig.AckTimeout`, `Reject` fails with `redisstream: dead-letter <id> to <stream>, delivering it again in <delay>`, and the entry waits for the longest retry delay, `Retry.Max`, or `ClaimIdle` without `Retry`. It is then claimed again, and the worker rejects it again, until the copy is added. Coming back at once instead, 2,000 such entries took about 15,000 Redis commands and 4,000 inbox queries a second while the dead-letter stream could not be written.

Dead letters keep their `x-msg-id`, so the worker rejects a replayed message again, back to the dead-letter stream, while its inbox row is *failed* or *rejected*. To process one again, first set its inbox row back to *waiting* with zero attempts, then add it to the original stream again, for example with `XADD` of its fields. See forgo's [Retrying failed messages](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#retrying-failed-messages). Read them with `XRANGE orders:billing:dlq - +`, and delete one with `XDEL` once handled.

**Validation.** `New` fails before connecting when `DeadLetter.Stream` is the stream the subscriber reads.

#### Trimming

Redis keeps every entry of a stream until it is trimmed, also once every group acknowledged it. `MaxLen` trims the stream on every entry the broker adds to it, to about `MaxLen` entries: Redis trims whole nodes of its stream, so it keeps somewhat more. Trimming drops the oldest entries whether or not every group has read and acknowledged them, including the pending ones and the ones waiting for a retry, so a dropped entry is never delivered again: the first claim drops it from its group. Size `MaxLen` well above the backlog a group can fall behind by. Without `MaxLen`, trim the stream yourself, for example with `XTRIM orders MINID ~ <id>` from a cron job.

#### Durability and memory

Redis acknowledges an entry once it is in memory. It is on disk after the next `fsync` of the append-only file (`appendfsync everysec` by default), and it reaches replicas asynchronously, so a crash or a failover can lose entries Redis acknowledged, which the outbox no longer holds. For durable messages, enable the append-only file and keep the failover window small.

What a loss costs, as measured on a scaffold under steady load:

- **Without persistence,** a restart or a `FLUSHALL` loses the streams with their groups. Every subscriber then stops with `NOGROUP`, and with it its app and the app's outbox relay, so the instances that start again create the groups before any message is published to them. The messages lost are the entries that were not handled yet: 172, the backlog of the group, in a run whose consumers lagged, and none when they kept up. A producer that keeps publishing while no consumer runs, such as a deployment of its own, adds entries before the groups exist, and a group created at `$` never receives them.
- **With the append-only file,** a `docker kill` of Redis lost nothing, and the subscribers carried on: only the machine crashing loses the last second of writes.
- **A Sentinel failover,** graceful or after the master was killed, lost nothing either, since the replicas were in sync or the entries they missed had been handled already.

**Memory.** Run the broker's Redis with `maxmemory-policy noeviction`. Under an `allkeys-*` policy, Redis evicts a stream to make room, its groups with it: its entries are lost and the subscribers stop with `NOGROUP`. Under `noeviction`, a Redis that reached `maxmemory` refuses what would add data (`OOM`), and goes on with the rest:

- publishes fail, and the outbox keeps the messages until memory is freed;
- reads, claims, retries and acknowledgements go on, so the workers keep handling the entries the streams hold;
- dead-letter copies fail, and their entries wait (see [Dead-letter streams](#dead-letter-streams));
- a worker that starts only checks that its groups exist, and its consumer is created by its first read.

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
| `RunWith(ctx, db, opts, fn) error` | `Run` with the `*sql.TxOptions` of the transaction it begins, such as its isolation level. A call that joins a transaction ignores them. |
| `From(ctx, db) Executor` | The transaction on `db` that `ctx` carries, or `db` itself outside a transaction. `Executor` has `ExecContext`, `QueryContext` and `QueryRowContext`. |

**Nesting.** A `Run` or `Do` inside another on the same database joins the outer transaction instead of starting one, and the outermost call commits or rolls back everything. Transactions are keyed by `*sql.DB`, so contexts can carry transactions on several databases at once, and `From` never hands one database's transaction to another.

**In the worker.** For each message, the consumer runs your handler and the inbox's `MarkCompleted` inside `Transactor.Do`. With `sqltx` and the SQL inbox, your writes and the completion commit together. A handler that fails rolls back both, and the message is retried. A crash after the commit leads to a redelivery that the inbox skips. A concurrent duplicate of the message rolls back, because `MarkCompleted` fails for the delivery that completes second.

**In your services.** Save messages with the outbox's [`Save`](#inbox-and-outbox) inside the same `Do` as the business change, as in forgo's [producer example](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#producer-outbox-relay). The message is then published only if the change commits.

### inbox and outbox

SQL implementations of forgo's [storage contracts](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#storage-contracts) for the worker consumer (inbox) and producer (outbox). They use the `inbox_events` and `outbox_events` tables whose schema is given there. Create them with your migrations, for example with forgo's `migrate` command.

```go
import (
	"github.com/efegreat/forgo-adapter/provider/inbox"
	"github.com/efegreat/forgo-adapter/provider/outbox"
)

forgo.WithProviders(
	inbox.NewPostgres,  // or inbox.NewMySQL
	outbox.NewPostgres, // or outbox.NewMySQL
)
```

| Constructor | Database | Notes |
| --- | --- | --- |
| `inbox.NewPostgres(*sql.DB)` | PostgreSQL | `INSERT … ON CONFLICT` increments attempts in one statement. |
| `inbox.NewMySQL(*sql.DB)` | MySQL 8 | `INSERT … ON DUPLICATE KEY UPDATE` increments attempts in one statement. Stamps rows with `UTC_TIMESTAMP(6)` and deletes them by their age on the database clock. |
| `outbox.NewPostgres(*sql.DB)` | PostgreSQL | Claims with `FOR UPDATE SKIP LOCKED`. Stamps and expires claims with `CURRENT_TIMESTAMP`. |
| `outbox.NewMySQL(*sql.DB)` | MySQL 8 | Claims with `FOR UPDATE SKIP LOCKED`. Stamps claims and processed rows with `UTC_TIMESTAMP(6)`, and expires and deletes them by their age on the database clock. |

Both outboxes time claims on the database clock alone: a claim expires once its `processing_at` is `ClaimTTL` older than the database's current time. Clock skew between producer instances, or between them and the database, does not change `ClaimTTL`. On MySQL, every timestamp the storages stamp is UTC, whatever the session time zone and the driver's `loc`, and the cleanups compare ages on the database clock too. Create the tables with the schema forgo's REFERENCE.md gives for your database, including its indexes: without them, claims and releases scan the outbox backlog, and cleanup batches scan whole tables, locking every row they read under MySQL's `REPEATABLE READ`.

Concurrent deliveries of a new message each count an attempt: the first inserts its row, and the others wait for that row and count theirs on it. A message only leaves *waiting*. Moving it out of another terminal state fails with `inbox.ErrNotWaiting`. Repeating `MarkFailed` or `MarkRejected` on a message already in that state succeeds. `MarkCompleted` is strict: it succeeds only when it moves the message out of *waiting* itself. When two deliveries of one message run at once, for example after a channel was lost while a handler ran, the one that completes second fails with `inbox.ErrNotWaiting`. Its transaction then rolls back its handler's writes instead of committing them a second time, and its redelivery is skipped as completed. Several producer instances can run against one outbox, because a claim skips the rows another claim has locked.

**Saving messages.** Services save their messages with the outbox's `Save(ctx, msg)`, inside the `Do` of the change a message announces. The row joins that transaction, so the relay publishes the message if and only if the change commits. Outside a transaction, the row is committed at once. Request an interface of your own with that method, which bootstrap matches to the outbox by assignability, and provide no other type with that method, or the request is ambiguous:

```go
// Outbox saves messages in the transaction of ctx: outbox.NewPostgres.
type Outbox interface {
	Save(ctx context.Context, msg worker.Message) error
}

err = s.tx.Do(ctx, func(ctx context.Context) error {
	if err := s.orders.Insert(ctx, order); err != nil {
		return err
	}
	return s.outbox.Save(ctx, msg)
})
```

`Save` keeps `CreatedAt` to the microsecond, truncated, and a zero `CreatedAt` takes the database's time, UTC on MySQL. On MySQL, the driver writes `CreatedAt` in its `loc`, UTC by default, in which the storage reads it back. A message without an `ID` fails with `outbox.ErrNoID`: the consumers' inboxes recognize a message by its ID, so they would drop a second message without one as a duplicate of the first. A message whose ID the outbox holds already fails with the database's duplicate key error.

Both outboxes are `worker.OutboxBacklogStorage`s too: `Backlog` returns the number of rows not processed yet, the claimed ones included, counting no more than its limit, and the `created_at` of the first one by `id`, which is the oldest with `NewMessage`'s UUIDv7 IDs. The producer samples it every `BacklogInterval`, with `BacklogLimit` (10000 by default) as the limit, for an observer that implements `worker.IBacklogObserver`, such as [observer](#observer)'s. One query does both through the index of the pending rows (`outbox_events_pending` on PostgreSQL, `outbox_events_processed` on MySQL), in `id` order: the count reads no more than the limit of pending rows however far the relay is behind, and the oldest one is the first entry of the index.

Every statement runs through `sqltx.From`, so a storage called inside `sqltx.Transactor.Do` joins that transaction. The operations that need several statements (MySQL `IncrementAttempt`, both `Claim`s and `Complete`s) run in `sqltx.Run` or `sqltx.RunWith`, which starts a transaction of their own or joins the caller's.

On MySQL, the transactions these operations start are `READ COMMITTED`, whatever the session's isolation. Under `REPEATABLE READ`, InnoDB also locks the gaps between the index records a locking statement reads, and a claim or release can then block the application's outbox inserts until it commits. `READ COMMITTED` writes need `binlog_format` `ROW`, the default, or `MIXED`: under `STATEMENT`, InnoDB refuses them (error 1665). Joined to a transaction of yours, the storages run at its isolation.

### locker

`locker.Redis` is an `operation.ILocker`. It enables `Lock` on cron jobs, CLI commands and message handlers (see [Locks](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#locks)).

```go
import "github.com/efegreat/forgo-adapter/provider/locker"

forgo.WithResources(config.Redis()),
forgo.WithProviders(locker.NewRedis),
```

| Method | Behavior |
| --- | --- |
| `Lock(ctx, key, ttl)` | `SET key token NX PX ttl`, once, without waiting. Returns whether the lock was acquired, and an error when Redis fails or `ttl` is not positive: a lock without an expiry would outlive a crash of its holder. |
| `WaitLock(ctx, key, attempts, delay, ttl)` | Tries up to `attempts` times (at least once), waiting `delay` between tries but not after the last one. Returns `ctx.Err()` when `ctx` ends first. forgo's servers never call it; it is for application code that waits for a lock. |
| `Unlock(ctx, key)` | Deletes the key only while it still holds this process's token. Otherwise returns `locker.ErrNotHeld` and leaves the key alone. |

All three run on the caller's context. forgo's `Locker` middleware passes the operation's context to `Lock`, and to `Unlock` a copy that is never canceled, so that a lock is released even after its operation's context ended.

The token is random per `locker.Redis`, so one instance per process. A lock that expires while its operation still runs can be taken by another process. The late `Unlock` then fails with `ErrNotHeld` instead of releasing the other process's lock, and forgo's `Locker` middleware logs that failure at critical severity. A TTL that is too short for the operation shows up in your logs this way. Size `Lock.TTL` to the longest expected run, every retry included.

Keys are used as given. forgo builds them from your `Lock.Key`, for example `<Key>-<subscriber>-<messageID>` for message handlers.

### ratelimit

`ratelimit.Redis` is a forgo `ratelimit.ILimiter`. Use it with the `RateLimitWith` middleware of the HTTP or the gRPC server to enforce a rate limit across every instance of your application (see [Rate limiting](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#rate-limiting)).

```go
import "github.com/efegreat/forgo-adapter/provider/ratelimit"

forgo.WithResources(config.Redis()),
forgo.WithProviders(ratelimit.NewRedis),

// on a route or the server
http.RateLimitWith(http.RateLimitConfig{Limit: 5, Period: time.Minute, Prefix: "login:"})
// on a gRPC service or method
fgrpc.RateLimitWith(fgrpc.RateLimitConfig{Limit: 100, Period: time.Minute, Prefix: "grpc:"})
```

It implements the generic cell rate algorithm (GCRA), the algorithm of forgo's `ratelimit.Memory`, so a `Rate` means the same with either limiter: `Limit` requests per `Period`, up to `Burst` at once, refilled continuously.

- **Atomic.** Each `Allow` is one Lua script that reads and writes a single key, so concurrent requests from any number of instances are counted exactly. It works on Redis Cluster.
- **One clock.** Time comes from the Redis server's `TIME`, so clock skew between app instances does not change the limit.
- **Small.** A key stores one timestamp and expires when its burst has fully recovered.
- **Keys.** Stored as `ratelimit:<Prefix><key>` (`ratelimit.KeyPrefix`). Give every `RateLimitWith` middleware its own `Prefix`.
- **Resolution.** Microseconds. Rates above one request per microsecond per key are counted as one per microsecond.
- **Failures.** A Redis error is returned to the middleware. By default the request fails with `rate_limit_unavailable` (503 over HTTP, `Unavailable` over gRPC). With `RateLimitConfig.FailOpen` it is admitted and the failure is logged.

### liveconfig

Stores and a cache for forgo's [live configs](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#live-configs). `liveconfig.Postgres` and `liveconfig.MySQL` are `forgo.LiveConfigStore`s: the source of truth, in the `live_configs` table whose schema is given there, one row per config name. Create it with your migrations. `liveconfig.RedisCache` is a `forgo.LiveConfigCache` that every instance of your app shares.

```go
import "github.com/efegreat/forgo-adapter/provider/liveconfig"

forgo.WithResources(config.Database(), config.Redis()),
forgo.WithProviders(
	liveconfig.NewPostgres,   // or liveconfig.NewMySQL
	liveconfig.NewRedisCache,
	forgo.NewLiveConfigs,
)
```

| Method | Behavior |
| --- | --- |
| SQL `Get(ctx, name)` | `SELECT data, version` of the row. A missing row returns version 0 and no error. |
| SQL `Set(ctx, name, data, version)` | Version 0 inserts the row at version 1. Any other version updates the row only `WHERE version = ?`, to `version + 1`. When no row was written, it returns `forgo.ErrLiveConfigConflict`. Sets `updated_at` to `CURRENT_TIMESTAMP` on PostgreSQL and `UTC_TIMESTAMP(6)` on MySQL. |
| Redis `Get(ctx, name)` | `HGET liveconfig:<name> data`. A missing entry returns `false` and no error. |
| Redis `Set(ctx, name, data, version)` | One Lua script that writes the hash `liveconfig:<name>` (`version`, `data`) unless it holds the same or a newer version. Entries never expire. |

- **Conflicts.** The conditional write is atomic, so of two saves from the same version, exactly one succeeds. PostgreSQL inserts with `ON CONFLICT (name) DO NOTHING`. MySQL inserts plainly, and when the insert fails, reads the row: a stored row means a conflict, and no row means the insert's own error.
- **Outside transactions.** Unlike the inbox and outbox, the SQL stores never join the transaction a context carries. forgo caches a document as soon as its save succeeds, and the cache never expires, so a save must be committed when `Set` returns.
- **Text parameters.** Documents are sent as text, because a JSON column rejects `[]byte` from some drivers: lib/pq sends it as `bytea`, and MySQL refuses JSON from the binary character set.
- **Newest version wins.** The cache compares versions in the script, so a save whose cache write arrives late never replaces a newer one. Versions are compared as Lua numbers, exact up to 2^53.
- **Keys.** Stored as `liveconfig:<name>` (`liveconfig.KeyPrefix`). Each entry is a single key, so the cache works on Redis Cluster. Configure Redis without eviction for these keys: an evicted entry is read from the store on every `Load` until the config is saved again.
- **One namespace per Redis.** Applications that share a Redis share these keys. When two of them have a config of the same name, the one at the higher version wins: the other's saves are refused by the version check, and its `Load` returns a document of the other application without an error. Give such configs names that are unique across the applications, or give each application a Redis database of its own.

---

## Metrics (OpenTelemetry)

The metrics of forgo applications live in a module of their own, `github.com/efegreat/forgo-adapter/otel`, so that the other adapters never pull in OpenTelemetry's SDK, its exporters or the Prometheus client:

```sh
go get github.com/efegreat/forgo-adapter/otel
```

It has two packages: `meter`, a resource that builds OpenTelemetry's `MeterProvider`, and `observer`, the [`operation.IObserver`](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#metrics) that records forgo's metrics with it. List both, and every server records metrics:

```go
import (
	"github.com/efegreat/forgo-adapter/otel/meter"
	"github.com/efegreat/forgo-adapter/otel/observer"
)

app := forgo.NewApp(
	forgo.WithResources(meter.Config{ServiceName: "orders"}),
	forgo.WithProviders(observer.New(observer.Config{})),
	forgo.WithServers(config.HTTP, config.GRPC, config.Consumer),
)
```

### meter

`meter.Config` is a resource whose `Value` is a `*meter.Provider`, the SDK's `MeterProvider`. Providers request it as `metric.MeterProvider` (`go.opentelemetry.io/otel/metric`), as `observer.New` does. The zero value pushes to the OpenTelemetry collector that the standard `OTEL_EXPORTER_OTLP_*` environment variables name, every minute.

| Field | Default | Description |
| --- | --- | --- |
| `Name` | `"meter"` | The resource's name in the bootstrap. |
| `ServiceName` | `OTEL_SERVICE_NAME`, else the SDK's `unknown_service:<executable>` | The `service.name` of the metrics. The other resource attributes come from `OTEL_RESOURCE_ATTRIBUTES`. |
| `Exporter` | `meter.ExporterOTLP` | How metrics leave the process: `ExporterOTLP` pushes, `ExporterPrometheus` serves them to Prometheus through `meter.PrometheusRoute`, and `ExporterNone` exports only through `Readers`. |
| `OTLP.Protocol` | `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` or `OTEL_EXPORTER_OTLP_PROTOCOL`, else `meter.OTLPHTTP` | `OTLPHTTP` (`http/protobuf`, port 4318) or `OTLPGRPC` (`grpc`, port 4317). |
| `OTLP.Endpoint` | `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT`, else localhost | The collector's `host:port`. |
| `OTLP.Insecure` | `false` | Sends without TLS. |
| `OTLP.Headers` | | Headers of every export, such as an API key. |
| `OTLP.Interval` | `OTEL_METRIC_EXPORT_INTERVAL`, else 1 minute | The time between two exports. |
| `Readers` | | Readers besides the exporter's, such as an `sdkmetric.ManualReader` in tests, or an exporter of your own with `ExporterNone`. |
| `DisableRuntime` | `false` | Leaves out the Go runtime metrics (`go.memory.used`, `go.goroutine.count`, …) of OpenTelemetry's runtime instrumentation. |
| `Global` | `false` | Makes the provider OpenTelemetry's global `MeterProvider`, for libraries that take it from there, such as otelgrpc's stats handler in the `StatsHandlers` of the gRPC server's config. |
| `ShutdownTimeout` | 5 seconds | Bounds the last export of `Close`. |

- **The collector may be down.** The OTLP exporter connects lazily, so an unreachable collector does not stop the application. Failed exports go to OpenTelemetry's error handler, which logs them to stderr unless you call `otel.SetErrorHandler`.
- **The last metrics are exported.** `Close` shuts the provider down, which exports what was recorded since the last export. The app closes resources once every server stopped, so the last operations are in it. A `Close` whose export fails returns the error, and the app logs it.

#### Prometheus

With `Exporter: meter.ExporterPrometheus`, Prometheus pulls the metrics from the route `meter.PrometheusRoute()` returns: `GET /metrics`, named `metrics`, in Prometheus's text format. Its scrapes are not logged (`ArgKeyRecorderDisabled`) and it is left out of API documents (`DisableAPIDoc`). Without the Prometheus exporter, the server that routes it fails at startup.

```go
var Admin = http.Config{
	ServerConfig: forgo.ServerConfig{Name: "admin"}, // logged and observed apart from the API's server
	Addr:         ":9100",                           // a port of its own, which the cluster does not expose
	Routes:       []http.RouteConfig{meter.PrometheusRoute(), {Name: "health", Method: "GET", Path: "/health", Handler: http.HealthHandler()}},
}

forgo.WithResources(meter.Config{Exporter: meter.ExporterPrometheus}),
forgo.WithServers(config.API, Admin),
```

Both servers are HTTP servers, which forgo names `http` by default. `ServerConfig.Name` names the admin server apart, so that its logs, its connection metrics (`forgo.server.name`) and forgo's `forgotest` tell it from the API's.

Metric names follow Prometheus's conventions: `forgo.operation.duration` is served as `forgo_operation_duration_seconds`, and attributes as labels with underscores.

#### Business metrics

Services record their own metrics with the same provider, which the bootstrap gives their constructors:

```go
func NewOrderService(mp metric.MeterProvider, repo OrderRepository) (*OrderService, error) {
	placed, err := mp.Meter("orders").Int64Counter("orders.placed", metric.WithUnit("{order}"))
	if err != nil {
		return nil, err
	}
	return &OrderService{repo: repo, placed: placed}, nil
}
```

### observer

`observer.New(cfg)` returns the provider of the observer, a `func(metric.MeterProvider) (operation.IObserver, error)`. Every forgo server picks it up. It also implements the interfaces of the [signals outside operations](https://github.com/efegreat/forgo/blob/main/REFERENCE.md#signals-outside-operations): `worker.IDeliveryObserver`, `worker.IRelayObserver`, `worker.IBacklogObserver`, `operation.ILockObserver` and `transport.IConnObserverFactory`. A config it rejects fails the bootstrap.

| Field | Default | Description |
| --- | --- | --- |
| `Buckets` | 5 ms to 10 s, the HTTP semantic conventions' | Boundaries, in seconds, of `forgo.operation.duration`, `http.server.request.duration` and `forgo.outbox.batch.duration`. They must increase strictly. |
| `LagBuckets` | 5 ms to 1 h | Boundaries, in seconds, of `forgo.message.lag`. |
| `DisableHTTPSemconv` | `false` | Leaves out `http.server.request.duration`. |
| `ConnIO` | `false` | Also counts the read and write errors of connections. It wraps every connection, which hides the zero-copy paths of `*net.TCPConn`. |

| Instrument | Kind | Attributes |
| --- | --- | --- |
| `forgo.operation.duration` | histogram (s) | `forgo.operation.type`, `forgo.operation.name`, `forgo.operation.status` (`ok`, or the `operation.Status` of the error, such as `not_found`) and, for failures, `forgo.error.code`. The same on every transport. Its count is the number of operations. |
| `forgo.operation.active` | up-down counter | `forgo.operation.type`, `forgo.operation.name` |
| `http.server.request.duration` | histogram (s) | The stable HTTP semantic conventions: `http.request.method` (`_OTHER` for unknown methods), `url.scheme`, `http.response.status_code`, `http.route` (the route's path template, none for `not_found`), `network.protocol.version`, and `error.type` for a 5xx. A hijacked connection is left out. |
| `forgo.message.settled` | counter | `forgo.subscriber`, `forgo.operation.name`, `forgo.settlement.kind` (`completed`, `retried`, `rejected`), `forgo.settlement.duplicate`, `forgo.settlement.failed` (the broker did not take it) |
| `forgo.message.lag` | histogram (s) | `forgo.subscriber`, `forgo.operation.name`. The time from a message's creation to the start of its handling, for messages whose producer sent it. |
| `forgo.outbox.published`, `forgo.outbox.failed` | counters | none. Failed messages go back to the outbox. |
| `forgo.outbox.batch.duration` | histogram (s) | `forgo.outbox.batch.failed`. Every batch is recorded, the empty ones included, so its count is the relay's heartbeat. |
| `forgo.outbox.backlog` | gauge | none. The outbox messages not published yet, sampled every `BacklogInterval` by each producer instance from an outbox that reports its backlog, such as [outbox](#inbox-and-outbox)'s: aggregate instances with `max`. It stops at the producer's `BacklogLimit`. |
| `forgo.outbox.backlog.age` | gauge (s) | none. The age of the oldest of them, 0 when there is none. It grows when the broker is down or the relay falls behind, also past `BacklogLimit`, so alert on it. |
| `forgo.lock.held`, `forgo.lock.failed` | counters | `forgo.operation.type`, `forgo.operation.name`. Never the lock key, which can hold a message ID. |
| `forgo.conn.accept_errors` | counter | `forgo.server.name`, `forgo.server.address` |
| `forgo.conn.opened` | counter | `forgo.server.name`, `forgo.server.address`. Its rate is the connection churn. |
| `forgo.conn.open` | up-down counter | `forgo.server.name`, `forgo.server.address`, `forgo.conn.state`: `new`, `active`, `idle` or `hijacked` on HTTP, `open` on gRPC, whatever the servers are named. The observer tells them apart by `transport.Server.ReportsActivity`. |
| `forgo.conn.io_errors` | counter | `forgo.server.name`, `forgo.server.address`, `forgo.conn.direction` (`read`, `write`). With `ConnIO` only. |

- **No request counter.** A histogram's count is the number of operations, with the same labels.
- **gRPC's semantic conventions** come from otelgrpc's stats handler in the gRPC server's `StatsHandlers`, with `meter.Config.Global` so that it records with this provider. The observer does not record them again.
- **Bounded labels.** Every attribute comes from configuration (route, job, handler, command and subscriber names, addresses) or from bounded sets (statuses, error codes, settlement kinds, HTTP methods and status codes). Nothing is labelled by an ID, a path or a message name a producer chose.
- **No allocation.** The observer builds the attribute set of a label combination once and reuses it, so recording an operation allocates nothing once its labels were seen.

---

## A complete worker app

```go
// config/config.go

// The resources are functions, so that tests can set DATABASE_URL and REDIS_ADDR before they are read.
func DB() sqldb.Config {
	return sqldb.Config{Name: "main", DriverName: "postgres", DSN: forgo.Env("DATABASE_URL", "")}
}

func Redis() redisdb.Config {
	return redisdb.Config{Name: "cache", Addrs: []string{forgo.Env("REDIS_ADDR", "localhost:6379")}}
}

func Broker() rabbitmq.Config {
	return rabbitmq.Config{
		Name:      "events",
		Exchanges: []rabbitmq.ExchangeConfig{{Name: "orders", Kind: amqp.ExchangeTopic, Durable: true}},
		Queues: []rabbitmq.QueueConfig{{
			Name:       "billing",
			Durable:    true,
			Bindings:   []rabbitmq.BindingConfig{{Exchange: "orders", RoutingKey: "order.*"}}, // Message.Key, such as order.placed
			DeadLetter: rabbitmq.DeadLetterConfig{Enabled: true, Args: map[string]any{"x-message-ttl": 7 * 24 * 3600 * 1000}},
		}},
		Publishers:  []rabbitmq.PublisherConfig{{Name: "order.placed"}},
		Subscribers: []rabbitmq.SubscriberConfig{{Name: "billing", Queue: "billing"}},
	}
}

var (
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
				Lock:    operation.Lock{Key: "order-placed", TTL: time.Minute}, // locker.Redis
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
	app := forgo.NewApp(
		forgo.WithName("shop-worker"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB(), config.Redis(), config.Broker()),
		forgo.WithProviders(config.Adapters...),
		forgo.WithProviders(config.Services...),
		forgo.WithServers(config.Consumer, config.Producer),
	)
	if err := app.Run(); err != nil { // logged; main picks the exit code
		os.Exit(1)
	}
}
```

---

## Errors

Every error is prefixed with its package and wraps its cause, so `errors.Is` and `errors.As` see the driver error.

| Prefix | Raised by |
| --- | --- |
| `sqldb:` | A negative `Logger.SlowThreshold`, and opening or pinging the database. |
| `redisdb:` | Validation (empty addresses, `ClusterMode` with `MasterName`, an unknown `ReadFrom`, a `DB` on a cluster) and pinging Redis. |
| `rabbitmq:` | Validation, connection, topology, channels, unknown publisher or subscriber names and missing queues, a closed delivery channel, nacked and unroutable publishes, publishes whose channel was lost before the broker confirmed them, retries that could not be delayed, acknowledgements of deliveries from a lost channel, consumers the recovery could not subscribe again. |
| `pubsub:` | Validation, the client, creating and checking topics and subscriptions (`pubsub: subscription exists with other settings`), unknown subscriber names, publishes without a `Destination` or after `Close`, failed publishes, receiving, acknowledgements, dead-letter copies that could not be published. |
| `redisstream:` | Validation, connecting (wrapping the `redisdb:` error), creating groups, unknown subscriber names and missing streams or groups, publishes without a `Destination` and failed publishes, reads and claims that stopped a subscriber, acknowledgements, retries and dead-letter copies that failed, deleting a consumer on `Close`. |
| `sqltx:` | Beginning, committing or rolling back a transaction. Errors returned by your function pass through unchanged. |
| `inbox:` | Inbox queries. `inbox.ErrNotWaiting` when a message left the waiting status. |
| `outbox:` | Outbox queries. |
| `locker:` | A TTL that is not positive, and Redis errors. `locker.ErrNotHeld` from `Unlock`. |
| `ratelimit:` | An invalid `Rate` and Redis errors. |
| `liveconfig:` | Live config queries and Redis errors. Conflicting saves return forgo's `forgo.ErrLiveConfigConflict`. |
| `meter:` | Validation (an unknown exporter or OTLP protocol, `ExporterNone` without `Readers`, negative durations), building the exporter or the runtime metrics, `PrometheusRoute` without `ExporterPrometheus`, and the last export of `Close`. |
| `observer:` | Boundaries that do not increase, a nil `MeterProvider`, and creating the instruments. |
