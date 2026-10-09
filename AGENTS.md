# AGENTS.md

Guide for coding agents and contributors working **on forgo-adapter itself**. For how to *use* the adapters, see [REFERENCE.md](REFERENCE.md). [README.md](README.md) is the public entry point.

## What forgo-adapter is

The concrete infrastructure for [forgo](https://github.com/efegreat/forgo): resources (SQL, Redis, RabbitMQ) and providers (a SQL transactor, inbox/outbox storages, a Redis locker, a Redis rate limiter) that implement forgo's interfaces. Module path: `github.com/efegreat/forgo-adapter` ([repository](https://github.com/efegreat/forgo-adapter)). Go 1.27. Both repositories are private and untagged before the first release, so `go.mod` requires `github.com/efegreat/forgo v0.0.0` and replaces it with `../forgo`: a sibling forgo checkout is required to build. At release, require a tagged forgo version and remove the `replace` directive.

The dependency points one way. **Adapters import forgo; forgo never imports adapters.** Anything a server needs from an adapter is an interface declared in forgo. When a forgo contract changes (`worker.IBroker`, `worker.InboxStorage`, `operation.ILocker`, `http.IRateLimiter`, …), update the adapters here in the same change set.

forgo's [AGENTS.md](https://github.com/efegreat/forgo/blob/main/AGENTS.md) applies here too: its design principles, coding conventions and logging rules. This file only adds what is specific to adapters.

## Design principles

forgo's four goals carry over:

1. **Simplicity.** A resource is a config value, a provider is a constructor. Zero fields mean defaults, filled by `Sanitize()`. Optional features, such as `rabbitmq.DeadLetterConfig`, are config structs whose zero value turns them off.
2. **Speed.** Connections, topology and scripts are set up in `New` and in constructors. Hot paths, such as `Allow`, `Lock` or `Publish`, do one round trip and no reflection, and Redis scripts are sent with `EVALSHA`.
3. **Abstraction.** An adapter implements a forgo contract and nothing more. It does not log, and it reports through returned errors. Logging, retries and acknowledgement policy belong to the forgo servers.
4. **Scalability.** Shared-state adapters must be correct with many app instances: atomic Redis scripts, `FOR UPDATE SKIP LOCKED` claims, the Redis server clock.

## Repository map

| Path | Responsibility |
| --- | --- |
| `resource/sqldb/` | `Config` → `*sql.DB`. Applies pool limits, pings with `ConnectTimeout`, closes the pool if the ping fails. |
| `resource/redisdb/` | `Config` → `*redis.Client`. Pings with `ConnectTimeout`, closes the client if the ping fails. |
| `resource/broker/rabbitmq/` | `Config` → `worker.IBroker`. `config.go`: config, `Sanitize`, `validate`, `RecoveryConfig`, `DeadLetterConfig`, queue args. `broker.go`: connection, owned channels, `declare` (topology through the `declarer` interface), publisher and subscriber factories. `recovery.go`: `ackGuard` and the `recoveryHooks` around the client's connection recovery. `publisher.go`: confirm-mode publishing, mandatory returns, `Message` → `amqp.Publishing` with identity headers. `subscriber.go`: prefetch, `Consume` over a `process.ConcurrentRunner`. `delivery.go`: `amqp.Delivery` → `worker.IDelivery` with guarded acknowledgements. |
| `provider/sqltx/` | `operation.Transactor` on `*sql.DB`. `Run`, `From`, `Executor`: transactions carried in the context, keyed by database. |
| `provider/inbox/` | `worker.InboxStorage` for PostgreSQL (`postgres.go`) and MySQL 8 (`mysql.go`). `ErrNotWaiting`. |
| `provider/outbox/` | `worker.OutboxStorage` for PostgreSQL and MySQL 8. `outbox.go`: the shared row scan. |
| `provider/locker/` | `operation.ILocker` on Redis. Token-guarded unlock script, `ErrNotHeld`. |
| `provider/ratelimit/` | `http.IRateLimiter` on Redis. GCRA Lua script on the server clock, `KeyPrefix`. |

## Invariants

Do not break these without an explicit decision from the maintainer.

### Resources

- `Config` is a value type with value receivers. `Sanitize()` returns a copy with defaults. It **never writes into the caller's slices or maps**, because configs are package-level vars shared by several apps and tests. Build new slices, and clone maps before adding keys (see `QueueConfig.args`).
- `New()` follows this order: `Sanitize` → `validate` (no network) → connect → verify (ping, declare). On any failure after connecting, close what was opened and join the close error with `errors.Join`.
- `Value()` returns the concrete client (`*sql.DB`, `*redis.Client`) or, for brokers, the forgo interface (`worker.IBroker(r)`). Bootstrap binds the dynamic type of `Value()` and matches by assignability. Document in REFERENCE.md what providers should request.
- `Close()` releases everything `New` opened.

### Providers

- One exported constructor per implementation, `NewX(dep) *X`, listed directly in `forgo.WithProviders`. Return the concrete pointer. Bootstrap matches it to the forgo interface by assignability, and returning an interface would hide the type from other providers.
- Ask for the narrowest dependency that works with every deployment shape: `*sql.DB` for SQL and `redis.UniversalClient` for Redis, so cluster and failover clients plug in.
- Every implementation has a compile-time assertion in its tests: `var _ operation.ILocker = (*Redis)(nil)`.

### RabbitMQ

- Published messages carry `worker.MessageIDHeader`, `MessageNameHeader` and `MessageTimestampHeader`, written from the `Message` fields over any user header of the same name. The consumer inbox depends on a stable ID across redeliveries.
- `Delivery`: `Complete` = ack, `Retry` = nack with requeue, `Reject` = reject without requeue. The dead-letter feature relies on `Reject` never requeueing. When the queue has `Retry`, `Retry` instead publishes a confirmed copy to the retry queue for its attempt and then acks through the `ackGuard`. It checks staleness before publishing, so a stale delivery is not duplicated, and it falls back to nack with requeue when the publish fails, so the message is never held or lost. The retry publisher is behind the `confirmedPublisher` interface for tests.
- `declare` declares a queue's retry queues **after** the queue. They are durable, and dead-letter through the default exchange (`x-dead-letter-exchange: ""`) with the queue's name as routing key, so an expired retry returns to that queue only. Their names derive from the delays, so changing `Retry` declares new ones and never redeclares an existing queue with other arguments.
- `declare` declares a queue's dead-letter exchange and queue **before** the queue. Dead-letter exchanges are durable direct exchanges and dead-letter queues are durable. Conflicts between `DeadLetter` and `x-dead-letter-*` in `Args` are rejected in `validate`.
- `Consume` waits for every handler it started (`runner.Wait`) before returning. It returns `nil` when `ctx` ends and an error when the delivery channel closes.
- Recovery is the client's (amqp091-go `Config.Recovery`), wrapped by `recoveryHooks`. The hooks recover only channels in `Broker.owned`. They also advance the `ackGuard` of a subscriber channel before the client reopens it, and the guard's read lock spans every acknowledgement. Never acknowledge a delivery around the guard: delivery tags restart on a recovered channel, so a stale tag settles another message.
- The topology channel stays open for the broker's lifetime, because the client forgets the topology recorded on a closed channel.
- `NewSubscriber` checks the queue with a passive declare **before** the channel is owned, so the channel the broker closes on a missing queue is not recovered. Prefetch (`basic.qos`) must precede `basic.consume`, which is why consuming starts in `Consume`, where the worker's concurrency is known.
- Publisher channels are in confirm mode. `Publish` holds the publisher's mutex from publish to confirm, so one message is in flight per channel and a mandatory return can be matched to it by message ID.

### SQL storages

- Every statement goes through `sqltx.From(ctx, db)`, and every multi-statement operation through `sqltx.Run`, so storages join the transaction of `sqltx.Transactor.Do`. The worker relies on it: `MarkCompleted` must commit or roll back with the handler.
- Each storage implements the contract and the comments in forgo's `server/worker/storage.go` exactly. The table schemas are documented in forgo's REFERENCE.md ("Storage contracts").
- Claims use `FOR UPDATE SKIP LOCKED`. Status transitions only leave `InboxWaiting`. Repeating `MarkFailed` or `MarkRejected` succeeds, but `MarkCompleted` must succeed only when it moved the row itself (`unmoved` in `provider/inbox/inbox.go`): it runs in the handler's transaction, and a success for a concurrent duplicate would commit the handler's writes twice.
- Each dialect lives in its own file. Placeholders differ (`$n` and `?`): keep the helpers local to the file that uses them.

### Redis

- Read-modify-write logic runs as one Lua script (`redis.NewScript`) that touches a single key, so it is atomic and works on Redis Cluster.
- Time-based logic uses Redis `TIME`, never the app clock.
- Keys the adapter invents are namespaced (`ratelimit.KeyPrefix`). Keys passed in by forgo, such as lock keys, are used as given.

## Coding conventions

- Errors: prefix with the package (`sqldb:`, `redisdb:`, `rabbitmq:`, `inbox:`, `outbox:`, `locker:`, `ratelimit:`), wrap with `%w`, and declare sentinels as package-level `var`s.
- Doc comments on exported identifiers explain the contract and the why. Match the surrounding code.
- Dependencies: forgo, `github.com/rabbitmq/amqp091-go`, `github.com/redis/go-redis/v9`. Test-only: `github.com/alicebob/miniredis/v2`, `github.com/DATA-DOG/go-sqlmock`. A new adapter may add the client library it wraps, and nothing else without a strong reason.
- Tests must not need running infrastructure. Use miniredis for Redis (it runs Lua and `TIME`, and `SetTime`/`FastForward` control the clock), sqlmock for SQL (`sqlmock.NewWithDSN` plus `sql.Open("sqlmock", dsn)` exercises `sqldb.New`; storage tests set `SetMaxOpenConns(1)`, so a statement that bypasses an open transaction blocks and fails on the test's deadline), and fakes of narrow interfaces for AMQP (`declarer`, `consumeChannel`, `publishChannel`, `confirmation` and `amqp.Acknowledger`). Table-driven where possible, passing under `-race`.

## Workflow

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .        # must print nothing
```

When you change behavior:

1. Update [REFERENCE.md](REFERENCE.md) in the same change whenever a config field, default, constructor, wire format or error changes. Update [README.md](README.md) only if the pitch, the table or the headline example is affected.
2. If an adapter is added or renamed, update the Adapters section of forgo's REFERENCE.md and the adapter row of its README.md.
3. If you fix an item from [Known gaps](#known-gaps), remove it. If you find a new one, add it.

### Checklists

**New resource**
- [ ] `Config` value type, `Sanitize()` that never mutates shared slices or maps, `New() (forgo.Resource, error)`.
- [ ] Validation before connecting. Verification (ping, declare) in `New`. Cleanup on every failure path.
- [ ] `Value()` type documented, and tests for `Sanitize`, a successful `New` and a failing one.
- [ ] REFERENCE.md section with a config table (field, default, description), plus rows in the overview table and README.

**New provider**
- [ ] `NewX(dep) *X` with the narrowest dependency type.
- [ ] Compile-time interface assertion in tests, and tests against miniredis or sqlmock.
- [ ] Atomic under concurrent instances (single script or `SKIP LOCKED`).
- [ ] REFERENCE.md section, plus rows in the overview table and README.

**New broker**
- [ ] Resource whose `Value()` is `worker.IBroker`, with named publishers and subscribers matching worker config names.
- [ ] Identity headers written on publish and read back through `worker.NewMessageFromDelivery`.
- [ ] `Complete`/`Retry`/`Reject` semantics as in forgo's broker contracts, and `Consume` that joins its handlers.
- [ ] A dead-letter option if the broker supports one.

## Known gaps

When you find one, verify it against the code and add it here with a `file:line` reference. Delete it in the change that fixes it.

- The RabbitMQ adapter is tested against fakes of the client only. Recovery, confirms and dead-lettering have not run against a live broker in this repository's tests.
- A consumer whose re-subscription fails during recovery is skipped by the client's default `OnTopologyEntityError`, so its subscriber stays connected without receiving messages (`resource/broker/rabbitmq/recovery.go:85`).
- Non-string AMQP header values are dropped on consume (`resource/broker/rabbitmq/delivery.go:26`).
- The PostgreSQL outbox stamps `processing_at` with the database clock but compares it with `ExpiredBefore` from the application clock, so clock skew shifts `ClaimTTL` (`provider/outbox/postgres.go:54`).
