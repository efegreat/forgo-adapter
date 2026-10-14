# AGENTS.md

Guide for coding agents and contributors working **on forgo-adapter itself**. For how to *use* the adapters, see [REFERENCE.md](REFERENCE.md). [README.md](README.md) is the public entry point.

## What forgo-adapter is

The concrete infrastructure for [forgo](https://github.com/efegreat/forgo): resources (SQL, Redis, RabbitMQ, Google Cloud Pub/Sub, Redis Streams) and providers (a SQL transactor, inbox/outbox storages, live config stores and a live config cache, a Redis locker, a Redis rate limiter) that implement forgo's interfaces. Module path: `github.com/efegreat/forgo-adapter` ([repository](https://github.com/efegreat/forgo-adapter)). Go 1.27. Both repositories are private and untagged before the first release, so `go.mod` requires `github.com/efegreat/forgo v0.0.0` and replaces it with `../forgo`: a sibling forgo checkout is required to build. At release, require a tagged forgo version and remove the `replace` directive.

The dependency points one way. **Adapters import forgo; forgo never imports adapters.** Anything a server needs from an adapter is an interface declared in forgo. When a forgo contract changes (`worker.IBroker`, `worker.InboxStorage`, `operation.ILocker`, `http.IRateLimiter`, `forgo.LiveConfigStore`, `forgo.LiveConfigCache`, …), update the adapters here in the same change set.

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
| `resource/redisdb/` | `Config` → `redis.NewUniversalClient`: a `*redis.Client` for a server or a Sentinel master, a `*redis.ClusterClient` for a cluster. `ReadFrom` routes read-only commands to replicas; for a Sentinel master it builds `redis.NewFailoverClusterClient` itself, because the universal client's `ReadOnly` would send writes to a replica too. Validates the mode, `ReadFrom` and `DB` before connecting, pings with `ConnectTimeout`, closes the client if the ping fails. |
| `resource/broker/rabbitmq/` | `Config` → `worker.IBroker`. `config.go`: config, `Sanitize`, `validate`, `RecoveryConfig`, `DeadLetterConfig`, queue args. `broker.go`: connection, owned channels, `declare` (topology through the `declarer` interface), publisher and subscriber factories. `recovery.go`: `ackGuard`, `consumerLoss` and the `recoveryHooks` around the client's connection and topology recovery. `publisher.go`: confirm-mode publishing, mandatory returns, `Message` → `amqp.Publishing` with identity headers. `subscriber.go`: prefetch, `Consume` over a `process.ConcurrentRunner`. `delivery.go`: `amqp.Delivery` → `worker.IDelivery` with guarded acknowledgements, and AMQP header values → strings (`headerValue`). |
| `resource/broker/pubsub/` | `Config` → `worker.IBroker` on `cloud.google.com/go/pubsub/v2`. `config.go`: config, `Sanitize`, `validate`, `RetryConfig`, `DeadLetterConfig`, the `*pb.Subscription` `New` creates, and full topic and subscription names. `broker.go`: client, `declare` (create topics and subscriptions, compare existing ones), subscriber factory. `publisher.go`: `Message` → Pub/Sub message with identity attributes, and `topics`, the client publishers shared per topic. `subscriber.go`: `Receive` with `MaxOutstandingMessages` and a concurrency semaphore. `delivery.go`: Pub/Sub message → `worker.IDelivery`, dead-letter copies. |
| `resource/broker/redisstream/` | `Config` → `worker.IBroker` on go-redis v9, over a client of its own built from a nested `redisdb.Config`. `config.go`: config, `Sanitize`, `validate`, `StreamConfig` (`MaxLen`, `Groups`), `RetryConfig` (`delay`), `DeadLetterConfig`, the wire field names. `broker.go`: client, `declare` (`XGROUP CREATE … MKSTREAM`, keeping existing groups), subscriber factory, consumer names. `publisher.go`: `Message` → entry fields, and `streams`, which adds entries and trims the streams with a `MaxLen`. `subscriber.go`: the read and claim loop over a semaphore, the heartbeat and `inflight`, `transient`, the consumer deletion script. `delivery.go`: entry → `worker.IDelivery`, retries handed to `forgo-retry`, dead-letter copies. |
| `provider/sqltx/` | `operation.Transactor` on `*sql.DB`. `Run`, `From`, `Executor`: transactions carried in the context, keyed by database. |
| `provider/inbox/` | `worker.InboxStorage` for PostgreSQL (`postgres.go`) and MySQL 8 (`mysql.go`). `ErrNotWaiting`. |
| `provider/outbox/` | `worker.OutboxStorage` for PostgreSQL and MySQL 8. `outbox.go`: the shared row scan. |
| `provider/locker/` | `operation.ILocker` on Redis. Token-guarded unlock script, `ErrNotHeld`. |
| `provider/ratelimit/` | `http.IRateLimiter` on Redis. GCRA Lua script on the server clock, `KeyPrefix`. |
| `provider/liveconfig/` | `forgo.LiveConfigStore` for PostgreSQL (`postgres.go`) and MySQL 8 (`mysql.go`) on the `live_configs` table, with versioned conditional saves. `liveconfig.go`: the shared `get` and `applied`. `redis.go`: `forgo.LiveConfigCache` on Redis, version-guarded Lua `Set`, `KeyPrefix`. |
| `.github/` | The CI workflow (lint, tests, govulncheck, releases on `v*` tags), Dependabot, and the issue and pull request templates. |

## Invariants

Do not break these without an explicit decision from the maintainer.

### Resources

- `Config` is a value type with value receivers. `Sanitize()` returns a copy with defaults. It **never writes into the caller's slices or maps**, because configs are package-level vars shared by several apps and tests. Build new slices, and clone maps before adding keys (see `QueueConfig.args`). The same holds for what `New` hands to a client: the go-redis cluster client appends discovered nodes to its seed list, so `redisdb` clones `Addrs`.
- `New()` follows this order: `Sanitize` → `validate` (no network) → connect → verify (ping, declare). On any failure after connecting, close what was opened and join the close error with `errors.Join`.
- `Value()` returns the concrete client (`*sql.DB`, the `*redis.Client` or `*redis.ClusterClient` of `redis.NewUniversalClient`) or, for brokers, the forgo interface (`worker.IBroker(r)`). Bootstrap binds the dynamic type of `Value()` and matches by assignability. Document in REFERENCE.md what providers should request.
- `Close()` releases everything `New` opened.

### Providers

- One exported constructor per implementation, `NewX(dep) *X`, listed directly in `forgo.WithProviders`. Return the concrete pointer. Bootstrap matches it to the forgo interface by assignability, and returning an interface would hide the type from other providers.
- Ask for the narrowest dependency that works with every deployment shape: `*sql.DB` for SQL and `redis.UniversalClient` for Redis, so cluster and failover clients plug in.
- Every implementation has a compile-time assertion in its tests: `var _ operation.ILocker = (*Redis)(nil)`.

### RabbitMQ

- Published messages carry `worker.MessageIDHeader`, `MessageNameHeader` and `MessageTimestampHeader`, written from the `Message` fields over any user header of the same name. The consumer inbox depends on a stable ID across redeliveries.
- `NewDelivery` keeps every header. `headerValue` converts values that are not strings to text, and tables and arrays to JSON, because `Message.Headers` holds strings only. Retry copies are built from the raw `amqp.Delivery` headers, never from `Message.Headers`, so they keep the AMQP types.
- `Delivery`: `Complete` = ack, `Retry` = nack with requeue, `Reject` = reject without requeue. The dead-letter feature relies on `Reject` never requeueing. When the queue has `Retry`, `Retry` instead publishes a confirmed copy to the retry queue for its attempt and then acks through the `ackGuard`. It checks staleness before publishing, so a stale delivery is not duplicated, and it falls back to nack with requeue when the publish fails, so the message is never held or lost. The retry publisher is behind the `confirmedPublisher` interface for tests.
- `declare` declares a queue's retry queues **after** the queue. They are durable, and dead-letter through the default exchange (`x-dead-letter-exchange: ""`) with the queue's name as routing key, so an expired retry returns to that queue only. Their names derive from the delays, so changing `Retry` declares new ones and never redeclares an existing queue with other arguments.
- `declare` declares a queue's dead-letter exchange and queue **before** the queue. Dead-letter exchanges are durable direct exchanges and dead-letter queues are durable. Conflicts between `DeadLetter` and `x-dead-letter-*` in `Args` are rejected in `validate`.
- `Consume` waits for every handler it started (`runner.Wait`) before returning. It returns `nil` when `ctx` ends and an error when the delivery channel closes.
- Recovery is the client's (amqp091-go `Config.Recovery`), wrapped by `recoveryHooks`. The hooks recover only channels in `Broker.owned`. They also advance the `ackGuard` of a subscriber channel before the client reopens it, and the guard's read lock spans every acknowledgement. Never acknowledge a delivery around the guard: delivery tags restart on a recovered channel, so a stale tag settles another message.
- `recoveryHooks.OnTopologyEntityError` skips every entity that fails to recover, so one failure never stalls the whole connection in reconnect attempts. A skipped consumer is reported by its tag to its subscriber's `consumerLoss`, and `Consume` returns `errConsumerLost`, because its delivery channel stays open and would never deliver again. Each subscriber therefore consumes under a tag of its own from `Broker.addConsumer` (`forgo-<name>-<n>`, unique on the connection), never the client's generated one.
- The topology channel stays open for the broker's lifetime, because the client forgets the topology recorded on a closed channel.
- `NewSubscriber` checks the queue with a passive declare **before** the channel is owned, so the channel the broker closes on a missing queue is not recovered. Prefetch (`basic.qos`) must precede `basic.consume`, which is why consuming starts in `Consume`, where the worker's concurrency is known.
- Publisher channels are in confirm mode. `Publish` holds the publisher's mutex from publish to confirm, so one message is in flight per channel and a mandatory return can be matched to it by message ID.

### Pub/Sub

- Published messages carry `worker.MessageIDHeader`, `MessageNameHeader` and `MessageTimestampHeader` as attributes, written from the `Message` fields over any header of the same name. `Message.Destination` is the topic and is required, `Message.Key` is the ordering key.
- Every `Publisher` of a broker shares one client publisher per topic (`topics`), so the worker's parallel publishes are batched, and the broker stops them in `Close` before it closes the client. They have `EnableMessageOrdering` on, so a `Key` never fails a publish. After a failed publish, `topics.publish` resumes the message's ordering key, which the client paused: the outbox retries the message later.
- `Delivery`: `Complete` = ack, `Retry` = nack, both through their `*WithResult` variant and waiting on the result, which only takes time with exactly-once delivery. The retry delay is the subscription's retry policy, and the attempt is not used. `Reject` publishes a copy to the dead-letter topic, waits for it and acks, or only acks without a dead-letter topic. When the copy fails, it nacks: it never acks a message whose copy was not stored.
- Pub/Sub's dead-letter policy cannot be triggered by the client, which is why `Reject` publishes copies. `DeadLetterConfig.MaxDeliveryAttempts` only adds the policy as a backstop for messages that keep failing without a verdict.
- `Consume` sets `ReceiveSettings.MaxOutstandingMessages` before `Receive`, and bounds the handlers with a semaphore of the worker's concurrency, because `Receive` runs up to `MaxOutstandingMessages` callbacks at once. A callback that gets no slot before the context ends nacks its message. `Receive` returns only after every callback returned **and every message it delivered was settled**, so every path settles its delivery. The worker does that for the deliveries it handles.
- `Consume` returns `errReceiveStopped` when `Receive` ends without an error before its context, so a subscriber never stops consuming silently.
- `New` creates only the topics and subscriptions listed in the config, and never updates one. An existing subscription must equal the config in every setting `differences` compares; a dead-letter subscription only in its topic (`topicDifference`). Created subscriptions never expire.
- `NewSubscriber` does not check that the subscription exists: `roles/pubsub.subscriber` cannot read subscriptions. `Receive` fails at once on a missing one.
- `NewClient` receives `context.Background()`, never a context with a deadline: the client keeps it for its credentials.

### Redis Streams

- Published entries carry `worker.MessageIDHeader`, `MessageNameHeader` and `MessageTimestampHeader` as fields, written from the `Message` fields over any header of the same name, plus `KeyField` and `PayloadField`. Headers named `KeyField` or `PayloadField` are dropped. `Message.Destination` is the stream and is required. An entry without `x-msg-id` takes the entry ID, which is stable across redeliveries.
- A group is a subscription: `declare` creates the listed groups with `MKSTREAM` and treats `BUSYGROUP` as success, never moving an existing group. Each subscriber reads as a consumer of its own (`consumerName`), created with `XGROUP CREATECONSUMER` in `NewSubscriber`, which also fails at startup on a missing stream or group.
- `Consume` reads (`XREADGROUP COUNT`) no more entries than it holds free slots, and claims (`XAUTOCLAIM`, cursor kept across passes) before reading. The read and claim commands run on `context.WithoutCancel`, so entries Redis handed over are always handled rather than left pending until `ClaimIdle`. `Consume` joins its handlers, then stops the heartbeat, in that order.
- Every in-flight entry is refreshed with `XCLAIM … JUSTID` every `ClaimIdle`/3, so handlers may outlive `ClaimIdle`. `inflight` is keyed by `*Delivery`, not entry ID, because a retried entry can be claimed again before the handler that retried it returns. Settlements remove their delivery from `inflight` **before** their command, and `remove` waits for a refresh in progress, so a refresh never reclaims an entry that was just parked or acknowledged.
- `Retry` never re-adds an entry, which would deliver it to every group of the stream. It parks the entry under the consumer `forgo-retry` with `XCLAIM … IDLE (ClaimIdle − delay) JUSTID`, so `XAUTOCLAIM` of any consumer picks it up once due. Hence `Retry.Max ≤ ClaimIdle`. `forgo-retry` is never deleted.
- `Reject` adds the dead-letter copy, then acknowledges: two commands, because the dead-letter stream may hash to another cluster slot. A failed copy parks the entry for an immediate claim and never acknowledges it.
- `Close` deletes the subscriber's consumer only through `delConsumerScript`, which checks `XPENDING` for that consumer first in the same script: `XGROUP DELCONSUMER` drops a consumer's pending entries from the group.
- `transient` decides whether a read or claim error stops `Consume`: errors without a Redis reply (network, pool) and `LOADING`, `READONLY`, `MASTERDOWN`, `CLUSTERDOWN`, `TRYAGAIN`, max clients are retried after `errorWait`. Every other reply, and `redis.ErrClosed`, returns, so a deleted group stops the app rather than leaving it up without consuming.
- `XReadGroupArgs.Block` must always be set: its zero value blocks forever.

### SQL storages

- Every statement goes through `sqltx.From(ctx, db)`, and every multi-statement operation through `sqltx.Run`, so storages join the transaction of `sqltx.Transactor.Do`. The worker relies on it: `MarkCompleted` must commit or roll back with the handler.
- The exception is `provider/liveconfig`, which runs on `db` directly. forgo caches a document as soon as its save succeeds, in a cache that never expires, so a save must be committed when `Set` returns.
- Live config saves are conditional on the version and atomic: one statement per save, and zero affected rows means `forgo.ErrLiveConfigConflict`. The MySQL insert tells a duplicate key apart by reading the row afterwards, so it needs no driver error codes.
- `liveconfig` sends documents as `string`, never `[]byte`: lib/pq sends `[]byte` as `bytea`, which a `JSONB` column rejects, and MySQL refuses JSON in the binary character set.
- Each storage implements the contract and the comments in forgo's `server/worker/storage.go` exactly. The table schemas are documented in forgo's REFERENCE.md ("Storage contracts").
- Claims use `FOR UPDATE SKIP LOCKED`. Status transitions only leave `InboxWaiting`. Repeating `MarkFailed` or `MarkRejected` succeeds, but `MarkCompleted` must succeed only when it moved the row itself (`unmoved` in `provider/inbox/inbox.go`): it runs in the handler's transaction, and a success for a concurrent duplicate would commit the handler's writes twice.
- Outbox claims are stamped and expired on the database clock only. `Claim` turns `ExpiredBefore` into an age (`claimAge` in `provider/outbox/outbox.go`) and compares `processing_at` with the database time minus that age, so skew between app instances and the database never shifts `ClaimTTL`. Never compare `processing_at` with a time from the app. MySQL uses `UTC_TIMESTAMP(6)`, because `CURRENT_TIMESTAMP` in a `DATETIME` column follows the session time zone and its daylight saving changes.
- Each dialect lives in its own file. Placeholders differ (`$n` and `?`): keep the helpers local to the file that uses them.

### Redis

- Read-modify-write logic runs as one Lua script (`redis.NewScript`) that touches a single key, so it is atomic and works on Redis Cluster.
- Time-based logic uses Redis `TIME`, never the app clock.
- The live config cache never expires its entries, and its `Set` keeps the newest version in one script, so out-of-order writes cannot regress it.
- Keys the adapter invents are namespaced (`ratelimit.KeyPrefix`, `liveconfig.KeyPrefix`). Keys passed in by forgo, such as lock keys, are used as given.

## Coding conventions

- Errors: prefix with the package (`sqldb:`, `redisdb:`, `rabbitmq:`, `pubsub:`, `redisstream:`, `inbox:`, `outbox:`, `liveconfig:`, `locker:`, `ratelimit:`), wrap with `%w`, and declare sentinels as package-level `var`s.
- Doc comments on exported identifiers explain the contract and the why. Match the surrounding code.
- Dependencies: forgo, `github.com/rabbitmq/amqp091-go`, `github.com/redis/go-redis/v9`, `cloud.google.com/go/pubsub/v2` (with `google.golang.org/api/option` for its client options). Test-only: `github.com/alicebob/miniredis/v2`, `github.com/DATA-DOG/go-sqlmock`. A new adapter may add the client library it wraps, and nothing else without a strong reason.
- Tests must not need running infrastructure. Use miniredis for Redis (it runs Lua and `TIME`, and `SetTime`/`FastForward` control the clock; stream idle times follow `SetTime`, and `SetError` fakes error replies), sqlmock for SQL (`sqlmock.NewWithDSN` plus `sql.Open("sqlmock", dsn)` exercises `sqldb.New`; storage tests set `SetMaxOpenConns(1)`, so a statement that bypasses an open transaction blocks and fails on the test's deadline), fakes of narrow interfaces for AMQP (`declarer`, `consumeChannel`, `publishChannel`, `confirmation` and `amqp.Acknowledger`), and `pstest` for Pub/Sub, the in-memory server of the client module, which the real client connects to with `option.WithGRPCConn`. Table-driven where possible, passing under `-race`.

## Workflow

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .        # must print nothing
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
```

CI (`.github/workflows/ci.yml`) runs these on every push and pull request, plus `go mod tidy -diff`, govulncheck and `go test` without the race detector. While `go.mod` replaces forgo with `../forgo`, CI checks out forgo's `main` next to this repository, so a change that needs an unmerged forgo change passes once that change is merged.

When you change behavior:

1. Update [REFERENCE.md](REFERENCE.md) in the same change whenever a config field, default, constructor, wire format or error changes. Update [README.md](README.md) only if the pitch, the table or the headline example is affected.
2. If an adapter is added or renamed, update the Adapters section of forgo's REFERENCE.md and the adapter row of its README.md.
3. If you fix an item from [Known gaps](#known-gaps), remove it. If you find a new one, add it.
4. Doc comments render on pkg.go.dev, with the `Example` functions of `example_test.go` files. Keep the examples in line with REFERENCE.md.

### Releasing

Versions follow semantic versioning. `go get` picks the latest tag, and pkg.go.dev documents it.

1. Require a released forgo (`go get github.com/efegreat/forgo@v1.2.3`) and remove the `replace` directive: modules that depend on this one ignore it, so a release that replaces forgo does not build for them. The release job of CI refuses such a tag.
2. Check that CI passes on `main`.
3. Tag the commit and push the tag: `git tag v1.2.3 && git push origin v1.2.3`. CI runs on the tag, then creates the GitHub release with generated notes and asks the module proxy for the version, so that pkg.go.dev lists it.

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
- The Pub/Sub adapter is tested against `pstest` only, which applies neither retry-policy delays nor IAM.
- The Redis Streams adapter is tested against miniredis only, which differs from Redis in ways the tests work around: it counts `XCLAIM … JUSTID` as a delivery, reports `0` instead of `0-0` as a group position, trims `MAXLEN ~` exactly, and keeps its blocking commands canceled after `Restart` (`resource/broker/redisstream/redisstream_test.go`, test `TestConsumeErrors/restart`).
- A Redis Streams entry whose dead-letter copy cannot be added is claimed again (`resource/broker/redisstream/delivery.go:57`). As with Pub/Sub, a message the inbox already marks *failed* or *rejected* is then acknowledged by the idempotency guard and never reaches the dead-letter stream.
- The consumers of Redis Streams instances that crashed stay in their group without pending entries, since only `Close` deletes a consumer (`resource/broker/redisstream/subscriber.go:144`).
- A Pub/Sub message whose dead-letter copy cannot be published is nacked (`resource/broker/pubsub/delivery.go:75`). When the inbox already marks it *failed* or *rejected*, the worker's idempotency guard acknowledges the redelivery, so the message never reaches the dead-letter topic. RabbitMQ dead-letters on the broker and has no such window.
