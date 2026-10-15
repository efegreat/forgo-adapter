# Contributing to forgo-adapter

- **Bugs:** open an issue with the smallest program or test that shows the problem, and the version of the database, broker or Redis it runs against. For a security issue, follow [SECURITY.md](SECURITY.md) instead.
- **New adapters and changes of behavior:** open an issue to discuss them before you send a pull request. A new adapter wraps one client library and implements a forgo contract.
- **Contracts** such as `worker.IBroker` or `operation.ILocker` belong to [forgo](https://github.com/efegreat/forgo). A change to one needs pull requests in both repositories.

## Making a change

[AGENTS.md](AGENTS.md) is the contributor guide: the repository map, the invariants of resources, brokers and storages, the coding conventions and the checklists for new resources, providers and brokers. Tests must not need running infrastructure: use miniredis, sqlmock, fakes of the client interfaces and `pstest`. The integration tests, which run against PostgreSQL, MySQL, RabbitMQ and Redis when their URLs or addresses are set, skip without them, and CI runs them.

Before you open a pull request, run:

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .        # must print nothing
```

CI also runs staticcheck and govulncheck.

- Update [REFERENCE.md](REFERENCE.md) in the same pull request whenever a config field, default, constructor, wire format or error changes.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org): `fix(rabbitmq): …`, `feat(redisstream): …`, `docs: …`.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
