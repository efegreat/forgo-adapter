# Security policy

## Supported versions

Security fixes are released for the latest minor version of forgo-adapter.

## Reporting a vulnerability

Do not open a public issue. Report the vulnerability privately through [GitHub's vulnerability reporting](https://github.com/efegreat/forgo-adapter/security/advisories/new), with the affected version, what an attacker can do, and a reproduction if you have one.

Areas where a flaw is likely to be a vulnerability: SQL built by the inbox, outbox and live config storages, the Redis scripts of the locker, the rate limiter and the live config cache, and message headers and attributes read from a broker.

A vulnerability in a client library, such as go-redis or amqp091-go, belongs to its maintainers, unless the way an adapter uses it makes it exploitable. Vulnerabilities of forgo itself belong to [forgo](https://github.com/efegreat/forgo/security/advisories/new).
