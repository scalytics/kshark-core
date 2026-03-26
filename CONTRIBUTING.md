# Contributing to kshark

We welcome contributions! This guide explains how to get started.

## Prerequisites

- **Go 1.23+** ([golang.org/dl](https://go.dev/dl/))
- **Docker** (for integration testbed)
- **golangci-lint** (optional, for local linting)

## Development Setup

```bash
git clone https://github.com/scalytics/kshark-core.git
cd kshark-core
go mod download
make build
make test
```

## Running Tests

```bash
# Unit tests with race detector
make test

# Or directly
go test ./... -v -race -timeout 120s

# With coverage
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# Integration testbed (requires Docker)
cd testbed && docker compose up -d
docker compose run --rm kshark /run-tests.sh
```

## Code Style

- Run `gofmt` on all code (enforced by CI)
- Run `go vet ./...` before committing
- Follow [Effective Go](https://go.dev/doc/effective_go) conventions
- Keep functions focused and testable
- Use table-driven tests with `t.Run()`

## Project Structure

```
cmd/kshark/          Main application (split into focused files)
  main.go            CLI flags, orchestration, main()
  ai.go              AI analysis client and prompt building
  auth.go            SASL/TLS authentication builders
  kafka.go           Kafka protocol helpers and produce/consume probe
  tls.go             DNS, TCP, TLS handshake checks
  ssrf.go            SSRF protection (deny/warn model)
  report.go          Report types, formatting, HTML/JSON output
  properties.go      Properties file parsing and presets
  httpcheck.go       Schema Registry and REST Proxy checks
  connector.go       Connector probe orchestration
  diagnostics.go     Traceroute and MTU diagnostics
  util.go            Logging, file hashing, utilities
internal/
  connectapi/        Kafka Connect REST API client
  probe/             Database probers (MongoDB, PostgreSQL, DB2)
testbed/             Docker Compose integration test environment
```

## Submitting Changes

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Write tests for new functionality
4. Ensure `go vet ./...` and `go test ./...` pass
5. Commit with a descriptive message (use [Conventional Commits](https://www.conventionalcommits.org/))
6. Push and open a Pull Request against `main`

## Commit Messages

Use conventional commit format:

```
feat: add MySQL connector probe
fix: handle empty bootstrap.servers gracefully
test: add fuzz tests for JDBC URL parser
docs: update deployment guide for Kubernetes
```

## Security

If you discover a security vulnerability, please do **not** open a public issue.
Instead, email security@scalytics.io with details. See [SECURITY.md](docs/SECURITY.md).

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
