# Changelog

All notable changes to kshark are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- CI/CD pipeline with GitHub Actions (build, test, vet, govulncheck)
- Weekly security scan workflow
- Dependabot configuration for Go modules and GitHub Actions
- CONTRIBUTING.md with development guide
- CHANGELOG.md (this file)
- PR and issue templates
- Unit tests for core logic functions (478 test cases, 47.8% total coverage)
- PostgreSQL DSN quoting (`pgQuote`) to prevent parameter injection
- SSRF validation on AI API endpoint
- `redactArgs()` to strip credentials from CLI args in reports
- `govulncheck` and `golangci-lint` Makefile targets
- `.golangci.yml` configuration with `gosec` security linter
- Coverage gate in CI pipeline
- Environment variable expansion in properties files (`${VAR}` syntax via `os.ExpandEnv`)
- File permission warnings for properties files (`warnInsecurePermissions` warns on mode > 0600)
- Fuzz testing: 4 fuzz targets (`auth_fuzz_test.go`, `properties_fuzz_test.go`, `ssrf_fuzz_test.go`, `jdbc_url_fuzz_test.go`)
- Signal handling: SIGINT/SIGTERM gracefully cancel the scan context for clean shutdown
- `scanConfig` struct to encapsulate all scan parameters
- `runScan()` function with context-guarded scan phases
- `checkRESTProxy()` extracted as standalone testable function

### Changed
- **BREAKING**: Module path changed from `github.com/your-username/kshark` to `github.com/scalytics/kshark-core`
- Split `main.go` (2,479 lines) into 12 focused files (~100-714 lines each)
- Report checksum now uses SHA256 instead of MD5
- HTTP response bodies are now bounded with `io.LimitReader` to prevent memory exhaustion
- AI API error responses no longer expose raw response body (information disclosure fix)
- Fixed duplicate `releaseminor` Makefile target
- Updated all placeholder URLs to `github.com/scalytics/kshark-core`
- Updated copyright headers
- Replaced `goto endScan` with `runScan()` function; all scan phases now exit via `ctx.Done()` checks
- `isLinkLocalIP` expanded to full SSRF deny list: 14 CIDR ranges (loopback, link-local, CGN, TEST-NETs, multicast, reserved, broadcast) + 4 warn ranges (RFC1918, ULA)
- REST Proxy check now properly guarded by `ctx.Done()` inside `runScan()` (previously ran unconditionally after `endScan` label)

### Security
- Added SSRF protection for AI API endpoint URL
- Bounded all HTTP response reads to 1-4 MB
- CLI arguments containing credentials are redacted in report metadata
- PostgreSQL DSN values are properly quoted to prevent parameter injection
- Replaced MD5 checksums with SHA256
- AI client `CheckRedirect` handler prevents redirect-based SSRF bypass
- REST Proxy extracted to `checkRESTProxy()` for httptest-based testing

## [0.1.0] - 2025-09-05

### Added
- Initial release
- Layered diagnostic tool for Apache Kafka (L3-L7)
- SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 authentication
- TLS/mTLS support with certificate expiry monitoring
- Schema Registry and REST Proxy connectivity checks
- AI-powered analysis with prompt export
- HTML and JSON report generation
- Docker and Kubernetes support
- Presets for Confluent Cloud and common configurations
- Traceroute and MTU diagnostics

## [0.2.0] - 2026-03-25

### Added
- Connector probe feature (SPEC-002)
  - MongoDB sink connector probing
  - PostgreSQL JDBC source connector probing
  - DB2 JDBC source connector probing (DRDA wire protocol)
- Kafka Connect REST API client with authentication
- JDBC URL parser for PostgreSQL and DB2
- Connector config auto-detection from connector class
- Fallback from Connect API to local config file
- Integration testbed with Docker Compose
- SSRF two-tier protection model (deny loopback/metadata, warn RFC1918)
- Credential redaction for connector configs
