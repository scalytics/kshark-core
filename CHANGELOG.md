# Changelog

All notable changes to kshark are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- Startup banner reads `Edition: Open Source (Apache-2.0)` instead of `Edition: Open Source (non-commercial)`.
- README, docs/FEATURES.md, docs/DEPLOYMENT.md and CONTRIBUTING.md match the code: Go 1.25, versioned release asset names, the real `ai_config.json` format, only the `cc-plain` and `self-scram` presets, GSSAPI marked as not implemented, no license file or premium tier, and HTML reports only after `-analyze`.

### Removed
- Stray example properties files, a tracked `reports/` directory, `my-report.json`, `docs/.DS_Store` and `test_docker.sh`

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

## [1.1.1] - 2026-10-22

### Added
- `validate-blueprint` subcommand with routine and verdict output (#23)
- Helm chart for a CronJob deployment (`deploy/helm/kshark`)
- Homebrew tap configuration in GoReleaser

### Changed
- Release binaries are built with Go 1.26.10 for CVE-2026-111943 (#44)
- Copyright holder is Scalytics, Inc. (#43)
- Dependency updates: kafka-go 0.4.51, mongo-driver 2.9.2, pgx 5.10.0, golang.org/x/text
- CI runs on Go 1.26 with golangci-lint v2

### Security
- Cleared critical CVEs in the shipped container image

## [1.1.0] - 2026-03-27

### Added
- Redis and Elasticsearch connector probes
- `-watch` mode, `kshark diff` and `kshark trend`
- `kshark doctor` subcommand
- Layered exit codes (1=DNS, 2=TCP, 3=TLS, 4=App, 5=Diag)

### Changed
- Scan orchestration moved from `main.go` to `scan.go`, broker discovery to `broker_discovery.go`

### Fixed
- Wire protocol probers read with bounded `io.Reader` handling
- Added `releasepatch` and `releasemajor` Makefile targets

## [1.0.3] - 2026-03-26

### Added
- MySQL, SQL Server and Oracle connector probes
- Broker discovery scan with advertised listener mismatch detection
- Neighborhood scan for connector targets

## [1.0.2] - 2026-03-26

### Removed
- Tracked `.DS_Store` and `kshark` binary

## [1.0.1] - 2026-03-26

### Added
- Probe direction control (`-probe-direction=up|full`)
- Neighborhood scan with restriction classification
- Diagnostics bundle with Terraform state redaction (`-bundle`, `-tf-state`, `-tf-plan`)

## [1.0.0] - 2026-03-26

### Changed
- First 1.0 release with connector probes and SSRF protection

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
