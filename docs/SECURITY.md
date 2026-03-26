---
layout: default
title: Security
nav_order: 5
---

# kshark Security Documentation

**Version:** 2.0
**Last Updated:** 2026-03-26
**Security Audit:** OWASP Top 10:2021

---

## Table of Contents

1. [Security Overview](#security-overview)
2. [OWASP Security Audit](#owasp-security-audit)
3. [Critical Security Issues](#critical-security-issues)
4. [High Priority Issues](#high-priority-issues)
5. [Medium Priority Issues](#medium-priority-issues)
6. [Security Best Practices](#security-best-practices)
7. [Secure Configuration](#secure-configuration)
8. [Vulnerability Disclosure](#vulnerability-disclosure)
9. [Security Roadmap](#security-roadmap)

---

## Security Overview

### Current Security Posture

**Overall Rating:** GOOD (8.2/10) -- *Updated 2026-03-26 after SSRF fix and connector probe security hardening*

kshark demonstrates **strong security practices** across input validation, credential redaction, SSRF protection, and connector credential handling.

### Security Strengths

✅ **Implemented Controls:**
- SSRF protection with two-tier model (DENY loopback/link-local/metadata, WARN RFC1918)
- Redirect-based SSRF bypass prevention (`CheckRedirect` handler)
- Command injection prevention via hostname validation
- Credential redaction in all outputs (including `sasl.jaas.config`, bearer tokens, connector passwords)
- Credential scrubbing in database probe error messages (`ScrubCredentials`)
- Connect API auth via environment variables (`KSHARK_CONNECT_AUTH`, `KSHARK_CONNECT_TOKEN`)
- Path traversal protection
- TLS 1.2+ enforcement
- Certificate expiry monitoring
- Non-root Docker execution
- Response body size limits on HTTP clients (1MB)
- URL scheme validation (http/https only)

### Remaining Considerations

⚠️ **Low Priority:**
- Plain-text credential storage in configuration files (mitigated with file permissions)
- Credentials in memory as `string` (consider `[]byte` for zeroability in future)

---

## OWASP Security Audit

Comprehensive audit performed against **OWASP Top 10:2021** standards.

### Security Scorecard

| OWASP Category | Status | Score | Notes |
|----------------|--------|-------|-------|
| **A01 - Broken Access Control** | ✅ Secure | 9/10 | Path traversal protection, non-root Docker |
| **A02 - Cryptographic Failures** | ✅ Good | 7/10 | TLS 1.2+ enforced, SHA256 checksums |
| **A03 - Injection** | ✅ Secure | 9/10 | Hostname regex, pgQuote DSN quoting |
| **A04 - Insecure Design** | ✅ Good | 8/10 | Layered architecture, credential separation |
| **A05 - Security Misconfiguration** | ✅ Good | 7/10 | File permission warnings, env var support |
| **A06 - Vulnerable Components** | ✅ Good | 8/10 | govulncheck in CI, Dependabot configured |
| **A07 - Authentication Failures** | ✅ Good | 7/10 | Credential redaction, env var fallback |
| **A08 - Data Integrity** | ✅ Good | 8/10 | SHA256 report checksums, prompt SHA256 |
| **A09 - Logging** | ✅ Good | 8/10 | Structured slog logging (JSON/text) |
| **A10 - SSRF** | ✅ Secure | 9/10 | Two-tier DENY/WARN model, redirect protection |

**Overall Security Score: 8.0/10**

---

## Previously Critical Issues (Now Resolved)

### SSRF Protection -- FIXED

**Original Severity:** CRITICAL (CVSS 8.6)
**Current Status:** RESOLVED (9/10)
**Fix Location:** `cmd/kshark/ssrf.go` + `cmd/kshark/httpcheck.go`

#### What Was Fixed

The original code accepted arbitrary URLs from configuration files for Schema Registry, REST Proxy, and the Connect API without validation, enabling internal network scanning, cloud metadata access, and service probing.

#### Current Protection Model

A **two-tier SSRF protection model** is now implemented in `cmd/kshark/ssrf.go`:

- **DENY (hard block):** Loopback (`127.0.0.0/8`, `::1`), link-local (`169.254.0.0/16`, `fe80::/10`), multicast, broadcast, and other reserved ranges. These are never legitimate targets for Kafka infrastructure.
- **WARN (allow with warning):** RFC1918 private ranges (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`). These are allowed because many Kafka clusters use AWS PrivateLink or VPC peering with private IPs.
- **Redirect protection:** `CheckRedirect` handler validates each redirect destination against the same SSRF rules.
- **Bounded reads:** All HTTP response bodies are limited via `io.LimitReader` (1MB) to prevent memory exhaustion.
- **Scheme validation:** Only `http://` and `https://` schemes are permitted.

#### Protected Surfaces

All user-supplied URLs are validated before use:
- Schema Registry URL (`schema.registry.url`)
- REST Proxy URL (`rest.proxy.url`)
- AI API endpoint URL
- Kafka Connect REST API URL (`-connect-url`)

#### Test Coverage

37 test cases across `cmd/kshark/ssrf_test.go`, `httpcheck_test.go`, `main_test.go`, and `ai_test.go` cover:
- Loopback, link-local, metadata IPs (denied)
- RFC1918 private ranges (warned)
- Public IPs (allowed)
- Invalid schemes, empty hosts, redirect chains

#### Validation Checklist (all completed)

- [x] Implement `isAllowedURL()` function -- `cmd/kshark/ssrf.go`
- [x] Implement IP classification (`classifyIP()`) -- `cmd/kshark/ssrf.go`
- [x] Add redirect validation to `httpClientFromTLS()` -- `checkRedirectSSRF`
- [x] Update `checkSchemaRegistry()` to validate URLs
- [x] Update REST Proxy check to validate URLs
- [x] RFC1918 allowed for PrivateLink (WARN, not DENY)
- [x] Test with various malicious URLs -- 37 SSRF test cases across 4 test files + 1 fuzz target
- [x] Document URL validation in configuration guide

#### Testing

**Test Cases:**
```bash
# Should FAIL - Internal IP
schema.registry.url=http://192.168.1.1:8081

# Should FAIL - Localhost
schema.registry.url=http://localhost:8081

# Should FAIL - AWS metadata
schema.registry.url=http://169.254.169.254/latest/meta-data/

# Should PASS - Valid public HTTPS
schema.registry.url=https://schema-registry.example.com

# Should FAIL - Redirect to internal IP
# (Requires redirect test server)
```

---

## High Priority Issues

### 🟠 HIGH: Plain-Text Credential Storage

**Severity:** HIGH
**CVSS Score:** 7.5 (High)
**Location:** Multiple files

#### Problem Description

Credentials are stored in plain text in configuration files:
- `client.properties` - Kafka credentials
- `ai_config.json` - AI provider API keys
- File permission validation now warns on insecure permissions (`warnInsecurePermissions`)
- Environment variable expansion (`${VAR}`) available to avoid storing secrets in files
- No encryption at rest

#### Vulnerable Files

**client.properties:**
```properties
sasl.username=YOUR_API_KEY
sasl.password=YOUR_API_SECRET  # ⚠️ Plain text
```

**ai_config.json:**
```json
{
  "provider": "openai",
  "api_key": "sk-xxxx..."  // ⚠️ Plain text
}
```

#### Impact

- Credentials exposed if file system is compromised
- Credentials visible in process listings
- Credentials may be logged or cached
- Difficult to rotate credentials
- Compliance issues (PCI-DSS, SOC 2, etc.)

#### Recommended Solutions

**Option 1: Environment Variable Support (Quick Win)**

Add environment variable substitution:

```go
// Add to loadProperties function (Line 346)
func loadProperties(path string) (map[string]string, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    props := map[string]string{}
    scanner := bufio.NewScanner(file)

    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if line == "" || strings.HasPrefix(line, "#") {
            continue
        }
        parts := strings.SplitN(line, "=", 2)
        if len(parts) == 2 {
            key := strings.TrimSpace(parts[0])
            value := strings.TrimSpace(parts[1])

            // ✅ ADD THIS: Expand environment variables
            value = os.ExpandEnv(value)

            props[key] = value
        }
    }
    return props, scanner.Err()
}
```

**Usage:**
```properties
# client.properties
sasl.username=${KAFKA_USERNAME}
sasl.password=${KAFKA_PASSWORD}
```

```bash
export KAFKA_USERNAME="my-api-key"
export KAFKA_PASSWORD="my-secret"
./kshark -props client.properties
```

**Option 2: File Permission Validation**

```go
// Add after loadProperties function
func validateFilePermissions(path string) error {
    info, err := os.Stat(path)
    if err != nil {
        return err
    }

    mode := info.Mode()

    // File should not be readable by group or others
    if mode.Perm()&0077 != 0 {
        return fmt.Errorf("file %s has insecure permissions %o (should be 0600)",
                         path, mode.Perm())
    }

    return nil
}

// Call in main() before loading properties
if err := validateFilePermissions(*propsFile); err != nil {
    fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
    fmt.Fprintln(os.Stderr, "Hint: Run 'chmod 600 <file>' to secure it")
}
```

**Option 3: Encrypted Configuration (Future)**

```go
// Encrypt configuration files with age or similar
// Decrypt at runtime with passphrase or key file
func loadEncryptedProperties(path, keyFile string) (map[string]string, error) {
    // Implementation using age or similar encryption library
}
```

**Option 4: Secret Manager Integration (Best Practice)**

```go
// AWS Secrets Manager
func getSecretFromAWS(secretName string) (string, error) {
    cfg, _ := config.LoadDefaultConfig(context.TODO())
    client := secretsmanager.NewFromConfig(cfg)
    result, err := client.GetSecretValue(context.TODO(), &secretsmanager.GetSecretValueInput{
        SecretId: aws.String(secretName),
    })
    if err != nil {
        return "", err
    }
    return *result.SecretString, nil
}

// Usage in properties:
// sasl.password=aws-secret://prod/kafka/password
```

#### Implementation Priority

1. ✅ **Done:** Environment variable support -- `os.ExpandEnv()` in `loadProperties()`, `${VAR}` syntax
2. ✅ **Done:** File permission validation -- `warnInsecurePermissions()` warns on mode > 0600
3. 📅 **Next Sprint:** Document secret manager integration patterns
4. 📅 **Future:** Implement built-in encryption support

---

### Structured Security Logging -- FIXED

**Original Severity:** HIGH (CVSS 6.5)
**Current Status:** RESOLVED (8/10)
**Fix Location:** `cmd/kshark/util.go` (logger init), throughout all source files

#### What Was Fixed

Structured logging via Go's `log/slog` is now integrated throughout the application:
- `--log-format text|json` flag selects output format
- `--log` flag writes scan logs to a file (default: `reports/kshark-<timestamp>.log`)
- All security-relevant events are logged with structured key-value pairs
- Log files include SHA256 checksums in the JSON report for integrity verification

#### Example Log Output (JSON mode)

```json
{"time":"2026-03-26T14:30:22Z","level":"DEBUG","msg":"scan start","props":"client.properties","timeout":"1m0s","kafka_timeout":"10s"}
{"time":"2026-03-26T14:30:23Z","level":"DEBUG","msg":"broker check start","host":"broker.example.com","port":"9092"}
{"time":"2026-03-26T14:30:24Z","level":"DEBUG","msg":"broker check ok","addr":"broker.example.com:9092"}
{"time":"2026-03-26T14:30:25Z","level":"DEBUG","msg":"rest proxy URL warning","warning":"RFC1918 address"}
```

---

## Medium Priority Issues

### Error Message Information Disclosure -- MITIGATED

**Severity:** MEDIUM (originally)
**Current Status:** MITIGATED
**Location:** `cmd/kshark/ai.go`

AI API error responses no longer expose raw response bodies to users. Full error details are logged to the structured log file (accessible only to the operator), while user-facing messages show only the HTTP status code. Connector credential values are scrubbed from database probe error messages via `ScrubCredentials()` in `internal/connectapi/redact.go`.

---

### 🟡 MEDIUM: Weak API Key Validation

**Severity:** MEDIUM
**Location:** `cmd/kshark/main.go`

#### Problem

```go
if strings.HasPrefix(providerConfig.APIKey, "YOUR_") || providerConfig.APIKey == "" {
    fmt.Fprintf(os.Stderr, "Error: API key for provider '%s' is a placeholder...\n")
    os.Exit(1)
}
// ⚠️ Only checks prefix, doesn't validate key format
```

#### Recommended Fix

```go
func validateAPIKey(provider, key string) error {
    if key == "" {
        return fmt.Errorf("API key is empty")
    }

    if strings.HasPrefix(key, "YOUR_") {
        return fmt.Errorf("API key is a placeholder")
    }

    // Provider-specific validation
    switch provider {
    case "openai":
        if !strings.HasPrefix(key, "sk-") || len(key) < 20 {
            return fmt.Errorf("invalid OpenAI API key format")
        }
    case "scalytics":
        if len(key) < 32 {
            return fmt.Errorf("invalid Scalytics API key format")
        }
    }

    return nil
}

// Usage
if err := validateAPIKey(providerConfig.Provider, providerConfig.APIKey); err != nil {
    fmt.Fprintf(os.Stderr, "Error: %v\n", err)
    os.Exit(1)
}
```

---

### 🟡 MEDIUM: Dependency Vulnerabilities

**Severity:** MEDIUM
**Location:** `go.mod`, `go.sum`

#### Current Dependencies

```
github.com/segmentio/kafka-go v0.4.49
github.com/klauspost/compress v1.15.9
golang.org/x/net v0.38.0
```

#### Recommended Actions

**1. Run Vulnerability Scan:**
```bash
# Install govulncheck
go install golang.org/x/vuln/cmd/govulncheck@latest

# Scan for vulnerabilities
govulncheck ./...
```

**2. Check for Updates:**
```bash
go list -m -u all
```

**3. Add Dependabot Configuration:**

Create `.github/dependabot.yml`:
```yaml
version: 2
updates:
  - package-ecosystem: "gomod"
    directory: "/"
    schedule:
      interval: "weekly"
    open-pull-requests-limit: 10
    reviewers:
      - "security-team"
    labels:
      - "dependencies"
      - "security"
```

**4. Add to CI/CD:**

```yaml
# .github/workflows/security.yml
name: Security Scan

on:
  push:
    branches: [main]
  pull_request:
  schedule:
    - cron: '0 0 * * 0'  # Weekly

jobs:
  govulncheck:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.23'

      - name: Install govulncheck
        run: go install golang.org/x/vuln/cmd/govulncheck@latest

      - name: Run govulncheck
        run: govulncheck ./...
```

---

## Security Best Practices

### For Users

#### 1. Secure Configuration Files

```bash
# Create secure configuration directory
mkdir -p ~/.kshark
chmod 700 ~/.kshark

# Create configuration with restricted permissions
cat > ~/.kshark/client.properties <<EOF
bootstrap.servers=broker.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.username=your-username
sasl.password=your-password
EOF

chmod 600 ~/.kshark/client.properties
```

#### 2. Use Environment Variables

```bash
# Set credentials in environment
export KAFKA_USERNAME="my-api-key"
export KAFKA_PASSWORD="my-secret"

# Reference in properties file
cat > client.properties <<EOF
bootstrap.servers=broker.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.username=${KAFKA_USERNAME}
sasl.password=${KAFKA_PASSWORD}
EOF
```

#### 3. Never Commit Credentials

```bash
# Add to .gitignore
cat >> .gitignore <<EOF
*.properties
ai_config.json
license.key
*.key
*.pem
*.crt
*secret*
*credential*
EOF
```

#### 4. Use TLS Always

```properties
# ✅ GOOD - TLS enabled
security.protocol=SASL_SSL

# ❌ BAD - No encryption
security.protocol=SASL_PLAINTEXT
```

#### 5. Prefer SCRAM over PLAIN

```properties
# ✅ BETTER - SCRAM-SHA-256
sasl.mechanism=SCRAM-SHA-256

# ⚠️ OKAY - PLAIN (with TLS only)
sasl.mechanism=PLAIN
```

---

### For Developers

#### 1. Input Validation

All user input must be validated:
- ✅ Hostname validation (implemented in `cmd/kshark/diagnostics.go`)
- ✅ Path traversal prevention (implemented in `cmd/kshark/report.go`)
- ✅ URL validation / SSRF protection (implemented in `cmd/kshark/ssrf.go`)

#### 2. Credential Handling

- ✅ Redact credentials in all outputs
- ✅ Never log passwords
- ✅ Environment variable fallback for connector credentials
- ✅ Database probe error messages scrubbed of credentials

#### 3. Dependency Management

```bash
# Regular updates
go get -u ./...
go mod tidy

# Security scanning
govulncheck ./...
```

#### 4. Code Review Checklist

- [x] No hardcoded credentials
- [x] All user input validated
- [x] Errors don't leak sensitive data
- [x] TLS used for all external connections
- [x] Credentials redacted in logs
- [x] File permissions checked (warning on group/other readable)
- [x] URLs validated before use (SSRF two-tier model)

---

## Secure Configuration

### Production Configuration Template

```properties
# Broker connection
bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}

# Security - Always use SASL_SSL
security.protocol=SASL_SSL

# Authentication - Prefer SCRAM
sasl.mechanism=SCRAM-SHA-256
sasl.username=${KAFKA_USERNAME}
sasl.password=${KAFKA_PASSWORD}

# TLS - Always verify certificates
ssl.ca.location=/path/to/ca-cert.pem
# Optionally use client certificates
ssl.certificate.location=/path/to/client-cert.pem
ssl.key.location=/path/to/client-key.pem

# Schema Registry (validate URL!)
schema.registry.url=https://schema-registry.example.com
basic.auth.user.info=${SR_KEY}:${SR_SECRET}
```

### Kubernetes Secret

```bash
kubectl create secret generic kshark-credentials \
  --from-literal=bootstrap.servers=broker.example.com:9092 \
  --from-literal=sasl.username=my-username \
  --from-literal=sasl.password=my-password \
  --dry-run=client -o yaml | kubectl apply -f -
```

### Docker Secrets

```bash
# Create secrets
echo "my-username" | docker secret create kafka_username -
echo "my-password" | docker secret create kafka_password -

# Use in compose
services:
  kshark:
    image: kshark:latest
    secrets:
      - kafka_username
      - kafka_password
    environment:
      - KAFKA_USERNAME_FILE=/run/secrets/kafka_username
      - KAFKA_PASSWORD_FILE=/run/secrets/kafka_password
```

---

## Vulnerability Disclosure

### Reporting Security Issues

**DO NOT** open public GitHub issues for security vulnerabilities.

**Instead, please:**

1. **Email:** security@scalytics.io
2. **Subject:** [SECURITY] kshark vulnerability report
3. **Include:**
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if available)

### Response Timeline

- **24 hours:** Acknowledgment of report
- **7 days:** Initial assessment
- **30 days:** Fix developed and tested
- **Coordinated disclosure:** After fix is released

### Security Advisories

Security advisories are published at:
- GitHub Security Advisories
- Release notes for patched versions

---

## Security Roadmap

### Completed

- [x] **CRITICAL:** SSRF protection (two-tier DENY/WARN model in `cmd/kshark/ssrf.go`)
  - [x] `isAllowedURL()` with full CIDR classification
  - [x] `classifyIP()` with 14 deny + 4 warn ranges
  - [x] Schema Registry URL validation
  - [x] REST Proxy URL validation
  - [x] AI endpoint URL validation
  - [x] Redirect-based SSRF prevention (`checkRedirectSSRF`)

- [x] **HIGH:** Environment variable support
  - [x] `os.ExpandEnv()` in `loadProperties()` -- `${VAR}` syntax works
  - [x] `KSHARK_CONNECT_AUTH` / `KSHARK_CONNECT_TOKEN` env var fallback

- [x] **HIGH:** File permission validation
  - [x] `warnInsecurePermissions()` warns on group/other readable files
  - [x] Documented in Security Best Practices

- [x] **HIGH:** Structured security logging
  - [x] `log/slog` integration with JSON/text handlers
  - [x] `--log-format json|text` flag
  - [x] All security events logged with structured key-value pairs

- [x] **MEDIUM:** Dependency scanning automation
  - [x] `govulncheck` in CI pipeline (every push + weekly)
  - [x] Dependabot for Go modules and GitHub Actions
  - [x] `golangci-lint` with `gosec` in CI

- [x] **MEDIUM:** Error message sanitization
  - [x] AI API errors no longer expose response body
  - [x] CLI args redacted in report metadata (`redactArgs()`)
  - [x] Connector credentials scrubbed from error messages

- [x] **LOW:** Fuzz testing for security-critical parsers
  - [x] `auth_fuzz_test.go` -- SASL credential parsing
  - [x] `properties_fuzz_test.go` -- Properties file parsing with env var expansion
  - [x] `ssrf_fuzz_test.go` -- URL validation and IP classification
  - [x] `jdbc_url_fuzz_test.go` -- JDBC URL parsing (DB2, PostgreSQL)

- [x] **MEDIUM:** CI quality gates
  - [x] `.golangci.yml` with `gosec` security linter
  - [x] Coverage gate in CI pipeline
  - [x] `govulncheck` in weekly security scan workflow (`security.yml`)

- [x] **MEDIUM:** Graceful signal handling
  - [x] SIGINT/SIGTERM cancel scan context via `signal.Notify`
  - [x] All scan phases guarded by `ctx.Done()` in `runScan()`

### Remaining

- [ ] **MEDIUM:** Secret manager integration (AWS/Vault/Azure)
- [ ] **LOW:** Configuration file encryption
- [ ] **LOW:** Certificate pinning
- [ ] **LOW:** SBOM generation and release signing

---

## Security Checklist

### Pre-deployment Security Checklist

- [ ] All credentials use environment variables or secret managers
- [ ] Configuration files have 0600 permissions
- [ ] SSRF protection implemented
- [ ] TLS 1.2+ enforced
- [ ] Certificate validation enabled
- [ ] Security logging configured
- [ ] Dependency vulnerabilities scanned
- [ ] Container image scanned (if using Docker)
- [ ] Network policies configured (if using Kubernetes)
- [ ] Secrets stored in Kubernetes Secrets (not ConfigMaps)

### Runtime Security Checklist

- [ ] Non-root user execution
- [ ] Read-only filesystem (where possible)
- [ ] Minimal container capabilities
- [ ] Network policies enforced
- [ ] Security monitoring enabled
- [ ] Regular security scans scheduled
- [ ] Incident response plan documented

---

## Appendix: Positive Security Controls

### Existing Strong Controls

These controls should be **maintained** in all future versions:

1. ✅ **SSRF Two-Tier Protection** (`cmd/kshark/ssrf.go`)
   - DENY loopback/link-local/metadata, WARN RFC1918
   - Redirect-based bypass prevention
   - Bounded HTTP reads (1MB)

2. ✅ **Structured slog Logging** (`cmd/kshark/util.go`)
   - JSON or text format via `--log-format`
   - All security events logged with structured key-value pairs
   - Log file SHA256 checksums in report

3. ✅ **Command Injection Prevention** (`cmd/kshark/diagnostics.go`)
   - `isValidHostname()` regex validation before shell commands

4. ✅ **Credential Redaction** (`cmd/kshark/util.go`, `internal/connectapi/redact.go`)
   - Redacts: password, secret, token, key, bearer, JAAS config, auth info
   - `ScrubCredentials()` for database probe error messages
   - `redactArgs()` for CLI arguments in report metadata

5. ✅ **Path Traversal Prevention** (`cmd/kshark/report.go`)
   - `createSafeReportPath()` uses `filepath.Base()` to strip directory components

6. ✅ **TLS Enforcement** (`cmd/kshark/tls.go`)
   - `tls.Config{MinVersion: tls.VersionTLS12}` on all TLS connections

7. ✅ **Certificate Expiry Monitoring** (`cmd/kshark/tls.go`)

8. ✅ **SHA256 Checksums** (`cmd/kshark/util.go`)
   - Report files, log files, and analysis prompts all have SHA256 checksums

9. ✅ **Timeout Controls** - All network operations have configurable timeouts

10. ✅ **Context Usage** - Proper context propagation for cancellation via `runScan(ctx, report, scanConfig{})`

16. ✅ **Signal Handling** - SIGINT/SIGTERM gracefully cancel scan context; all phases exit at next `ctx.Done()` check

11. ✅ **Non-root Docker User** (Dockerfile)

12. ✅ **HTML Template Auto-escaping** (Go html/template)

13. ✅ **Minimal GitHub Actions Permissions**

14. ✅ **Release Checksum Generation** (.goreleaser.yaml)

15. ✅ **Safe JSON Deserialization** (No reflection attacks)

---

**Document Version:** 2.0
**Security Audit Date:** 2026-03-26
**Next Security Review:** 2026-06-26
**Contact:** security@scalytics.io
