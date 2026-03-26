---
layout: default
title: Architecture
nav_order: 2
---

# kshark Architecture Overview

**Version:** 1.1
**Last Updated:** 2026-03-26
**Status:** Production

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Architecture](#system-architecture)
3. [Application Structure](#application-structure)
4. [Core Components](#core-components)
5. [Data Flow](#data-flow)
6. [Technology Stack](#technology-stack)
7. [Design Patterns](#design-patterns)
8. [Security Architecture](#security-architecture)
9. [Deployment Architecture](#deployment-architecture)
10. [Extension Points](#extension-points)

---

## Executive Summary

**kshark** is a command-line diagnostic tool for Apache Kafka connectivity, designed as a "network sniffer" for Kafka infrastructure. It performs comprehensive health checks across all layers of the client-to-broker communication path.

### Key Architectural Characteristics

- **Modular Architecture** - Core logic in main.go, probe engine and Connect API in `internal/` packages
- **Layered Testing Approach** - Systematic validation from L3 (DNS) to L7 (Kafka/MongoDB/PostgreSQL/DB2)
- **Report-Centric Architecture** - All checks append results to a central Report structure
- **Connector Probe Engine** - Reads connector configs from Kafka Connect REST API or local JSON files
- **External AI Integration** - Optional AI-powered analysis via REST APIs with layered reasoning
- **Multi-Platform Support** - Cross-compiled for Linux, macOS, and Windows

### Project Statistics

| Metric | Value |
|--------|-------|
| **Total Lines of Code** | ~10,400 lines (Go) across 21 source files + 23 test files |
| **Test Cases** | 478 unit tests + 4 fuzz targets |
| **Programming Language** | Go 1.23.2 |
| **Binary Size** | ~24MB (statically linked, pure Go) |
| **Packages** | `cmd/kshark` (12 files), `internal/probe` (5 files), `internal/connectapi` (4 files) |
| **Test Coverage** | cmd/kshark 41.5%, internal/connectapi 73.6%, internal/probe 52.3%, **total 47.8%** |
| **Dependencies** | kafka-go, mongo-driver, pgx (all pure Go, no CGO) |
| **License** | Apache License 2.0 |

---

## System Architecture

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         kshark CLI                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Configuration Loading                                    │  │
│  │  • Properties Parser    • AI Config   • License Checker   │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Layered Connectivity Checks                              │  │
│  │                                                           │  │
│  │  L3 (Network)    →  DNS Resolution                        │  │
│  │  L4 (Transport)  →  TCP Connection                        │  │
│  │  L5-6 (Security) →  TLS Handshake & Certificate Validation│  │
│  │  L7 (Application)→  Kafka Protocol, Schema Registry       │  │
│  │  Diagnostics     →  Traceroute, MTU Check                 │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Report Generation                                        │  │
│  │  • Console Output (Pretty Print)                          │  │
│  │  • HTML Report (with Template)                            │  │
│  │  • JSON Export (Premium)                                  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  AI Analysis (Optional, Premium)                          │  │
│  │  • OpenAI / Scalytics-Connect Integration                 │  │
│  │  • Root Cause Analysis                                    │  │
│  │  • Actionable Recommendations                             │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    ┌──────────────────────┐
                    │  External Systems    │
                    ├──────────────────────┤
                    │  • Kafka Cluster     │
                    │  • Schema Registry   │
                    │  • REST Proxy        │
                    │  • AI Provider APIs  │
                    └──────────────────────┘
```

### Component Interaction

```
User → CLI Flags → Config Parser → scanConfig → runScan(ctx) → Report Builder → Output
                                         ↓              ↑
                                  AI Analyzer      SIGINT/SIGTERM
                                  (Optional)       cancels context
```

---

## Application Structure

### Directory Layout

```
kshark-core/
├── cmd/kshark/                  # CLI application (12 focused source files)
│   ├── main.go                  # Entry point, CLI flags, scan orchestration (~714 lines)
│   ├── ai.go                   # AI client, prompt building, analysis
│   ├── auth.go                 # SASL authentication (PLAIN, SCRAM, JAAS fallback)
│   ├── connector.go            # Connector probe orchestration
│   ├── diagnostics.go          # Traceroute, MTU checks
│   ├── httpcheck.go            # Schema Registry, REST Proxy HTTP checks
│   ├── kafka.go                # Kafka dialer, metadata, produce/consume probes
│   ├── properties.go           # Properties file loading, presets
│   ├── report.go               # Report model, JSON/HTML output, summarize
│   ├── ssrf.go                 # SSRF two-tier protection (deny/warn model)
│   ├── tls.go                  # TLS config, certificate validation
│   ├── util.go                 # Shared helpers (logging, file I/O, redaction)
│   └── *_test.go               # 14 test files including 3 fuzz targets (auth, ai, kafka, ssrf, tls, ...)
├── internal/
│   ├── probe/                   # Database probing engine
│   │   ├── types.go             # ProbeTarget, ProbeStep, Prober interface
│   │   ├── common.go            # ProbeDNS, ProbeTCP, ProbeTLS helpers
│   │   ├── common_test.go       # Probe helper tests
│   │   ├── mongodb.go           # MongoDB prober (mongo-driver)
│   │   ├── mongodb_test.go      # MongoDB prober tests
│   │   ├── postgres.go          # PostgreSQL prober (pgx)
│   │   ├── postgres_test.go     # PostgreSQL prober tests
│   │   ├── db2.go               # DB2 DRDA wire protocol prober
│   │   └── db2_test.go          # DRDA message construction tests
│   └── connectapi/              # Kafka Connect integration
│       ├── client.go            # Connect REST API client (SSRF-protected)
│       ├── client_test.go       # Connect API client tests
│       ├── config_parser.go     # Connector type detection + extraction
│       ├── config_parser_test.go
│       ├── jdbc_url.go          # JDBC URL parser (DB2, PostgreSQL)
│       ├── jdbc_url_test.go
│       ├── jdbc_url_fuzz_test.go # JDBC URL fuzz target
│       ├── redact.go            # Credential redaction
│       └── redact_test.go
├── testbed/                     # Docker integration testbed
│   ├── docker-compose.yml       # 6 services (Kafka, Connect, MongoDB, PG, DB2, kshark)
│   ├── configs/                 # Connector config examples
│   ├── init/                    # Database initialization scripts
│   └── run-tests.sh             # 10 automated integration tests
├── web/templates/               # HTML report template
├── docs/                        # Jekyll documentation site
├── Dockerfile                   # Multi-stage Alpine container
├── go.mod                       # Go module (kafka-go, mongo-driver, pgx)
└── README.md                    # Project documentation
```

### Source Code Organization

The application follows a **multi-file modular architecture** within the `cmd/kshark` package, split from the original monolithic `main.go` into 12 focused source files:

| File | Responsibility |
|------|---------------|
| **main.go** (~714 lines) | Entry point, CLI flag parsing, `scanConfig` struct, `runScan()` orchestration, `checkRESTProxy()`, signal handling |
| **ai.go** | AI client, prompt construction, analysis dispatch, HTML report |
| **auth.go** | SASL mechanism setup (PLAIN, SCRAM-SHA-256/512), JAAS fallback extraction |
| **connector.go** | Connector probe orchestration, Connect API + local config fallback |
| **diagnostics.go** | Traceroute, MTU check, hostname validation |
| **httpcheck.go** | Schema Registry and REST Proxy HTTP health checks |
| **kafka.go** | Kafka dialer construction, metadata fetch, produce/consume probes |
| **properties.go** | Properties file parser with `os.ExpandEnv()`, presets, `warnInsecurePermissions()` |
| **report.go** | Report data model, JSON/HTML output, summarize, pretty-print |
| **ssrf.go** | Two-tier SSRF protection: DENY loopback/link-local/metadata, WARN RFC1918 |
| **tls.go** | TLS config builder, certificate chain validation, expiry checks |
| **util.go** | Shared helpers: slog init, file I/O, SHA256 checksums, redaction |

Internal packages provide reusable logic:

| Package | Responsibility |
|---------|---------------|
| **internal/probe** | Database prober interface + implementations (MongoDB, PostgreSQL, DB2 DRDA) |
| **internal/connectapi** | Kafka Connect REST client, connector config parser, JDBC URL parser, credential redaction |

---

## Core Components

### 1. Configuration System

**Purpose:** Load and parse configuration from multiple sources

**Files:**
- `client.properties` - Kafka connection parameters (Java properties format)
- `ai_config.json` - AI provider configuration (JSON)
- `license.key` - Premium feature activation (JSON)

**Key Functions:**
- `loadProperties()` (`cmd/kshark/properties.go`) - Parse Java-style properties files with `os.ExpandEnv()` for `${VAR}` expansion
- `loadAIConfig()` (`cmd/kshark/ai.go`) - Load AI configuration
- `applyPreset()` (`cmd/kshark/properties.go`) - Apply quick configuration presets

**Configuration Flow:**
```
CLI Flags → Preset (optional) → Properties File → Default Values → Configuration Map
```

**Supported Presets:**
- `confluent-cloud` - Confluent Cloud SASL/SSL defaults
- `bitnami` - Bitnami Kafka stack defaults
- `aws-msk` - AWS MSK IAM authentication defaults
- `plaintext` - Local development (no security)

---

### 2. Connectivity Testing Engine

**Purpose:** Systematically test all layers of Kafka connectivity

#### 2.1 Layer 3 - Network (DNS)

**Function:** `checkDNS()` (`cmd/kshark/httpcheck.go`)

**Checks:**
- DNS resolution of broker hostnames
- Multiple A/AAAA records detection
- Resolution latency

**Success Criteria:** Hostname resolves to at least one IP address

---

#### 2.2 Layer 4 - Transport (TCP)

**Function:** `checkTCP()` (`cmd/kshark/httpcheck.go`)

**Checks:**
- TCP connection establishment
- Connection latency measurement
- Timeout handling (default: 60s)

**Success Criteria:** Successful TCP handshake

---

#### 2.3 Layer 5-6 - Security (TLS)

**Functions** (`cmd/kshark/tls.go`):
- `tlsConfigFromProps()` - Build TLS configuration
- `wrapTLS()` - Perform TLS handshake
- `peerCN()` - Extract peer certificate CN
- `earliestExpiry()` - Find earliest certificate expiry

**Checks:**
- TLS handshake success
- TLS version verification (minimum TLS 1.2)
- Server certificate validation
- Certificate chain validation
- Certificate expiry (warns if <30 days)
- Server name (CN) extraction

**Security Features:**
- Enforces TLS 1.2 minimum
- Supports custom CA certificates
- Client certificate authentication
- Certificate expiry monitoring

---

#### 2.4 Layer 7 - Application (Kafka Protocol)

**Functions** (`cmd/kshark/kafka.go`):
- `dialerFromProps()` - Create Kafka dialer with SASL/TLS
- `kafkaConn()` - Establish Kafka connection
- `checkTopic()` - Verify topic visibility via metadata
- `probeProduceConsume()` - End-to-end data flow test

**Checks:**
- Kafka protocol handshake
- SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- Broker metadata retrieval
- Topic visibility (if specified)
- Produce/consume round-trip (if topic specified)

**Authentication Support:**
- SASL/PLAIN
- SASL/SCRAM-SHA-256
- SASL/SCRAM-SHA-512
- SASL/GSSAPI (Kerberos) - requires build tag
- Mutual TLS (mTLS)

---

#### 2.5 Layer 7 - HTTP Services

**Functions** (`cmd/kshark/httpcheck.go`, `cmd/kshark/main.go`):
- `checkSchemaRegistry()` - Test Schema Registry
- `checkRESTProxy()` - Test REST Proxy (extracted to testable function in `main.go`)
- Schema Registry subject listing

**Checks:**
- HTTP/HTTPS connectivity
- Basic authentication
- Subject enumeration (/subjects endpoint)
- Response latency

---

#### 2.6 Diagnostics Layer

**Functions** (`cmd/kshark/diagnostics.go`):
- `bestEffortTraceroute()` - Network path tracing
- `mtuCheck()` - Maximum Transmission Unit discovery
- `isValidHostname()` - Hostname sanitization

**Checks:**
- Network path visualization (traceroute/tracepath/tracert)
- MTU discovery via ping with Don't Fragment bit
- Maximum of 100 lines output per diagnostic

**Security:** Hostname validation prevents command injection

---

### 3. Report System

**Purpose:** Collect, aggregate, and present diagnostic results

#### 3.1 Data Model

**Report Structure** (`cmd/kshark/report.go`):
```go
type Report struct {
    Timestamp   string            // Scan timestamp
    Host        string            // Target hostname
    Layers      map[Layer][]Row   // Results grouped by layer
    ConfigEcho  map[string]string // Redacted configuration
    AIAnalysis  *AIAnalysisResponse // Optional AI analysis
}
```

**Row Structure** (`cmd/kshark/report.go`):
```go
type Row struct {
    Component string      // e.g., "dns", "tcp", "tls"
    Target    string      // Target host/service
    Layer     Layer       // L3, L4, L5-6, L7, DIAG
    Status    CheckStatus // OK, WARN, FAIL, SKIP
    Detail    string      // Technical details
    Hint      string      // Actionable guidance
}
```

#### 3.2 Output Formats

**1. Console Output (Pretty Print)**
- Function: `printPretty()` (`cmd/kshark/report.go`)
- Features:
  - Color-coded status (green=OK, yellow=WARN, red=FAIL, gray=SKIP)
  - Grouped by layer
  - Summary statistics
  - TTY detection for color support

**2. HTML Report**
- Function: `writeHTMLReport()` (`cmd/kshark/ai.go`)
- Template: `web/templates/report_template.html`
- Features:
  - Responsive design
  - Embedded CSS
  - AI analysis section
  - Detailed results table
  - Timestamp and configuration echo

**3. JSON Export (Premium)**
- Function: `writeJSON()` (`cmd/kshark/report.go`)
- Features:
  - Machine-readable format
  - Includes full report structure
  - Redacted credentials
  - AI analysis (if available)

#### 3.3 Report Aggregation

**Function:** `summarize()` (`cmd/kshark/report.go`)

**Metrics per Layer:**
- Total checks
- OK count
- WARN count
- FAIL count
- SKIP count

---

### 4. AI Analysis System (Premium)

**Purpose:** Provide intelligent root cause analysis and recommendations

#### 4.1 Architecture

```
Report → AI Client → API Provider → AI Analysis → Enhanced Report
```

#### 4.2 Components

**AIClient Structure** (`cmd/kshark/ai.go`):
```go
type AIClient struct {
    config     *AIProviderConfig
    client     *http.Client
    maxRetries int
}
```

**Supported Providers:**
- **OpenAI** - GPT-4 models
  - Endpoint: `https://api.openai.com/v1/chat/completions`
  - Model: `gpt-4` (configurable)
- **Scalytics-Connect** - Custom endpoint
  - Endpoint: Configurable
  - Model: Configurable

#### 4.3 Analysis Flow

1. **Report Submission**
   - Function: `AnalyzeReport()` (`cmd/kshark/ai.go`)
   - Serializes report to JSON
   - Constructs analysis prompt
   - Sends to AI provider via REST API

2. **Analysis Prompt** (`cmd/kshark/ai.go`)
   ```
   You are a Kafka diagnostics expert. Analyze this connectivity report...

   Focus on:
   1. Failures or warnings (status FAIL/WARN)
   2. Identify problematic OSI layer
   3. Provide root cause analysis
   4. Offer actionable fix suggestions
   ```

3. **Response Processing**
   - Parses AI provider response
   - Extracts analysis text
   - Handles errors gracefully
   - Falls back to basic report if AI unavailable

4. **Output Integration**
   - Function: `printIllustrativeAnalysis()` (`cmd/kshark/ai.go`)
   - Displays analysis with visual separators
   - Markdown formatting support

#### 4.4 Security Considerations

- API keys stored in `ai_config.json` (recommend environment variables)
- Credentials redacted before sending to AI
- 60-second timeout per request
- HTTPS required for API communication

---

### 5. Security Architecture

**See:** `docs/SECURITY.md` for comprehensive security analysis

#### Key Security Features

1. **Input Validation**
   - Hostname sanitization (regex validation)
   - Path traversal prevention
   - Command injection protection

2. **SSRF Protection** (`cmd/kshark/ssrf.go`)
   - Two-tier model: DENY loopback/link-local/metadata IPs, WARN RFC1918
   - Redirect-based SSRF bypass prevention (`CheckRedirect` handler)
   - URL scheme validation (http/https only)
   - Response body size limits (1MB) via `io.LimitReader`

3. **Credential Protection**
   - Redaction function for sensitive fields (passwords, secrets, tokens, bearer, JAAS)
   - Credential scrubbing in database probe error messages (`ScrubCredentials`)
   - Connect API auth via environment variables (`KSHARK_CONNECT_AUTH`, `KSHARK_CONNECT_TOKEN`)
   - Functions in `cmd/kshark/util.go` and `internal/connectapi/redact.go`

4. **Structured Logging** (`log/slog`)
   - Configurable format: `--log-format text|json`
   - Security events logged (authentication attempts, API calls, SSRF blocks)
   - Scan log written to file with SHA256 checksum

5. **TLS Enforcement**
   - Minimum TLS 1.2
   - Certificate validation and chain checking
   - Custom CA support
   - Certificate expiry monitoring (<30 days warning)

6. **Secure Defaults**
   - Non-root Docker user
   - Timeouts on all network operations
   - Fail-safe error handling
   - SHA256 checksums for report artifacts

#### Security Concerns (See SECURITY.md for details)

- **Credential Storage:** Plain-text configuration files (mitigated with env var support and file permissions)
- **Memory Zeroability:** Credentials held as `string` rather than `[]byte` (low priority)

---

## Data Flow

### Complete Diagnostic Flow

```
┌─────────────────┐
│   User Input    │
│  CLI Flags +    │
│  Config Files   │
└────────┬────────┘
         ↓
┌─────────────────┐     ┌──────────────────┐
│  Initialization │     │  Signal Handler  │
│ • Load config   │     │  SIGINT/SIGTERM  │
│ • ExpandEnv     │     │  → cancel(ctx)   │
│ • Warn perms    │     └────────┬─────────┘
│ • Build scanCfg │              │ (cancels
└────────┬────────┘              │  context)
         ↓                       ↓
┌─────────────────┐
│   Scan Plan     │
│ • Display plan  │
│ • User confirm  │
└────────┬────────┘
         ↓
┌──────────────────────────────────────────┐
│          runScan(ctx, report, cfg)        │
│  (all checks guarded by ctx.Done())      │
│                                          │
│  ┌─────────────────┐                     │
│  │ L3: DNS Check   │                     │
│  │ Resolve hostname│                     │
│  └────────┬────────┘                     │
│           ↓                              │
│  ┌─────────────────┐                     │
│  │ L4: TCP Check   │                     │
│  │ Connect to port │                     │
│  └────────┬────────┘                     │
│           ↓                              │
│  ┌─────────────────┐                     │
│  │ L5-6: TLS Check │                     │
│  │ Handshake + Cert│                     │
│  └────────┬────────┘                     │
│           ↓                              │
│  ┌─────────────────┐                     │
│  │ L7: Kafka Check │                     │
│  │ • Metadata      │                     │
│  │ • Topic check   │                     │
│  │ • Produce/Consume│                    │
│  └────────┬────────┘                     │
│           ↓                              │
│  ┌─────────────────┐                     │
│  │ L7: HTTP Checks │                     │
│  │ • Schema Reg    │                     │
│  │ • Connector Probe│                    │
│  │ • REST Proxy    │                     │
│  └────────┬────────┘                     │
│           ↓                              │
│  ┌─────────────────┐                     │
│  │  Diagnostics    │                     │
│  │ • Traceroute    │                     │
│  │ • MTU Check     │                     │
│  └─────────────────┘                     │
└──────────────────┬───────────────────────┘
                   ↓
┌─────────────────┐
│ Generate Report │
│ • Summarize     │
│ • Console output│
│ • HTML/JSON     │
└────────┬────────┘
         ↓
┌─────────────────┐
│ AI Analysis     │
│  (if enabled)   │
└────────┬────────┘
         ↓
┌─────────────────┐
│  Final Output   │
│ • Reports saved │
│ • Analysis shown│
└─────────────────┘
```

### Data Structures Flow

```
Properties Map → TLS Config → Kafka Dialer → Connection
                                              ↓
                                         Check Results
                                              ↓
                                          Row Objects
                                              ↓
                                        Report Struct
                                              ↓
                              ┌───────────────┴───────────────┐
                              ↓                               ↓
                        AI Analysis                    Report Output
                              ↓                               ↓
                      Enhanced Report              Console/HTML/JSON
```

---

## Technology Stack

### Core Technologies

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Language** | Go | 1.23.2 | Application runtime |
| **Kafka Client** | segmentio/kafka-go | 0.4.49 | Kafka protocol implementation |
| **Compression** | klauspost/compress | 1.15.9 | Message compression |
| **SCRAM Auth** | xdg-go/scram | 1.1.2 | SASL SCRAM authentication |
| **Build Tool** | Go toolchain | 1.23 | Compilation |
| **Release Tool** | GoReleaser | latest | Cross-platform builds |
| **Container** | Docker | - | Containerization |
| **CI/CD** | GitHub Actions | - | Automation |

### Build Flags

```bash
CGO_ENABLED=0           # Static linking
-ldflags="-w -s"       # Strip debug info
-X main.version={{.Version}}  # Version injection
```

### Dependencies Graph

```
kshark
├── github.com/segmentio/kafka-go (v0.4.49)
│   ├── github.com/klauspost/compress (v1.16.7)
│   ├── github.com/pierrec/lz4/v4 (v4.1.15)
│   ├── github.com/xdg-go/pbkdf2 (v1.0.0)
│   ├── github.com/xdg-go/scram (v1.1.2)
│   │   └── github.com/xdg-go/stringprep (v1.0.4)
│   │       └── golang.org/x/text (v0.23.0)
│   └── golang.org/x/sync (v0.12.0)
├── go.mongodb.org/mongo-driver/v2 (v2.1.0)
├── github.com/jackc/pgx/v5 (v5.7.4)
│   ├── github.com/jackc/pgpassfile (v1.0.0)
│   └── github.com/jackc/pgservicefile
└── Standard Library
    ├── crypto/tls
    ├── encoding/json
    ├── log/slog
    ├── net/http
    ├── html/template
    └── ...
```

---

## Design Patterns

### 1. Multi-File Package Architecture

**Rationale:**
- Each file has a single, clear responsibility (~100-714 lines)
- Easier to navigate and maintain than a monolithic file
- Enables focused unit testing per concern
- Simplifies code review (changes scoped to relevant file)
- Still compiles to a single binary for easy deployment

**Trade-offs:**
- More files to manage (mitigated by consistent naming conventions)
- Cross-file dependencies within the package require care

---

### 2. Builder Pattern

**Usage:** Configuration construction

```go
// TLS config builder
func tlsConfigFromProps(p map[string]string) (*tls.Config, error) {
    conf := &tls.Config{MinVersion: tls.VersionTLS12}

    if caFile := p["ssl.ca.location"]; caFile != "" {
        // Build CA pool...
    }

    if certFile := p["ssl.certificate.location"]; certFile != "" {
        // Add client cert...
    }

    return conf, nil
}

// SASL config builder
func saslFromProps(p map[string]string) (sasl.Mechanism, error) {
    mechanism := p["sasl.mechanism"]
    switch mechanism {
    case "PLAIN":
        return plain.Mechanism{...}, nil
    case "SCRAM-SHA-256":
        return scram.Mechanism(...), nil
    // ...
    }
}
```

---

### 3. Strategy Pattern

**Usage:** Authentication mechanisms

```go
// Different SASL strategies selected at runtime
switch saslMechanism {
case "PLAIN":
    mechanism = plain.Mechanism{Username: user, Password: pass}
case "SCRAM-SHA-256":
    mechanism, _ = scram.Mechanism(scram.SHA256, user, pass)
case "SCRAM-SHA-512":
    mechanism, _ = scram.Mechanism(scram.SHA512, user, pass)
}
```

---

### 4. Template Method Pattern

**Usage:** HTML report generation

```go
// Template defines structure, data fills in details
tmpl, _ := template.ParseFiles("web/templates/report_template.html")
tmpl.Execute(file, report)
```

---

### 5. Facade Pattern

**Usage:** Simplified Kafka connectivity

```go
// Complex Kafka setup hidden behind simple function
func kafkaConn(ctx context.Context, dialer *kafka.Dialer, addr string) (*kafka.Conn, error) {
    // Handles timeout, context, error checking internally
    return dialer.DialContext(ctx, "tcp", addr)
}
```

---

### 6. Report Accumulator Pattern

**Usage:** Collecting diagnostic results

```go
// Global-like report object accumulates all results
func addRow(r *Report, row Row) {
    layer := row.Layer
    r.Layers[layer] = append(r.Layers[layer], row)
}

// All checks append to the same report
checkDNS(report, ...)
checkTCP(report, ...)
checkTLS(report, ...)
```

---

## Extension Points

### 1. Authentication Mechanisms

**Current:** PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, (Kerberos with build tag)

**Extension Point:** `saslFromProps()` function (`cmd/kshark/auth.go`)

**How to Add:**
```go
case "SCRAM-SHA-1":  // Add new case
    mechanism, err = scram.Mechanism(scram.SHA1, username, password)
case "OAUTH":
    // Implement OAuth mechanism
```

---

### 2. AI Providers

**Current:** OpenAI, Scalytics-Connect

**Extension Point:** `ai_config.json` + `AnalyzeReport()` function

**How to Add:**
```json
{
  "provider": "anthropic",
  "api_key": "sk-ant-...",
  "api_endpoint": "https://api.anthropic.com/v1/messages",
  "model": "claude-3-opus"
}
```

**Code modification needed:** Update API request/response format in `AnalyzeReport()`

---

### 3. Output Formats

**Current:** Console, HTML, JSON

**Extension Point:** After report generation in `cmd/kshark/main.go`

**How to Add:**
```go
if *xmlOut != "" {
    writeXMLReport(report, *xmlOut)
}
```

---

### 4. Layer Checks

**Current:** L3 (DNS), L4 (TCP), L5-6 (TLS), L7 (Kafka/HTTP), DIAG

**Extension Point:** `runScan()` function in `cmd/kshark/main.go`

**How to Add:**
```go
// Add a new phase inside runScan(), guarded by ctx.Done()
select {
case <-ctx.Done():
    addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached", ""})
    return
default:
    checkNewService(ctx, report, cfg.props)
}
```

---

### 5. Configuration Presets

**Current:** confluent-cloud, bitnami, aws-msk, plaintext

**Extension Point:** `applyPreset()` function (`cmd/kshark/properties.go`)

**How to Add:**
```go
case "azure-eventhub":
    p["security.protocol"] = "SASL_SSL"
    p["sasl.mechanism"] = "PLAIN"
    p["sasl.username"] = "$ConnectionString"
```

---

## Deployment Architecture

### Local Deployment

```
User's Machine
├── kshark binary
├── client.properties (config)
├── ai_config.json (optional)
├── license.key (optional)
└── reports/ (output)
```

**Execution:**
```bash
./kshark -props client.properties -topic test-topic --analyze
```

---

### Docker Deployment

```
Docker Container (alpine:latest)
├── /usr/local/bin/kshark (binary)
├── /app/web/templates/ (HTML template)
├── /app/client.properties (mounted)
├── /app/ai_config.json (mounted)
├── /app/license.key (mounted)
└── /app/reports/ (mounted volume)
```

**Build:**
```bash
docker build -t kshark:latest .
```

**Run:**
```bash
docker run -v $(pwd):/app -u $(id -u):$(id -g) \
    kshark:latest -props /app/client.properties --analyze
```

---

### Kubernetes Deployment

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: kshark-health-check
spec:
  schedule: "0 * * * *"  # Hourly
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: kshark
            image: kshark:latest
            args:
              - "-props"
              - "/config/client.properties"
              - "-topic"
              - "health-check"
              - "--analyze"
              - "-y"
            volumeMounts:
            - name: config
              mountPath: /config
            - name: reports
              mountPath: /app/reports
          volumes:
          - name: config
            secret:
              secretName: kshark-config
          - name: reports
            persistentVolumeClaim:
              claimName: kshark-reports
          restartPolicy: OnFailure
```

---

### CI/CD Pipeline Architecture

```
Developer Push (Tag v*)
    ↓
GitHub Actions Trigger
    ↓
Checkout Code (fetch-depth: 0)
    ↓
Setup Go 1.23
    ↓
GoReleaser Build
    ├── Linux (amd64, arm64)
    ├── macOS (amd64, arm64)
    └── Windows (amd64, arm64)
    ↓
Create Archives
    ├── tar.gz (Linux, macOS)
    └── zip (Windows)
    ↓
Generate Checksums
    ↓
Create GitHub Release
    ├── Upload binaries
    ├── Upload checksums
    └── Generate changelog
```

**Workflow File:** `.github/workflows/build-and-release.yml`

---

## Performance Characteristics

### Typical Execution Time

| Check Type | Time Range | Notes |
|------------|------------|-------|
| DNS Resolution | 10-100ms | Depends on DNS server |
| TCP Connection | 50-200ms | Network latency dependent |
| TLS Handshake | 100-500ms | Certificate chain length |
| Kafka Metadata | 100-300ms | Cluster size dependent |
| Produce/Consume | 200-1000ms | Topic partition count |
| Traceroute | 5-30s | Hop count dependent |
| AI Analysis | 5-30s | AI provider latency |

### Resource Usage

| Resource | Typical | Peak | Notes |
|----------|---------|------|-------|
| Memory | 20MB | 50MB | Includes binary overhead |
| CPU | <5% | 20% | Mostly I/O wait |
| Network | <1KB/s | 100KB/s | During checks |
| Disk | 1-5MB | 10MB | Report output |

---

## Future Architecture Considerations

### Completed Improvements

1. **Modularization** (done)
   - Split monolithic `main.go` (2,479 lines) into 12 focused files
   - `internal/probe` and `internal/connectapi` packages extracted
   - 23 test files (14 in cmd/kshark including 3 fuzz targets, 9 in internal/) with 478 test cases

2. **Structured Logging** (done)
   - `log/slog` integration with `--log-format text|json` flag
   - Security events logged throughout scan lifecycle

3. **SSRF Protection** (done)
   - Two-tier deny/warn model in `cmd/kshark/ssrf.go` (14 deny CIDRs + 4 warn CIDRs)
   - Redirect validation (`checkRedirectSSRF`), bounded reads (`io.LimitReader`), scheme validation

4. **Control Flow Refactor** (done)
   - Extracted `runScan()` function replacing `goto endScan` pattern
   - Extracted `checkRESTProxy()` as testable function
   - `scanConfig` struct encapsulates all scan parameters
   - All scan phases guarded by `ctx.Done()` for clean timeout/cancellation

5. **Signal Handling** (done)
   - SIGINT/SIGTERM graceful shutdown cancels scan context
   - Existing `ctx.Done()` checks in `runScan()` handle early exit

6. **Fuzz Testing** (done)
   - 4 fuzz targets: `auth_fuzz_test.go`, `properties_fuzz_test.go`, `ssrf_fuzz_test.go`, `jdbc_url_fuzz_test.go`
   - Covers security-critical parsers (SASL auth, properties, SSRF URL validation, JDBC URLs)

7. **CI Quality Gates** (done)
   - `.golangci.yml` with `gosec` linter enabled
   - Coverage gate in CI pipeline
   - `govulncheck` in weekly security scan workflow

### Remaining Improvements

1. **Concurrency**
   - Parallel layer checks where possible
   - Worker pool for multiple brokers
   - Async AI analysis

2. **Caching**
   - DNS result caching
   - TLS session resumption
   - Configuration caching

3. **Observability**
   - Prometheus metrics export
   - OpenTelemetry integration

4. **Persistence**
   - Historical report database
   - Trend analysis
   - Alerting on degradation

---

## Conclusion

kshark's architecture prioritizes **simplicity, reliability, and comprehensive diagnostics**. The multi-file modular design within the `cmd/kshark` package keeps each concern focused while still compiling to a single binary. The `internal/` packages (`probe`, `connectapi`) provide reusable logic for connector probing and database health checks.

The layered testing approach ensures thorough validation of all connectivity components, and the optional AI integration adds intelligent analysis capabilities without compromising the core functionality.

---

**Document Version:** 1.1
**Author:** kshark Development Team
**Last Review:** 2026-03-26
**Next Review:** 2026-06-26
