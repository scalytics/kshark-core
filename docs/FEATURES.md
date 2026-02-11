# kshark Feature Documentation

**Version:** 1.0
**Last Updated:** 2025-11-13

---

## Table of Contents

1. [Overview](#overview)
2. [Core Features](#core-features)
3. [Connectivity Checks](#connectivity-checks)
4. [Authentication Methods](#authentication-methods)
5. [Configuration Options](#configuration-options)
6. [Output Formats](#output-formats)
7. [AI-Powered Analysis](#ai-powered-analysis)
8. [Premium Features](#premium-features)
9. [Command-Line Interface](#command-line-interface)
10. [Configuration Presets](#configuration-presets)
11. [Network Diagnostics](#network-diagnostics)
12. [Use Cases](#use-cases)

---

## Overview

kshark provides comprehensive Kafka connectivity diagnostics through a layered testing approach. Each layer builds on the success of the previous one, providing clear insights into exactly where connectivity issues occur.

### Design Philosophy

- **Systematic Testing:** Test every layer from DNS to Kafka protocol
- **Fail-Fast:** Stop at the first failure to provide clear diagnostics
- **Actionable Results:** Every failure includes hints for resolution
- **Security-First:** Redact credentials, validate inputs, enforce TLS

---

## Core Features

### 1. Layered Connectivity Testing

kshark tests connectivity across all network layers:

#### Layer 3: Network Layer
- **DNS Resolution**
  - Resolves broker hostnames to IP addresses
  - Measures DNS query latency
  - Detects multiple A/AAAA records
  - Identifies DNS resolution failures

**Example Output:**
```
✓ DNS Resolution: broker.example.com → 192.0.2.1 (45ms)
```

---

#### Layer 4: Transport Layer
- **TCP Connection**
  - Establishes TCP connection to broker port
  - Measures connection establishment time
  - Validates port accessibility
  - Detects network timeouts

**Example Output:**
```
✓ TCP Connection: 192.0.2.1:9092 established (123ms)
```

---

#### Layer 5-6: Security Layer
- **TLS Handshake**
  - Performs TLS negotiation
  - Validates server certificates
  - Checks certificate chain
  - Monitors certificate expiry
  - Enforces TLS 1.2+ minimum
  - Extracts server CN (Common Name)

**Example Output:**
```
✓ TLS Handshake: TLS 1.3 successful (234ms)
✓ Certificate: CN=broker.example.com, expires in 87 days
⚠ Certificate Expiry: Certificate expires in <30 days
```

**Certificate Validation:**
- Validates against system CA bundle
- Supports custom CA certificates
- Verifies hostname matching
- Checks revocation status (if configured)

---

#### Layer 7: Application Layer (Kafka)
- **Kafka Protocol**
  - Establishes Kafka connection
  - Performs SASL authentication
  - Retrieves broker metadata
  - Lists available topics
  - Checks topic visibility
  - Performs produce/consume test

**Example Output:**
```
✓ Kafka Metadata: 3 brokers, 42 partitions visible
✓ Topic Visibility: 'orders' found with 6 partitions
✓ Produce/Consume: Message round-trip successful (456ms)
```

---

#### Layer 7: HTTP Services
- **Schema Registry**
  - HTTP/HTTPS connectivity test
  - Basic authentication validation
  - Subject listing (/subjects endpoint)
  - Response time measurement

- **REST Proxy**
  - HTTP/HTTPS connectivity test
  - Topic listing (/topics endpoint)
  - Basic authentication validation

**Example Output:**
```
✓ Schema Registry: 12 subjects available
✓ REST Proxy: 42 topics accessible
```

---

### 2. End-to-End Data Flow Validation

When a topic is specified, kshark performs a complete produce/consume cycle:

**Process:**
1. Create a unique message ID
2. Produce message to specified topic
3. Consume message from same topic
4. Validate message content matches
5. Measure round-trip time

**Benefits:**
- Validates ACL permissions (both produce and consume)
- Tests serialization/deserialization
- Confirms end-to-end data flow
- Identifies partition assignment issues

**Command:**
```bash
./kshark -props client.properties -topic test-topic
```

---

## Connectivity Checks

### DNS Resolution Check
**Function:** `checkDNS()`
**Location:** `cmd/kshark/main.go:1069-1082`

**What it checks:**
- Hostname resolution to IP address(es)
- DNS query latency
- Multiple IP addresses (load balancing scenarios)

**Possible Outcomes:**
- ✓ **OK:** Hostname resolves successfully
- ✗ **FAIL:** DNS lookup fails

**Failure Hints:**
- Check DNS server configuration
- Verify hostname spelling
- Test with `nslookup` or `dig`

---

### TCP Connection Check
**What it checks:**
- TCP 3-way handshake completion
- Connection establishment time
- Port accessibility

**Possible Outcomes:**
- ✓ **OK:** TCP connection established
- ✗ **FAIL:** Connection refused or timeout

**Failure Hints:**
- Verify broker is running
- Check firewall rules
- Validate port number
- Test with `telnet` or `nc`

---

### TLS Handshake Check
**What it checks:**
- TLS version negotiation (min TLS 1.2)
- Certificate validation
- Certificate chain completeness
- Certificate expiry date
- Server name matching

**Possible Outcomes:**
- ✓ **OK:** TLS handshake successful
- ⚠ **WARN:** Certificate expires soon (<30 days)
- ✗ **FAIL:** TLS handshake failed

**Failure Hints:**
- Verify TLS is enabled on broker
- Check certificate validity
- Validate CA certificate
- Test with `openssl s_client`

---

### Kafka Protocol Check
**What it checks:**
- Kafka protocol handshake
- SASL authentication
- Metadata API access
- Topic visibility
- Produce/consume permissions

**Possible Outcomes:**
- ✓ **OK:** Kafka connection successful
- ✗ **FAIL:** Authentication or protocol error

**Failure Hints:**
- Verify SASL credentials
- Check authentication mechanism
- Validate ACL permissions
- Review Kafka broker logs

---

## Authentication Methods

### SASL/PLAIN

**Configuration:**
```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.username=your-username
sasl.password=your-password
```

**Use Cases:**
- Confluent Cloud
- Simple authentication scenarios
- Development environments

**Security Note:** Credentials sent in plaintext (use with TLS)

---

### SASL/SCRAM-SHA-256

**Configuration:**
```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.username=your-username
sasl.password=your-password
```

**Use Cases:**
- Enhanced security over PLAIN
- Modern Kafka deployments
- Compliance requirements

**Benefits:**
- Password never sent over network
- Mutual authentication
- Replay attack protection

---

### SASL/SCRAM-SHA-512

**Configuration:**
```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.username=your-username
sasl.password=your-password
```

**Use Cases:**
- Maximum security requirements
- Regulatory compliance
- High-security environments

**Benefits:**
- Stronger cryptographic hash
- All SCRAM-SHA-256 benefits

---

### Mutual TLS (mTLS)

**Configuration:**
```properties
security.protocol=SSL
ssl.ca.location=/path/to/ca-cert.pem
ssl.certificate.location=/path/to/client-cert.pem
ssl.key.location=/path/to/client-key.pem
```

**Use Cases:**
- Certificate-based authentication
- Zero-trust architectures
- Service-to-service communication

**Benefits:**
- No passwords to manage
- Certificate-based identity
- Mutual authentication

---

### SASL/GSSAPI (Kerberos)

**Note:** Requires special build tag

**Configuration:**
```properties
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.kerberos.principal=client@REALM.COM
sasl.kerberos.keytab=/path/to/client.keytab
```

**Use Cases:**
- Enterprise environments
- Active Directory integration
- Centralized authentication

**Build Command:**
```bash
go build -tags kerberos -o kshark ./cmd/kshark
```

---

## Configuration Options

### Complete Configuration Reference

#### Broker Connection

| Property | Description | Example |
|----------|-------------|---------|
| `bootstrap.servers` | Comma-separated broker list | `broker1:9092,broker2:9092` |

#### Security Protocol

| Property | Description | Values |
|----------|-------------|--------|
| `security.protocol` | Security protocol | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |

#### SASL Configuration

| Property | Description | Values |
|----------|-------------|--------|
| `sasl.mechanism` | SASL mechanism | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI` |
| `sasl.username` | SASL username | API key or username |
| `sasl.password` | SASL password | API secret or password |

#### TLS/SSL Configuration

| Property | Description | Example |
|----------|-------------|---------|
| `ssl.ca.location` | CA certificate path | `/path/to/ca-cert.pem` |
| `ssl.certificate.location` | Client certificate path | `/path/to/client-cert.pem` |
| `ssl.key.location` | Client private key path | `/path/to/client-key.pem` |
| `ssl.key.password` | Private key password | `key-password` |

#### Schema Registry

| Property | Description | Example |
|----------|-------------|---------|
| `schema.registry.url` | Schema Registry URL | `https://sr.example.com` |
| `basic.auth.user.info` | Basic auth credentials | `sr-key:sr-secret` |

#### REST Proxy

| Property | Description | Example |
|----------|-------------|---------|
| `rest.proxy.url` | REST Proxy URL | `https://rest.example.com` |

---

## Output Formats

### 1. Console Output (Default)

**Features:**
- Color-coded status indicators
- Grouped by layer
- Summary statistics
- TTY detection (colors only in terminal)

**Status Colors:**
- 🟢 **Green (OK):** Check passed
- 🟡 **Yellow (WARN):** Warning condition
- 🔴 **Red (FAIL):** Check failed
- ⚪ **Gray (SKIP):** Check skipped

**Example:**
```
╔═══════════════════════════════════════╗
║      kshark Diagnostic Report         ║
╚═══════════════════════════════════════╝

[L3: Network Layer]
✓ DNS Resolution: broker.example.com → 192.0.2.1

[L4: Transport Layer]
✓ TCP Connection: Connected (123ms)

Summary: 2 OK, 0 WARN, 0 FAIL, 0 SKIP
```

---

### 2. HTML Report

**Features:**
- Responsive web design
- Visual summaries per layer
- AI analysis section (if available)
- Configuration echo (redacted)
- Shareable report file

**Generated Location:**
```
reports/analysis_report_<hostname>_<timestamp>.html
```

**Sections:**
1. **Header:** Target hostname, timestamp
2. **AI Analysis:** Root cause and recommendations
3. **Summary:** Statistics per layer
4. **Detailed Results:** All checks with status
5. **Configuration:** Redacted properties (footer)

**Access:**
```bash
# Open in browser
open reports/analysis_report_broker.example.com_20250113_143022.html
```

---

### 3. JSON Export (Premium)

**Features:**
- Machine-readable format
- Complete report structure
- Credential redaction
- Integration-ready

**Command:**
```bash
./kshark -props client.properties -json output.json
```

**Structure:**
```json
{
  "timestamp": "2025-01-13T14:30:22Z",
  "host": "broker.example.com",
  "layers": {
    "L3": [
      {
        "component": "dns",
        "target": "broker.example.com",
        "status": "OK",
        "detail": "192.0.2.1",
        "hint": ""
      }
    ]
  },
  "config_echo": {
    "bootstrap.servers": "broker.example.com:9092",
    "sasl.password": "***"
  },
  "ai_analysis": { ... }
}
```

**Use Cases:**
- CI/CD integration
- Automated monitoring
- Metrics collection
- Historical analysis

---

## AI-Powered Analysis

### Overview

kshark integrates with AI providers to automatically analyze diagnostic results and provide intelligent recommendations.

### Supported Providers

#### 1. OpenAI

**Configuration:**
```json
{
  "provider": "openai",
  "api_key": "sk-...",
  "api_endpoint": "https://api.openai.com/v1/chat/completions",
  "model": "gpt-4"
}
```

**Models:**
- `gpt-4` (recommended)
- `gpt-4-turbo`
- `gpt-3.5-turbo`

---

#### 2. Scalytics-Connect

**Configuration:**
```json
{
  "provider": "scalytics",
  "api_key": "your-api-key",
  "api_endpoint": "https://api.scalytics.io/v1/analyze",
  "model": "kafka-diagnostics-v1"
}
```

---

#### 3. Custom AI Provider

**Configuration:**
```json
{
  "provider": "custom",
  "api_key": "your-api-key",
  "api_endpoint": "https://your-ai.example.com/v1/chat",
  "model": "your-model"
}
```

**Requirements:**
- OpenAI-compatible API format
- HTTPS endpoint
- JSON request/response

---

### Analysis Output

**What AI Analyzes:**
1. Failures and warnings in the report
2. Which OSI layer is problematic
3. Root cause of connectivity issues
4. Specific configuration problems
5. ACL or permission issues

**What AI Provides:**
1. Layer identification (e.g., "Issue at L5-6: TLS")
2. Root cause explanation
3. Step-by-step fix suggestions
4. Related documentation links
5. Prevention recommendations

**Example Analysis:**
```
🤖 AI Analysis:

Problem Layer: L7 (Kafka Protocol)

Root Cause:
The connection fails during SASL authentication with error
"SASL authentication failed". This indicates incorrect
credentials or mechanism mismatch.

Recommended Fix:
1. Verify sasl.username and sasl.password are correct
2. Confirm sasl.mechanism matches broker configuration
3. Check if the user has necessary ACLs:
   kafka-acls --list --principal User:your-username
4. Verify security.protocol is SASL_SSL (not SSL)

Prevention:
- Use environment variables for credentials
- Implement credential rotation
- Monitor authentication metrics
```

---

### Usage

**Command:**
```bash
./kshark -props client.properties -topic test-topic --analyze
```

**Requirements:**
1. Valid `license.key` file
2. `ai_config.json` with provider configuration
3. Network access to AI provider
4. Valid API key with sufficient quota

**Cost Considerations:**
- Each analysis is one API call
- Typical token usage: 500-2000 tokens
- Cost varies by provider and model
- Consider caching for repeated diagnostics

---

## Premium Features

Premium features require a valid `license.key` file.

### 1. AI-Powered Analysis

**See:** [AI-Powered Analysis](#ai-powered-analysis) section

**Benefit:** Intelligent root cause analysis

---

### 2. JSON Export

**Command:**
```bash
./kshark -props client.properties -json report.json
```

**Benefits:**
- Machine-readable output
- CI/CD integration
- Automated processing
- Historical trending

---

### License Management

**License File:** `license.key`

**Format:** JSON
```json
{
  "licensee": "Company Name",
  "expiry": "2026-01-13",
  "features": ["ai-analysis", "json-export"]
}
```

**Validation:**
- Checked at startup
- Expiry date validation
- Feature flag verification
- File must be in current directory

---

## Command-Line Interface

### Flags Reference

#### Required

| Flag | Description | Example |
|------|-------------|---------|
| `-props` | Properties file path | `-props client.properties` |

#### Optional

| Flag | Description | Default | Example |
|------|-------------|---------|---------|
| `-topic` | Topic to test | (none) | `-topic orders` |
| `-group` | Consumer group for probe | (ephemeral) | `-group kshark-probe` |
| `-timeout` | Global timeout for entire scan | 60s | `-timeout 120s` |
| `-kafka-timeout` | Kafka metadata/dial timeout | 10s | `-kafka-timeout 20s` |
| `-op-timeout` | Produce/consume timeout | 10s | `-op-timeout 30s` |
| `-produce-timeout` | Produce timeout (overrides `-op-timeout`) | (none) | `-produce-timeout 20s` |
| `-consume-timeout` | Consume timeout (overrides `-op-timeout`) | (none) | `-consume-timeout 45s` |
| `-start-offset` | Probe read start offset (`earliest|latest`) | `earliest` | `-start-offset latest` |
| `-balancer` | Probe partition balancer (`least|rr|random`) | `least` | `-balancer rr` |
| `-diag` | Enable traceroute/MTU diagnostics | true | `-diag=false` |
| `-log` | Write detailed scan log to file | auto | `-log /tmp/kshark.log` |
| `-y` | Skip confirmation | false | `-y` |
| `--analyze` | AI analysis | false | `--analyze` |
| `-no-ai` | Skip AI analysis even if enabled | false | `-no-ai` |
| `-provider` | AI provider name from `ai_config.json` | (default) | `-provider openai` |
| `-json` | JSON output file | (none) | `-json report.json` |
| `--preset` | Config preset | (none) | `--preset cc-plain` |
| `--version` | Show version | - | `--version` |

---

### Examples

**Basic connectivity check:**
```bash
./kshark -props client.properties
```

**With topic validation:**
```bash
./kshark -props client.properties -topic my-topic
```

**Automated (no prompts):**
```bash
./kshark -props client.properties -y
```

**Extended timeout:**
```bash
./kshark -props client.properties -timeout 120s
```

**With AI analysis:**
```bash
./kshark -props client.properties --analyze
```

**JSON export:**
```bash
./kshark -props client.properties -json output.json
```

**Using preset:**
```bash
./kshark --preset cc-plain -props client.properties
```

---

## Configuration Presets

Presets provide quick configuration templates for common Kafka distributions.

### 1. Confluent Cloud

**Preset:** `confluent-cloud`

**Pre-configured:**
```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
```

**Required Overrides:**
- `bootstrap.servers`
- `sasl.username`
- `sasl.password`

**Usage:**
```bash
./kshark --preset confluent-cloud \
  -override bootstrap.servers=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
  -override sasl.username=YOUR_API_KEY \
  -override sasl.password=YOUR_API_SECRET
```

---

### 2. Bitnami

**Preset:** `bitnami`

**Pre-configured:**
```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
```

**Required Overrides:**
- `bootstrap.servers`
- `sasl.username`
- `sasl.password`

**Usage:**
```bash
./kshark --preset bitnami \
  -override bootstrap.servers=kafka.local:9092 \
  -override sasl.username=user \
  -override sasl.password=password
```

---

### 3. AWS MSK

**Preset:** `aws-msk`

**Pre-configured:**
```properties
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
```

**Required Overrides:**
- `bootstrap.servers`

**Usage:**
```bash
./kshark --preset aws-msk \
  -override bootstrap.servers=b-1.msk-cluster.xxxxx.kafka.us-east-1.amazonaws.com:9098
```

**Note:** Requires AWS credentials in environment

---

### 4. Plaintext

**Preset:** `plaintext`

**Pre-configured:**
```properties
security.protocol=PLAINTEXT
```

**Required Overrides:**
- `bootstrap.servers`

**Usage:**
```bash
./kshark --preset plaintext \
  -override bootstrap.servers=localhost:9092
```

**Warning:** No encryption or authentication. Development only.

---

## Network Diagnostics

### Traceroute

**What it does:**
- Traces network path to broker
- Identifies routing hops
- Measures hop latency
- Detects network bottlenecks

**Platform Support:**
- Linux: `traceroute`
- macOS: `traceroute`
- Windows: `tracert`

**Output Limit:** 100 lines

**Example:**
```
Traceroute to broker.example.com (192.0.2.1):
 1  gateway (192.168.1.1)  1.234 ms
 2  isp-router (10.20.30.1)  5.678 ms
 3  backbone (203.0.113.1)  12.345 ms
 ...
```

---

### MTU Discovery

**What it does:**
- Tests Maximum Transmission Unit
- Uses ping with Don't Fragment flag
- Identifies MTU path limitations
- Detects fragmentation issues

**Platform Support:**
- Linux: `ping -M do -s <size>`
- macOS: `ping -D -s <size>`
- Windows: `ping -f -l <size>`

**Test Sizes:**
- 1472 bytes (Standard Ethernet: 1500 - 28 headers)
- 8972 bytes (Jumbo frames: 9000 - 28 headers)

**Example:**
```
MTU Check:
✓ 1472 bytes: Success (standard Ethernet)
✗ 8972 bytes: Fragmentation required (no jumbo frames)
```

---

## Use Cases

### 1. Troubleshooting Connectivity Issues

**Scenario:** Application cannot connect to Kafka

**Command:**
```bash
./kshark -props app-config.properties
```

**Benefit:** Identifies exact layer of failure (DNS, TCP, TLS, SASL)

---

### 2. Validating New Cluster Setup

**Scenario:** New Kafka cluster deployment

**Command:**
```bash
./kshark -props prod-kafka.properties -topic test-cluster-health
```

**Benefit:** Verifies all layers and end-to-end data flow

---

### 3. Monitoring Certificate Expiry

**Scenario:** Prevent certificate expiration outages

**Command:**
```bash
./kshark -props kafka.properties -y > /var/log/kshark.log
```

**Automation:** Daily cron job
```cron
0 6 * * * /usr/local/bin/kshark -props /etc/kafka/client.properties -y | grep -i "expires"
```

---

### 4. Continuous Monitoring

**Scenario:** Kubernetes health checks

**Deployment:** CronJob every 15 minutes

**Benefit:** Early detection of connectivity degradation

---

### 5. Pre-Deployment Validation

**Scenario:** CI/CD pipeline integration

**Command:**
```bash
./kshark -props $ENV-kafka.properties -topic ci-test -y -json report.json
```

**Integration:** Parse JSON for failures, fail build if issues detected

---

### 6. Customer Support

**Scenario:** Customer reports connection issues

**Command:**
```bash
./kshark -props customer-config.properties --analyze
```

**Benefit:** AI-powered diagnosis and fix suggestions

---

## Best Practices

### Configuration Management

1. **Never commit credentials**
   ```bash
   # Add to .gitignore
   echo "client.properties" >> .gitignore
   echo "ai_config.json" >> .gitignore
   ```

2. **Use environment variables**
   ```bash
   export KAFKA_PASSWORD="secret"
   # Reference in properties: ${KAFKA_PASSWORD}
   ```

3. **Secure file permissions**
   ```bash
   chmod 600 client.properties
   chmod 600 ai_config.json
   chmod 600 license.key
   ```

---

### Automation

1. **Non-interactive mode**
   ```bash
   ./kshark -props config.properties -y
   ```

2. **Timeout adjustment**
   ```bash
   ./kshark -props config.properties -timeout 30s
   ```

3. **Error handling**
   ```bash
   if ! ./kshark -props config.properties -y; then
     echo "Connectivity check failed"
     exit 1
   fi
   ```

---

### Security

1. **Use TLS always**
   ```properties
   security.protocol=SASL_SSL  # Not SASL_PLAINTEXT
   ```

2. **Prefer SCRAM over PLAIN**
   ```properties
   sasl.mechanism=SCRAM-SHA-256  # Not PLAIN
   ```

3. **Validate certificates**
   ```properties
   ssl.ca.location=/path/to/ca-cert.pem  # Don't skip validation
   ```

---

## Troubleshooting

### Issue: "license.key required"

**Solution:** AI analysis and JSON export are premium features. Either:
1. Obtain a license.key file
2. Use standard HTML/console output (free)

---

### Issue: DNS Resolution Fails

**Check:**
```bash
nslookup your-broker.example.com
```

**Solutions:**
- Verify hostname spelling
- Check DNS server configuration
- Test with IP address directly

---

### Issue: TLS Handshake Fails

**Check:**
```bash
openssl s_client -connect broker.example.com:9092 -showcerts
```

**Solutions:**
- Verify TLS is enabled on broker
- Check certificate validity
- Validate CA certificate path

---

### Issue: SASL Authentication Fails

**Check:**
- Credentials are correct
- Mechanism matches broker config (PLAIN vs SCRAM-SHA-256)
- ACLs are configured for the user

**Kafka ACL Check:**
```bash
kafka-acls --list --principal User:your-username
```

---

## Feature Roadmap

### Planned Features

- [ ] Multiple broker testing in parallel
- [ ] Historical trend analysis
- [ ] Prometheus metrics export
- [ ] Slack/PagerDuty integration
- [ ] REST API mode
- [ ] OAuth authentication support
- [ ] Kubernetes operator mode

---

**Document Version:** 1.0
**Author:** kshark Development Team
**Last Review:** 2025-11-13
**Next Review:** 2025-12-13
