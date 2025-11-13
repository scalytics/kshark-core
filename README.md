# kshark

![kshark title image](docs/images/title.png)

**A powerful command-line diagnostic tool for Apache Kafka connectivity**

`kshark` acts like a network sniffer for Kafka, providing comprehensive health checks of your entire client-to-broker communication path. It systematically tests every layer from DNS resolution through TLS security to Kafka protocol-level interactions, helping developers and SREs quickly identify and resolve connectivity issues.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23-blue.svg)](https://golang.org/)

---

## Table of Contents

- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)

---

## Key Features

### Comprehensive Connectivity Testing

-   **Layered Diagnostics:** Systematically tests all layers of connectivity:
    -   **L3 (Network):** DNS resolution and latency
    -   **L4 (Transport):** TCP connection establishment
    -   **L5-6 (Security):** TLS handshake, certificate validation, and expiry monitoring
    -   **L7 (Application):** Kafka protocol, metadata retrieval, topic visibility
    -   **L7 (HTTP):** Schema Registry and REST Proxy connectivity
    -   **Diagnostics:** Traceroute, MTU discovery, network path analysis

-   **End-to-End Testing:** Full produce-and-consume loop validation to verify complete data flow

-   **Multiple Authentication Methods:**
    -   SASL/PLAIN
    -   SASL/SCRAM-SHA-256
    -   SASL/SCRAM-SHA-512
    -   Mutual TLS (mTLS)
    -   SASL/GSSAPI (Kerberos) - with build tag

### AI-Powered Analysis (Premium)

-   **Intelligent Root Cause Detection:** Identifies which layer is causing issues
-   **Actionable Recommendations:** Provides specific fix suggestions
-   **Multiple AI Providers:** Supports OpenAI, Scalytics-Connect, and custom endpoints
-   **Automatic Problem Prioritization:** Focuses on critical failures first

### Developer-Friendly

-   **Familiar Configuration:** Java properties file format (works with existing Kafka configs)
-   **Rich Output Formats:**
    -   Color-coded console output
    -   Detailed HTML reports with visual summaries
    -   Machine-readable JSON export (Premium)
-   **Quick Presets:** Pre-configured templates for common Kafka distributions
    -   Confluent Cloud
    -   Bitnami
    -   AWS MSK
    -   Plaintext (development)

### Production-Ready

-   **Cross-Platform:** Linux, macOS, Windows (amd64, arm64)
-   **Docker Support:** Minimal Alpine-based container (~50MB)
-   **Kubernetes-Ready:** CronJob examples for continuous monitoring
-   **CI/CD Integration:** Automated releases via GitHub Actions + GoReleaser
-   **Security-Focused:**
    -   Credential redaction in reports
    -   Command injection prevention
    -   TLS 1.2+ enforcement
    -   Non-root container execution

---

## Quick Start

### 5-Minute Test

```bash
# 1. Download the latest release for your platform
wget https://github.com/your-org/kshark-core/releases/latest/download/kshark-linux-amd64.tar.gz
tar -xzf kshark-linux-amd64.tar.gz

# 2. Create a configuration file
cat > client.properties <<EOF
bootstrap.servers=your-broker.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.username=your-api-key
sasl.password=your-api-secret
EOF

# 3. Run the diagnostic
./kshark -props client.properties

# 4. Test with a specific topic
./kshark -props client.properties -topic test-topic
```

### Quick Preset Example

```bash
# Use a preset for Confluent Cloud
./kshark --preset confluent-cloud \
  -override bootstrap.servers=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
  -override sasl.username=YOUR_KEY \
  -override sasl.password=YOUR_SECRET
```

---

## Installation

### Option 1: Download Pre-built Binary (Recommended)

Download the latest release for your platform from the [Releases page](https://github.com/your-org/kshark-core/releases):

```bash
# Linux (amd64)
wget https://github.com/your-org/kshark-core/releases/latest/download/kshark-linux-amd64.tar.gz
tar -xzf kshark-linux-amd64.tar.gz

# macOS (arm64 - Apple Silicon)
wget https://github.com/your-org/kshark-core/releases/latest/download/kshark-darwin-arm64.tar.gz
tar -xzf kshark-darwin-arm64.tar.gz

# Windows (amd64)
wget https://github.com/your-org/kshark-core/releases/latest/download/kshark-windows-amd64.zip
unzip kshark-windows-amd64.zip
```

Verify the checksum:
```bash
sha256sum -c checksums.txt
```

### Option 2: Build from Source

**Prerequisites:** Go 1.23 or newer

```bash
# Clone the repository
git clone https://github.com/your-org/kshark-core.git
cd kshark-core

# Build
go build -o kshark ./cmd/kshark

# Verify
./kshark --version
```

### Option 3: Docker

```bash
# Pull the image (when published)
docker pull your-registry/kshark:latest

# Or build locally
docker build -t kshark:latest .

# Run with mounted configuration
docker run -v $(pwd):/config kshark:latest -props /config/client.properties
```

---

## Usage

### Basic Commands

```bash
# Basic connectivity check
./kshark -props client.properties

# Check with topic validation
./kshark -props client.properties -topic my-topic

# Include produce/consume test
./kshark -props client.properties -topic my-topic

# Skip confirmation prompt (for automation)
./kshark -props client.properties -y

# Adjust timeout
./kshark -props client.properties -timeout 120s

# Generate HTML report
./kshark -props client.properties -topic my-topic
# Report saved to: reports/analysis_report_<hostname>_<timestamp>.html
```

### Premium Features (Requires license.key)

```bash
# AI-powered analysis
./kshark -props client.properties -topic my-topic --analyze

# Export to JSON
./kshark -props client.properties -json report.json
```

### Using Presets

```bash
# Confluent Cloud
./kshark --preset confluent-cloud -override bootstrap.servers=YOUR_BROKER:9092 \
  -override sasl.username=KEY -override sasl.password=SECRET

# AWS MSK with IAM
./kshark --preset aws-msk -override bootstrap.servers=YOUR_MSK_ENDPOINT:9098

# Local development (no security)
./kshark --preset plaintext -override bootstrap.servers=localhost:9092
```

### Command-Line Flags

| Flag | Description | Default | Example |
|------|-------------|---------|---------|
| `-props` | Path to properties file | (required) | `-props config.properties` |
| `-topic` | Topic name to test | (optional) | `-topic orders` |
| `--preset` | Configuration preset | (none) | `--preset confluent-cloud` |
| `-override` | Override property value | (none) | `-override sasl.username=key` |
| `--analyze` | Enable AI analysis | false | `--analyze` |
| `-json` | Export to JSON file | (none) | `-json output.json` |
| `-timeout` | Connection timeout | 60s | `-timeout 120s` |
| `-y` | Skip confirmation prompt | false | `-y` |
| `--version` | Show version info | - | `--version` |

---

## Configuration

### Properties File Format

kshark uses standard Java properties format, compatible with Kafka client configurations:

```properties
# Broker connection
bootstrap.servers=broker1.example.com:9092,broker2.example.com:9092

# Security protocol
security.protocol=SASL_SSL

# SASL configuration
sasl.mechanism=SCRAM-SHA-256
sasl.username=your-username
sasl.password=your-password

# TLS configuration
ssl.ca.location=/path/to/ca-cert.pem
ssl.certificate.location=/path/to/client-cert.pem
ssl.key.location=/path/to/client-key.pem

# Optional: Schema Registry
schema.registry.url=https://schema-registry.example.com
basic.auth.user.info=sr-key:sr-secret

# Optional: REST Proxy
rest.proxy.url=https://rest-proxy.example.com
```

### Supported Configuration Options

See [Configuration Guide](docs/FEATURES.md#configuration-options) for complete list.

### AI Configuration (Optional)

For AI-powered analysis, create `ai_config.json`:

```json
{
  "provider": "openai",
  "api_key": "sk-...",
  "api_endpoint": "https://api.openai.com/v1/chat/completions",
  "model": "gpt-4"
}
```

Or use environment variables:
```bash
export KSHARK_AI_PROVIDER=openai
export KSHARK_AI_API_KEY=sk-...
```

---

## Documentation

Comprehensive documentation is available in the `docs/` directory:

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System architecture, components, and design patterns |
| [FEATURES.md](docs/FEATURES.md) | Complete feature list and usage examples |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Deployment guides for Docker, Kubernetes, and CI/CD |
| [SECURITY.md](docs/SECURITY.md) | Security audit, OWASP analysis, and recommendations |

### Quick Links

- **Architecture Overview:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Feature Documentation:** [docs/FEATURES.md](docs/FEATURES.md)
- **Deployment Guide:** [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)
- **Security Best Practices:** [docs/SECURITY.md](docs/SECURITY.md)
- **API Documentation:** [GoDoc](https://pkg.go.dev/github.com/your-org/kshark-core)

---

## Architecture

kshark uses a layered testing approach to systematically validate connectivity:

```
┌─────────────────────────────────────────┐
│  L3: Network Layer                      │
│  • DNS Resolution                       │
│  • Hostname to IP mapping               │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  L4: Transport Layer                    │
│  • TCP Connection                       │
│  • Latency Measurement                  │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  L5-6: Security Layer                   │
│  • TLS Handshake                        │
│  • Certificate Validation               │
│  • Expiry Monitoring                    │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  L7: Application Layer                  │
│  • Kafka Protocol                       │
│  • SASL Authentication                  │
│  • Metadata Retrieval                   │
│  • Produce/Consume Test                 │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Diagnostics                            │
│  • Traceroute / Path Analysis           │
│  • MTU Discovery                        │
└─────────────────────────────────────────┘
```

For detailed architecture information, see [ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Project Structure

```
kshark-core/
├── cmd/kshark/          # Main application source code
│   └── main.go          # Single-file application (1,350 lines)
├── web/templates/       # HTML report templates
├── docs/                # Documentation
│   ├── ARCHITECTURE.md  # Architecture overview
│   ├── FEATURES.md      # Feature documentation
│   ├── DEPLOYMENT.md    # Deployment guide
│   ├── SECURITY.md      # Security recommendations
│   └── images/          # Documentation images
├── .github/workflows/   # CI/CD automation
├── reports/             # Generated reports (gitignored)
├── Dockerfile           # Container build definition
├── .goreleaser.yaml     # Release configuration
├── go.mod               # Go module definition
├── LICENSE              # Apache 2.0 license
└── README.md            # This file
```

---

## Examples

### Example Output

**Console Output:**
```
╔═══════════════════════════════════════════════════════════════╗
║                    kshark Diagnostic Report                    ║
║                   Target: broker.example.com                   ║
╚═══════════════════════════════════════════════════════════════╝

[L3: Network Layer]
✓ DNS Resolution: broker.example.com → 192.0.2.1 (45ms)

[L4: Transport Layer]
✓ TCP Connection: 192.0.2.1:9092 established (123ms)

[L5-6: Security Layer]
✓ TLS Handshake: TLS 1.3 successful (234ms)
✓ Certificate: CN=broker.example.com, expires in 87 days
⚠ Certificate Expiry: Certificate expires in <90 days

[L7: Application Layer]
✓ Kafka Metadata: 3 brokers, 42 partitions
✓ Topic Visibility: 'orders' found with 6 partitions
✓ Produce/Consume: Message round-trip successful (456ms)

[Diagnostics]
✓ Network Path: 8 hops, avg latency 45ms
✓ MTU: 1500 bytes (standard Ethernet)

Summary: 9 OK, 1 WARN, 0 FAIL
```

### Docker Example

```bash
# Build image
docker build -t kshark:latest .

# Run diagnostic
docker run --rm \
  -v $(pwd)/client.properties:/config/client.properties:ro \
  -v $(pwd)/reports:/reports \
  kshark:latest -props /config/client.properties -topic test -y

# Check the report
open reports/analysis_report_*.html
```

### Kubernetes CronJob Example

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: kafka-health-check
spec:
  schedule: "*/15 * * * *"  # Every 15 minutes
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: kshark
            image: kshark:latest
            args: ["-props", "/config/client.properties", "-topic", "health-check", "-y"]
            volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
          volumes:
          - name: config
            secret:
              secretName: kafka-credentials
          restartPolicy: OnFailure
```

---

## Troubleshooting

### Common Issues

**Problem:** DNS resolution fails
```
Solution: Check DNS server configuration, verify hostname is correct
Check: nslookup your-broker.example.com
```

**Problem:** TLS handshake fails
```
Solution: Verify TLS version support, check certificate chain
Check: openssl s_client -connect broker.example.com:9092 -showcerts
```

**Problem:** SASL authentication fails
```
Solution: Verify credentials, check SASL mechanism matches broker config
Common issues: Wrong mechanism (PLAIN vs SCRAM), incorrect credentials
```

**Problem:** "license.key required" error
```
Solution: AI analysis and JSON export are premium features
Option 1: Obtain a license.key file
Option 2: Use standard console/HTML output (free)
```

### Debug Mode

For verbose output, check the generated HTML report which includes:
- Full configuration (credentials redacted)
- Detailed error messages
- Network diagnostic output
- Timestamp and version information

---

## Contributing

We welcome contributions! Please see our contributing guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/your-username/kshark-core.git
cd kshark-core

# Install dependencies
go mod download

# Run tests (if available)
go test ./...

# Build
go build -o kshark ./cmd/kshark

# Test locally
./kshark -props client.properties.example
```

---

## Security

For security concerns and vulnerability reports, please see [SECURITY.md](docs/SECURITY.md).

**Security Features:**
- Credential redaction in all outputs
- Command injection prevention
- Path traversal protection
- TLS 1.2+ enforcement
- Non-root container execution

**Known Security Considerations:**
- Credentials stored in plain text configuration files (use file permissions 0600)
- SSRF risk with Schema Registry URLs (validate URLs before use)
- See [SECURITY.md](docs/SECURITY.md) for detailed analysis and recommendations

---

## Roadmap

- [ ] Unit test coverage
- [ ] Concurrency for multi-broker checks
- [ ] Historical trend analysis
- [ ] Prometheus metrics export
- [ ] OpenTelemetry integration
- [ ] Modular architecture (separate packages)
- [ ] Additional authentication methods (OAuth)
- [ ] REST API mode

---

## License

This project is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for complete details.

```
Copyright 2025 kshark Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

## Acknowledgments

- Built with [segmentio/kafka-go](https://github.com/segmentio/kafka-go)
- Inspired by network diagnostic tools like `tcpdump`, `wireshark`, and `netcat`
- Special thanks to the Kafka community

---

## Support

- **Documentation:** [docs/](docs/)
- **Issues:** [GitHub Issues](https://github.com/your-org/kshark-core/issues)
- **Discussions:** [GitHub Discussions](https://github.com/your-org/kshark-core/discussions)

---

**Made with ❤️ for the Kafka community**
