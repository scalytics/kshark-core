---
layout: default
title: Home
nav_order: 1
permalink: /
---

# kshark Documentation

**A powerful command-line diagnostic tool for Apache Kafka and connector target connectivity.**

kshark acts like a network sniffer for Kafka, providing comprehensive health checks of your entire client-to-broker communication path -- and also validates connectivity from Kafka Connect workers to their source and sink database targets (MongoDB, PostgreSQL, DB2).

---

## Quick Links

| Document | Description |
|----------|-------------|
| [Architecture]({% link ARCHITECTURE.md %}) | System architecture, components, and design patterns |
| [Features]({% link FEATURES.md %}) | Complete feature list and configuration reference |
| [Deployment]({% link DEPLOYMENT.md %}) | Docker, Kubernetes, and CI/CD deployment guides |
| [Security]({% link SECURITY.md %}) | Security audit, OWASP analysis, and hardening |
| [Release Notes]({% link RELEASE.md %}) | Release history and upgrade notes |
| [Connector Probe]({% link connector-probe.md %}) | Probing MongoDB, PostgreSQL, and DB2 targets |
| [Contributing]({% link contributing.md %}) | How to contribute to kshark |
| [Changelog]({% link changelog.md %}) | Detailed change log |

---

## Layered Diagnostics

kshark systematically tests every layer of connectivity:

```
L3 (Network)      DNS resolution and latency
L4 (Transport)    TCP connection establishment
L5-6 (Security)   TLS handshake, certificate validation, expiry monitoring
L7 (Application)  Kafka protocol, SASL auth, metadata, produce/consume
L7 (HTTP)         Schema Registry and REST Proxy connectivity
Diagnostics       Traceroute, MTU discovery, network path analysis
```

## Highlights (RELEASE-1a)

- **Signal handling** -- SIGINT/SIGTERM gracefully cancel an in-progress scan
- **Environment variable expansion** -- use `${VAR}` in properties files
- **File permission warnings** -- warns when properties files are group/other readable
- **Fuzz testing** -- 4 fuzz targets covering security-critical parsers
- **478 test cases** with 47.8% total coverage
- **Control flow refactor** -- `runScan()` replaces `goto endScan`, all phases guarded by `ctx.Done()`

---

## Quick Start

```bash
# Download and run
./kshark -props client.properties -topic test-topic

# With Confluent Cloud preset
./kshark --preset cc-plain -props client.properties

# Non-interactive (CI/CD)
./kshark -props client.properties -y -timeout 120s

# Connector probe
./kshark --connect-url http://connect:8083 --connector-name my-sink
```

---

Built with care for the Kafka community by [Scalytics](https://scalytics.io).
