---
layout: default
title: Acceptance Protocol
nav_order: 3.5
---

# Post-Deployment Acceptance Protocol

**Version:** 1.0
**Last Updated:** 2026-03-27

---

## Table of Contents

1. [Overview](#overview)
2. [Workflow](#workflow)
3. [Step 1: Capture Infrastructure State](#step-1-capture-infrastructure-state)
4. [Step 2: Neighborhood Scan](#step-2-neighborhood-scan)
5. [Step 3: Create the Diagnostics Bundle](#step-3-create-the-diagnostics-bundle)
6. [Complete Example](#complete-example)
7. [Bundle Contents](#bundle-contents)
8. [Reading the Acceptance Protocol](#reading-the-acceptance-protocol)
9. [CI/CD Integration](#cicd-integration)
10. [FAQ](#faq)

---

## Overview

After deploying Kafka infrastructure (brokers, connectors, networking), you need a structured way to verify that everything works — without running aggressive network scans that could trigger IDS alerts or violate security policies.

kshark's **Acceptance Protocol** workflow combines three capabilities into a formal inspection report (**Prüfprotokoll / Abnahme-Protokoll**):

| Step | Capability | What it does |
|------|-----------|--------------|
| 1 | **Terraform State Grabber** | Captures infrastructure state with automatic credential redaction |
| 2 | **Neighborhood Scan** | Non-intrusive, targeted port probing to map network restrictions |
| 3 | **Diagnostics Bundle** | Packages everything into a signed `.tar.gz` archive for handoff |

**Key principle:** Capture the environment, don't attack it. All probes are targeted at known-relevant ports only — no port sweeps, no brute force, no aggressive scanning.

---

## Workflow

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  1. TF State     │     │  2. Neighborhood  │     │  3. Bundle       │
│     Capture      │────▶│     Scan          │────▶│     Creation     │
│                  │     │                   │     │                  │
│  • terraform     │     │  • Targeted ports │     │  • report.json   │
│    .tfstate      │     │  • Restriction    │     │  • report.html   │
│  • Credential    │     │    classification │     │  • kshark.log    │
│    redaction     │     │  • Broker         │     │  • TF state      │
│  • 24 sensitive  │     │    discovery      │     │  • System context│
│    patterns      │     │  • No aggressive  │     │  • MANIFEST.md   │
│                  │     │    scans          │     │  • SHA256 sums   │
└──────────────────┘     └──────────────────┘     └──────────────────┘
                                                          │
                                                          ▼
                                                  ┌──────────────────┐
                                                  │  Acceptance      │
                                                  │  Protocol        │
                                                  │  (.tar.gz)       │
                                                  │                  │
                                                  │  Ready for:      │
                                                  │  • Handoff       │
                                                  │  • Audit         │
                                                  │  • Support       │
                                                  └──────────────────┘
```

---

## Step 1: Capture Infrastructure State

The Terraform state grabber reads your `.tfstate` file and produces a redacted copy safe for inclusion in the bundle. No credentials, keys, or secrets leave the machine.

### Redacted Fields (24 patterns)

Sensitive fields are automatically replaced with `[REDACTED]`:

| Category | Patterns |
|----------|----------|
| Credentials | `password`, `secret`, `api_key`, `access_key`, `sasl.jaas.config` |
| Keys | `private_key`, `public_key`, `certificate_body`, `ssl_key` |
| Tokens | `token`, `bearer`, `session_id`, `cookie` |
| Connection | `connection_string`, `connection_url`, `jdbc_url` |
| Cloud | `client_secret`, `tenant_id`, `subscription_id` |

### Usage

```bash
# Point kshark at your Terraform state
./kshark -props client.properties \
  -tf-state /path/to/terraform.tfstate \
  -bundle

# Optionally include the Terraform plan output
./kshark -props client.properties \
  -tf-state /path/to/terraform.tfstate \
  -tf-plan /path/to/plan.txt \
  -bundle
```

### Safety Limits

- **50 MB** — warning threshold (state files this large may indicate embedded blobs)
- **200 MB** — hard reject (prevents accidental inclusion of large binary state)

---

## Step 2: Neighborhood Scan

Instead of scanning every port on every host, kshark probes only well-known ports that are relevant to the deployment. This approach is:

- **Targeted** — only ports that Kafka, Connect, or connectors actually use
- **Non-hostile** — no SYN floods, no brute force, no banner grabbing
- **Fast** — concurrent probes with 3s per-port and 5s total timeout
- **Informative** — classifies restrictions, not just open/closed

### Default Port Set

| Port | Service |
|------|---------|
| 80 | HTTP (proxy/LB health) |
| 443 | HTTPS (Schema Registry, REST Proxy) |
| 9092 | Kafka (PLAINTEXT/SASL_PLAINTEXT) |
| 9093 | Kafka (SSL/SASL_SSL) |
| 9094 | Kafka (additional listener) |
| 8081 | Schema Registry |
| 8083 | Kafka Connect REST API |

### Connector-Specific Ports

When probing connectors, kshark adds the target database ports:

| Connector Type | Additional Ports |
|---------------|-----------------|
| MongoDB | 27017, 27018, 27019 |
| PostgreSQL | 5432 |
| MySQL | 3306 |
| DB2 | 50000, 50001 |
| SQL Server | 1433 |
| Oracle | 1521 |
| Elasticsearch | 9200, 9300 |
| Redis | 6379 |

### Restriction Classification

The scan classifies network restrictions with confidence levels:

| Classification | Meaning | Confidence |
|---------------|---------|------------|
| `selective_port_filtering` | Some ports open, others blocked (firewall rules) | High |
| `all_tcp_blocked` | No TCP ports reachable (network-level block) | High |
| `host_unreachable` | ICMP fails too — host or route is down | High |
| `service_not_listening` | Port refused — host reachable, service not running | Medium |
| `connection_timeout` | Possible firewall drop (no RST, no ICMP unreachable) | Medium |

### Usage

```bash
# Auto-triggers on TCP failure (default when -diag is enabled)
./kshark -props client.properties

# Force neighborhood scan even when TCP succeeds
./kshark -props client.properties -neighborhood

# Custom port set
./kshark -props client.properties -neighborhood -neighborhood-ports "9092,9093,443,8083,27017"
```

### Broker Discovery

When kshark successfully connects to a bootstrap broker, it also discovers all advertised brokers from metadata and probes non-bootstrap brokers for reachability. This catches the most common Kafka misconfiguration: **advertised listener mismatches**.

---

## Step 3: Create the Diagnostics Bundle

The bundle packages all diagnostic results into a single `.tar.gz` archive.

```bash
# Create bundle with default name (auto-generated timestamp)
./kshark -props client.properties -bundle -y

# Create bundle at a specific path
./kshark -props client.properties -bundle /tmp/acceptance-report.tar.gz -y

# Full acceptance protocol: TF state + neighborhood + bundle
./kshark -props client.properties \
  -tf-state terraform.tfstate \
  -neighborhood \
  -bundle /tmp/acceptance.tar.gz \
  -y
```

After bundle creation, kshark prints ready-to-use export commands based on the detected environment:

```
Export commands:
  scp user@host:/tmp/acceptance.tar.gz .
  docker cp <container>:/tmp/acceptance.tar.gz .
  kubectl cp <namespace>/<pod>:/tmp/acceptance.tar.gz ./acceptance.tar.gz
```

---

## Complete Example

A full acceptance protocol run after deploying a Confluent Cloud environment with Terraform:

```bash
# 1. Run the complete acceptance protocol
./kshark \
  -props client.properties \
  -topic acceptance-test \
  -tf-state ./terraform/terraform.tfstate \
  -tf-plan ./terraform/plan.txt \
  -probe-direction full \
  -neighborhood \
  -bundle ./reports/acceptance-$(date +%Y%m%d).tar.gz \
  -y

# 2. Review the HTML report
open ./reports/analysis_report_*.html

# 3. Transfer the bundle for archival or support
scp ./reports/acceptance-*.tar.gz support@internal:/incoming/
```

### What this does

1. Tests all layers (DNS → TCP → TLS → Kafka → Produce/Consume) in `full` mode (no fail-fast)
2. Captures and redacts the Terraform state and plan
3. Runs neighborhood scan on all relevant ports
4. Discovers all brokers from metadata and checks reachability
5. Collects system context (OS, interfaces, routes, DNS config)
6. Packages everything into a timestamped `.tar.gz` with SHA256 manifest

---

## Bundle Contents

```
acceptance-20260327.tar.gz
├── report.json                          # Machine-readable scan results
├── report.html                          # Human-readable HTML report
├── kshark.log                           # Detailed scan log
├── terraform/
│   ├── terraform.tfstate.redacted       # Infrastructure state (credentials removed)
│   └── terraform-plan.txt               # Plan output (credentials removed)
├── config/
│   ├── client.properties.redacted       # Kafka client config (credentials removed)
│   └── connector.json.redacted          # Connector config if applicable
├── context/
│   ├── env.txt                          # OS, arch, Go version, hostname
│   ├── dns-resolv.conf                  # DNS resolver configuration
│   ├── network-interfaces.txt           # Network interfaces and IPs
│   └── routes.txt                       # Routing table
└── MANIFEST.md                          # File listing with SHA256 checksums
```

All files containing credentials are automatically redacted before inclusion.

---

## Reading the Acceptance Protocol

The bundle serves as a formal record of the deployment state. Here's how to interpret it:

### For Operators / Deployment Sign-Off

1. Open `report.html` — check that all layers show green
2. Review the neighborhood scan section — confirm expected ports are open
3. Check broker discovery — verify all advertised listeners are reachable
4. Archive the `.tar.gz` as the official acceptance record

### For Support / Troubleshooting

1. Start with `report.json` — machine-parseable, includes all timings
2. Check `context/` — environment details help reproduce issues
3. Review `terraform/terraform.tfstate.redacted` — verify resource configuration
4. Read `kshark.log` — full diagnostic trace with timestamps

### For Compliance / Audit

1. Verify `MANIFEST.md` — SHA256 checksums confirm bundle integrity
2. Confirm no credentials in redacted files — grep for known patterns
3. The bundle itself is the evidence that connectivity was validated post-deployment

---

## CI/CD Integration

### GitHub Actions

```yaml
- name: Post-deployment acceptance check
  run: |
    ./kshark \
      -props ${{ secrets.KAFKA_PROPS_PATH }} \
      -topic ci-acceptance-test \
      -tf-state ./terraform/terraform.tfstate \
      -neighborhood \
      -bundle ./acceptance-report.tar.gz \
      -y

- name: Upload acceptance protocol
  uses: actions/upload-artifact@v4
  with:
    name: acceptance-protocol
    path: ./acceptance-report.tar.gz
```

### Kubernetes Job

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: kshark-acceptance
spec:
  template:
    spec:
      containers:
      - name: kshark
        image: scalytics/kshark:latest
        command:
        - /kshark
        - -props
        - /config/client.properties
        - -topic
        - acceptance-test
        - -neighborhood
        - -bundle
        - /output/acceptance.tar.gz
        - -y
        volumeMounts:
        - name: config
          mountPath: /config
        - name: output
          mountPath: /output
      volumes:
      - name: config
        secret:
          secretName: kafka-client-properties
      - name: output
        emptyDir: {}
      restartPolicy: Never
```

---

## FAQ

### Is the neighborhood scan safe to run in production?

Yes. It only probes well-known ports with standard TCP connect calls. There are no SYN scans, no banner grabbing, no protocol fuzzing. The scan is indistinguishable from a normal application connecting to a service.

### What if my Terraform state is too large?

kshark warns at 50 MB and rejects at 200 MB. If your state exceeds these limits, extract the relevant resources with `terraform state pull | jq '.resources[] | select(.type | startswith("confluent_"))'` and pass the filtered file.

### Can I use this without Terraform?

Absolutely. The Terraform flags are optional. Without `-tf-state`, the bundle still includes the scan results, system context, and configurations.

### How do I verify bundle integrity?

```bash
tar xzf acceptance.tar.gz
# Check manifest
cat MANIFEST.md
# Verify a specific file
sha256sum report.json
```

### What ports does the neighborhood scan hit by default?

80, 443, 9092, 9093, 9094, 8081, 8083. Override with `-neighborhood-ports`.

---

**Document Version:** 1.0
**Author:** kshark Development Team
**Last Review:** 2026-03-27
