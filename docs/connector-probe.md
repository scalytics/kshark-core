---
layout: default
title: Connector Probe
nav_order: 7
---

# Connector Probe

kshark can probe the connectivity between Kafka Connect workers and their source/sink database targets. It supports **MongoDB**, **PostgreSQL**, and **DB2**.

---

## How It Works

1. **Fetch config** from Kafka Connect REST API or a local JSON file
2. **Parse** the connector class and extract connection parameters (host, port, credentials, TLS)
3. **Probe** the target database layer by layer: DNS -> TCP -> TLS -> Auth -> Application

## Supported Connector Types

| Connector Class | Database | Protocol |
|----------------|----------|----------|
| `MongoSinkConnector` / `MongoSourceConnector` | MongoDB | MongoDB wire protocol |
| `JdbcSourceConnector` / `JdbcSinkConnector` (PostgreSQL) | PostgreSQL | PostgreSQL startup + auth |
| `JdbcSourceConnector` / `JdbcSinkConnector` (DB2) | IBM DB2 | DRDA wire protocol |

## Usage

### Via Kafka Connect REST API

```bash
# Probe a connector by name (fetches config from Connect API)
./kshark --connect-url https://connect:8083 --connector-name my-mongo-sink

# With authentication
./kshark --connect-url https://connect:8083 \
  --connector-name my-mongo-sink \
  --connect-basic-auth user:password

# With bearer token
./kshark --connect-url https://connect:8083 \
  --connector-name my-mongo-sink \
  --connect-bearer-token eyJhbG...
```

### Via Local Config File

```bash
# Probe using a local connector config JSON file
./kshark --connector-config configs/postgres-source.json
```

### Fallback Mode

```bash
# Try Connect API first, fall back to local file if API unreachable
./kshark --connect-url https://connect:8083 \
  --connector-name my-sink \
  --connector-config configs/fallback.json
```

## Config File Format

Local config files are JSON with connector configuration:

```json
{
  "name": "my-postgres-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://db-host:5432/mydb?sslmode=require",
    "connection.user": "dbuser",
    "connection.password": "dbpass"
  }
}
```

## Probe Layers

Each probe produces step-by-step diagnostics:

| Layer | What's Checked | Example Output |
|-------|---------------|----------------|
| L3-DNS | Hostname resolution | `Resolved to 10.0.1.5` |
| L4-TCP | TCP connection | `Connected to db:5432 in 12ms` |
| L5-6-TLS | TLS handshake (if enabled) | `TLS 1.3, CN=*.example.com` |
| L7-Auth | Database authentication | `Authenticated as 'dbuser'` |
| L7-App | Application-level check | `Connected to database 'mydb'` |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `KSHARK_CONNECT_AUTH` | Fallback for `--connect-basic-auth` (avoids shell history) |
| `KSHARK_CONNECT_TOKEN` | Fallback for `--connect-bearer-token` |

## Credential Redaction

All connector credentials are automatically redacted in:
- Console output
- HTML reports
- JSON exports
- Log files
