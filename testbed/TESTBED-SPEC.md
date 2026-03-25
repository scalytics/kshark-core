# TESTBED-SPEC: kshark Connector Probe Integration Testbed

**Version**: 1.0
**Date**: 2026-03-25
**Status**: DRAFT
**Author**: Mirko Kampf (Confluent PS)
**Depends on**: SPEC-002 (Connector Config Probe)

---

## 1. Purpose

This testbed provides a fully self-contained Docker Compose environment for
end-to-end integration testing of kshark's connector probe feature (SPEC-002).
It validates that kshark can:

1. Retrieve connector configurations from a live Kafka Connect REST API
2. Parse connector configs from local JSON files
3. Probe MongoDB, PostgreSQL, and DB2 targets layer by layer (DNS, TCP, TLS, Auth, App)
4. Produce correct PASS/FAIL diagnostics for both healthy and broken connections
5. Fall back from Connect API to local config file when the API is unavailable

---

## 2. Architecture

```
 +------------------------------------------------------------------+
 |                       testbed-net (bridge)                        |
 |                                                                   |
 |  +-----------+     +-----------+     +-----------+                |
 |  |  mongodb  |     | postgres  |     |    db2    |                |
 |  |  :27017   |     |  :5432    |     |  :50000   |                |
 |  +-----------+     +-----------+     +-----------+                |
 |        ^                 ^                 ^                      |
 |        |                 |                 |                      |
 |        +--------+--------+--------+--------+                     |
 |                 |                                                 |
 |           +-----------+                                           |
 |           |  connect  |  Kafka Connect (REST :8083)               |
 |           |           |  - MongoDB Sink Connector                 |
 |           |           |  - JDBC Source (PostgreSQL)               |
 |           |           |  - JDBC Source (DB2)                      |
 |           +-----------+                                           |
 |                 |                                                 |
 |           +-----------+                                           |
 |           |   kafka   |  Confluent Platform (KRaft, :9092)        |
 |           +-----------+                                           |
 |                                                                   |
 |           +-----------+                                           |
 |           |   kshark  |  Probe test runner                        |
 |           |           |  - Registers connectors via REST          |
 |           |           |  - Runs kshark in all three modes         |
 |           |           |  - Reports PASS/FAIL per test             |
 |           +-----------+                                           |
 +------------------------------------------------------------------+
```

### Data Flow

```
run-tests.sh
  |
  +--> POST /connectors (Register mongo-sink, postgres-source, db2-source)
  |
  +--> kshark --connect-url http://connect:8083 --connector-name mongo-sink-test
  |      |
  |      +--> GET /connectors/mongo-sink-test/config
  |      +--> Parse MongoSinkConnector config
  |      +--> Probe mongodb:27017  (DNS -> TCP -> Auth -> Ping -> Collection)
  |      +--> Report PASS/FAIL
  |
  +--> kshark --connector-config /configs/postgres-source.json
  |      |
  |      +--> Parse local JSON (JdbcSourceConnector, jdbc:postgresql://...)
  |      +--> Probe postgres:5432  (DNS -> TCP -> Auth -> Ready)
  |      +--> Report PASS/FAIL
  |
  +--> kshark --connector-config /configs/db2-source.json
  |      |
  |      +--> Parse local JSON (JdbcSourceConnector, jdbc:db2://...)
  |      +--> Probe db2:50000  (DNS -> TCP -> DRDA -> Auth -> DB)
  |      +--> Report PASS/FAIL
  |
  +--> kshark --connect-url http://connect:8083 --connector-name mongo-sink-test \
  |           --connector-config /configs/mongo-sink.json
  |      |
  |      +--> Try Connect API (succeeds) -> uses API config
  |      +--> Report PASS/FAIL
  |
  +--> [Failure test] kshark --connect-url http://badhost:8083 \
  |           --connector-name mongo-sink-test \
  |           --connector-config /configs/mongo-sink.json
  |      |
  |      +--> Connect API fails -> falls back to local file
  |      +--> Probe mongodb:27017
  |      +--> Report PASS/FAIL (should pass with fallback warning)
```

---

## 3. Services

### 3.1 kafka

| Property      | Value                                      |
|---------------|--------------------------------------------|
| Image         | `confluentinc/cp-kafka:7.7.1`              |
| Mode          | KRaft (no ZooKeeper)                       |
| Listeners     | `PLAINTEXT://kafka:9092`                   |
| Healthcheck   | `kafka-broker-api-versions --bootstrap-server kafka:9092` |

Single-node Kafka broker providing the messaging backbone for Kafka Connect.

### 3.2 connect

| Property      | Value                                      |
|---------------|--------------------------------------------|
| Image         | `confluentinc/cp-kafka-connect:7.7.1`      |
| REST API      | `:8083`                                    |
| Plugins       | MongoDB Kafka Connector, Confluent JDBC Connector |
| Healthcheck   | `curl -f http://localhost:8083/connectors`  |

Kafka Connect worker with connector plugins installed at startup via
`confluent-hub install --no-prompt`. Connector configurations are registered
at test time via the REST API.

### 3.3 mongodb

| Property      | Value                                      |
|---------------|--------------------------------------------|
| Image         | `mongo:7.0`                                |
| Port          | `27017`                                    |
| Auth          | Enabled (`MONGO_INITDB_ROOT_USERNAME/PASSWORD`) |
| Credentials   | `testuser` / `testpass`                    |
| Database      | `testdb`                                   |
| Collection    | `events`                                   |
| Healthcheck   | `mongosh --eval "db.runCommand('ping')"`   |
| Init script   | `init/mongo-init.js`                       |

### 3.4 postgres

| Property      | Value                                      |
|---------------|--------------------------------------------|
| Image         | `postgres:16`                              |
| Port          | `5432`                                     |
| Credentials   | `pguser` / `pgpass`                        |
| Database      | `testdb`                                   |
| Table         | `customers`                                |
| Healthcheck   | `pg_isready -U pguser -d testdb`           |
| Init script   | `init/postgres-init.sql`                   |

### 3.5 db2

| Property      | Value                                      |
|---------------|--------------------------------------------|
| Image         | `icr.io/db2_community/db2:11.5.9.0`       |
| Port          | `50000`                                    |
| Credentials   | `db2inst1` / `db2pass`                     |
| Database      | `TESTDB`                                   |
| Table         | `ORDERS`                                   |
| Healthcheck   | `su - db2inst1 -c "db2 connect to TESTDB"` |
| Init script   | `init/db2-init.sql`                        |
| Platform      | `linux/amd64` (x86_64 only; see note below)|
| Privileged    | Yes (required by DB2 container)            |

**Apple Silicon note**: The DB2 Community Edition image is x86_64 only. On
Apple Silicon (M1/M2/M3/M4) Macs, Docker Desktop will use Rosetta 2 emulation
via `platform: linux/amd64`. This works but is significantly slower. DB2 startup
may take 5-10 minutes under emulation. If DB2 testing is not required, start the
testbed with `docker compose up -d --scale db2=0`.

### 3.6 kshark

| Property      | Value                                      |
|---------------|--------------------------------------------|
| Build         | Local `Dockerfile` (parent directory)      |
| Volumes       | `./configs:/configs`, `./run-tests.sh:/run-tests.sh` |
| Entrypoint    | `/run-tests.sh`                            |
| Depends on    | All other services (healthy)               |

---

## 4. Test Scenarios

### 4.1 Happy Path Tests

| ID    | Test Name                        | Mode                  | Expected Result |
|-------|----------------------------------|-----------------------|-----------------|
| T-01  | MongoDB via Connect API          | `--connect-url` + `--connector-name` | All layers PASS |
| T-02  | PostgreSQL via local config      | `--connector-config`  | All layers PASS |
| T-03  | DB2 via local config             | `--connector-config`  | All layers PASS (DRDA handshake + auth + DB access) |
| T-04  | MongoDB API + local fallback     | Both flags, API up    | Uses API config, all layers PASS |

### 4.2 Failure / Fallback Tests

| ID    | Test Name                        | Mode                  | Expected Result |
|-------|----------------------------------|-----------------------|-----------------|
| T-05  | Connect API unreachable fallback | `--connect-url` (bad) + `--connector-config` | Warning about API, falls back to file, probe PASS |
| T-06  | Connector not found              | `--connect-url` + bad `--connector-name` | FAIL with 404 hint |
| T-07  | MongoDB wrong credentials        | Modified config       | FAIL at L7-Auth |
| T-08  | PostgreSQL wrong database        | Modified config       | FAIL at L7-Startup |
| T-09  | DB2 wrong credentials            | Modified config       | FAIL at L7-Auth (SECCHKCD != 0x0000) |

### 4.3 Edge Case Tests

| ID    | Test Name                        | Mode                  | Expected Result |
|-------|----------------------------------|-----------------------|-----------------|
| T-10  | Credential redaction             | Any mode              | Output must not contain `testpass`, `pgpass`, or `db2pass` |

---

## 5. How to Run

### Prerequisites

- Docker Engine 24+ and Docker Compose v2
- At least 8 GB RAM allocated to Docker (DB2 alone needs ~2 GB)
- On Apple Silicon: Docker Desktop with Rosetta 2 enabled

### Quick Start

```bash
cd IMPL/kshark/testbed

# Start all services (first run pulls images and builds kshark)
docker compose up -d

# Watch logs until all healthchecks pass
docker compose logs -f

# Run the test suite (from the host, or exec into kshark container)
docker compose exec kshark /run-tests.sh

# Or run tests and view output directly
docker compose run --rm kshark /run-tests.sh
```

### Skip DB2 (Apple Silicon / faster iteration)

```bash
docker compose up -d --scale db2=0
docker compose exec kshark /run-tests.sh --skip-db2
```

### Teardown

```bash
docker compose down -v
```

---

## 6. File Manifest

```
testbed/
  TESTBED-SPEC.md            This document
  docker-compose.yml          Service definitions
  .env                        Environment variables
  run-tests.sh                Test runner script
  configs/
    mongo-sink.json           MongoDB sink connector config
    postgres-source.json      PostgreSQL JDBC source connector config
    db2-source.json           DB2 JDBC source connector config
  init/
    mongo-init.js             MongoDB initialization (user, db, collection, data)
    postgres-init.sql         PostgreSQL initialization (table, data)
    db2-init.sql              DB2 initialization (table, data)
```

---

## 7. Maintenance

- **Image versions**: Pinned to specific tags (`7.7.1`, `7.0`, `16`, `11.5.9.0`).
  Update `.env` to change versions.
- **Connector plugins**: Installed at connect container startup. Version pins are
  in `docker-compose.yml` command block.
- **Test data**: Minimal seed data in `init/` scripts. Add more as needed for
  new test scenarios.
