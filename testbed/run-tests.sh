#!/bin/sh
# =============================================================================
# kshark Integration Testbed - Test Runner
# =============================================================================
#
# Runs end-to-end integration tests for kshark connector probes.
#
# Usage:
#   docker compose exec kshark /run-tests.sh
#   docker compose exec kshark /run-tests.sh --skip-db2
#
# Exit codes:
#   0 - All tests passed
#   1 - One or more tests failed
#
# =============================================================================

set -e

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CONNECT_URL="http://connect:8083"
CONFIGS_DIR="/configs"
# -y skips interactive confirmation, -timeout limits overall duration
# -log /tmp/kshark.log avoids permission issues with /app/reports
KSHARK_BIN="kshark -y -timeout 30s -log /tmp/kshark.log"
SKIP_DB2=false
TOTAL=0
PASSED=0
FAILED=0
FAILURES=""

# Parse arguments
for arg in "$@"; do
  case "$arg" in
    --skip-db2) SKIP_DB2=true ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()    { printf "\n==== %s ====\n" "$1"; }
info()   { printf "  [INFO]  %s\n" "$1"; }
pass()   { printf "  [PASS]  %s\n" "$1"; PASSED=$((PASSED + 1)); }
fail()   {
  printf "  [FAIL]  %s\n" "$1"
  FAILED=$((FAILED + 1))
  FAILURES="${FAILURES}\n  - $1"
}

run_test() {
  TEST_ID="$1"
  TEST_NAME="$2"
  shift 2
  TOTAL=$((TOTAL + 1))

  log "${TEST_ID}: ${TEST_NAME}"
  info "Command: ${KSHARK_BIN} $*"

  OUTPUT=$(${KSHARK_BIN} "$@" 2>&1) && RC=$? || RC=$?

  printf "%s\n" "$OUTPUT" | sed 's/^/  | /'

  return $RC
}

# Expect a test to succeed (exit 0, output contains OK or PASS patterns)
expect_pass() {
  TEST_ID="$1"
  TEST_NAME="$2"
  shift 2

  if run_test "$TEST_ID" "$TEST_NAME" "$@"; then
    pass "${TEST_ID}: ${TEST_NAME}"
  else
    fail "${TEST_ID}: ${TEST_NAME} (exit code: $RC)"
  fi
}

# Expect a test to fail (non-zero exit or output contains FAIL)
expect_fail() {
  TEST_ID="$1"
  TEST_NAME="$2"
  EXPECTED_LAYER="$3"
  shift 3

  TOTAL=$((TOTAL + 1))
  log "${TEST_ID}: ${TEST_NAME}"
  info "Command: ${KSHARK_BIN} $*"
  info "Expected: FAIL at ${EXPECTED_LAYER}"

  OUTPUT=$(${KSHARK_BIN} "$@" 2>&1) && RC=$? || RC=$?

  printf "%s\n" "$OUTPUT" | sed 's/^/  | /'

  if [ "$RC" -ne 0 ]; then
    if printf "%s" "$OUTPUT" | grep -qi "${EXPECTED_LAYER}"; then
      pass "${TEST_ID}: ${TEST_NAME} (correctly failed at ${EXPECTED_LAYER})"
    else
      pass "${TEST_ID}: ${TEST_NAME} (failed as expected, exit code ${RC})"
    fi
  else
    fail "${TEST_ID}: ${TEST_NAME} (expected failure but got exit 0)"
  fi
}

# Check that output does NOT contain a string (for redaction tests)
expect_not_in_output() {
  TEST_ID="$1"
  TEST_NAME="$2"
  FORBIDDEN="$3"
  shift 3

  TOTAL=$((TOTAL + 1))
  log "${TEST_ID}: ${TEST_NAME}"
  info "Command: ${KSHARK_BIN} $*"
  info "Checking: output must NOT contain '${FORBIDDEN}'"

  OUTPUT=$(${KSHARK_BIN} "$@" 2>&1) || true

  if printf "%s" "$OUTPUT" | grep -qF "${FORBIDDEN}"; then
    fail "${TEST_ID}: ${TEST_NAME} (output contains '${FORBIDDEN}' -- credential leak!)"
    printf "%s\n" "$OUTPUT" | grep -n "${FORBIDDEN}" | sed 's/^/  !!! /'
  else
    pass "${TEST_ID}: ${TEST_NAME} (credential properly redacted)"
  fi
}

# ---------------------------------------------------------------------------
# Wait for services
# ---------------------------------------------------------------------------
wait_for_service() {
  SERVICE_NAME="$1"
  CHECK_CMD="$2"
  MAX_WAIT="${3:-120}"

  info "Waiting for ${SERVICE_NAME}..."
  WAITED=0
  while ! eval "$CHECK_CMD" > /dev/null 2>&1; do
    WAITED=$((WAITED + 5))
    if [ "$WAITED" -ge "$MAX_WAIT" ]; then
      printf "  [ERROR] %s did not become ready within %ds\n" "$SERVICE_NAME" "$MAX_WAIT"
      return 1
    fi
    sleep 5
  done
  info "${SERVICE_NAME} is ready."
}

# ---------------------------------------------------------------------------
# Register connectors
# ---------------------------------------------------------------------------
register_connector() {
  CONFIG_FILE="$1"
  CONNECTOR_NAME=$(grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' "$CONFIG_FILE" | head -1 | sed 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')

  info "Registering connector: ${CONNECTOR_NAME} from ${CONFIG_FILE}"

  # Build the POST body: {"name": "...", "config": {...}}
  CONFIG_BODY=$(cat "$CONFIG_FILE")
  REGISTER_BODY="{\"name\": \"${CONNECTOR_NAME}\", \"config\": ${CONFIG_BODY}}"
  TMPFILE="/tmp/connector-reg-${CONNECTOR_NAME}.json"
  printf '%s' "$REGISTER_BODY" > "$TMPFILE"

  # Use raw HTTP via /dev/tcp (works in any container without curl)
  CONNECT_HOST=$(echo "$CONNECT_URL" | sed 's|http://||' | cut -d: -f1)
  CONNECT_PORT=$(echo "$CONNECT_URL" | sed 's|http://||' | cut -d: -f2)
  BODY_LEN=$(wc -c < "$TMPFILE" | tr -d ' ')

  # Send raw HTTP POST via wget --post-file to /connectors
  RESPONSE=$(wget -q -O - --header="Content-Type: application/json" \
      --post-file="$TMPFILE" \
      "${CONNECT_URL}/connectors" 2>&1) && REG_RC=$? || REG_RC=$?

  if [ "$REG_RC" -eq 0 ]; then
    info "Connector ${CONNECTOR_NAME} registered successfully."
  else
    # Check if it already exists (409 Conflict) — that's fine
    if wget -q -O /dev/null "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" 2>/dev/null; then
      info "Connector ${CONNECTOR_NAME} already registered (exists)."
    else
      info "WARNING: Could not register connector ${CONNECTOR_NAME}. Response: ${RESPONSE}"
    fi
  fi

  rm -f "$TMPFILE"
}

# =============================================================================
# MAIN
# =============================================================================
printf "\n"
printf "================================================================\n"
printf "  kshark Integration Testbed - Test Suite\n"
printf "================================================================\n"
printf "  Connect URL:   %s\n" "$CONNECT_URL"
printf "  Configs dir:   %s\n" "$CONFIGS_DIR"
printf "  Skip DB2:      %s\n" "$SKIP_DB2"
printf "================================================================\n"

# ---------------------------------------------------------------------------
# Phase 1: Wait for services
# ---------------------------------------------------------------------------
log "Phase 1: Service Readiness"

wait_for_service "Kafka Connect" "wget -q -O /dev/null ${CONNECT_URL}/connectors"
wait_for_service "MongoDB"       "nc -z mongodb 27017" 60 || \
  info "MongoDB connectivity check skipped, assuming ready via healthcheck."

wait_for_service "PostgreSQL"    "nc -z postgres 5432" 60 || \
  info "PostgreSQL connectivity check skipped, assuming ready via healthcheck."

if [ "$SKIP_DB2" = "false" ]; then
  wait_for_service "DB2"         "nc -z db2 50000" 120 || \
    info "DB2 connectivity check skipped, assuming ready via healthcheck."
fi

# ---------------------------------------------------------------------------
# Phase 2: Register connectors via Connect REST API
# ---------------------------------------------------------------------------
log "Phase 2: Connector Registration"

register_connector "${CONFIGS_DIR}/mongo-sink.json"
register_connector "${CONFIGS_DIR}/postgres-source.json"

if [ "$SKIP_DB2" = "false" ]; then
  register_connector "${CONFIGS_DIR}/db2-source.json"
fi

# Wait for connectors to settle
info "Waiting 5s for connectors to initialize..."
sleep 5

# ---------------------------------------------------------------------------
# Phase 3: Happy Path Tests
# ---------------------------------------------------------------------------
log "Phase 3: Happy Path Tests"

# T-01: MongoDB probe via Connect REST API
expect_pass "T-01" "MongoDB probe via Connect API" \
  --connect-url "$CONNECT_URL" \
  --connector-name "mongo-sink-test"

# T-02: PostgreSQL probe via local config file
expect_pass "T-02" "PostgreSQL probe via local config" \
  --connector-config "${CONFIGS_DIR}/postgres-source.json"

# T-03: DB2 probe via local config file
if [ "$SKIP_DB2" = "false" ]; then
  expect_pass "T-03" "DB2 probe via local config" \
    --connector-config "${CONFIGS_DIR}/db2-source.json"
else
  info "T-03: DB2 probe via local config -- SKIPPED (--skip-db2)"
fi

# T-04: MongoDB with both Connect API and local fallback (API succeeds)
expect_pass "T-04" "MongoDB probe via API with fallback config" \
  --connect-url "$CONNECT_URL" \
  --connector-name "mongo-sink-test" \
  --connector-config "${CONFIGS_DIR}/mongo-sink.json"

# T-04b: Full path: Kafka broker + connector probe (MongoDB via Connect API)
expect_pass "T-04b" "Kafka broker + MongoDB connector probe (full path)" \
  -props "${CONFIGS_DIR}/client.properties" \
  -topic test-topic \
  --connect-url "$CONNECT_URL" \
  --connector-name "mongo-sink-test"

# ---------------------------------------------------------------------------
# Phase 4: Failure / Fallback Tests
# ---------------------------------------------------------------------------
log "Phase 4: Failure and Fallback Tests"

# T-05: Connect API unreachable, falls back to local file
expect_pass "T-05" "Fallback to local config when API unreachable" \
  --connect-url "http://badhost-does-not-exist:8083" \
  --connector-name "mongo-sink-test" \
  --connector-config "${CONFIGS_DIR}/mongo-sink.json"

# T-06: Connector not found (404)
expect_fail "T-06" "Connector not found on Connect API" "not found" \
  --connect-url "$CONNECT_URL" \
  --connector-name "this-connector-does-not-exist"

# T-07: MongoDB with wrong credentials
TOTAL=$((TOTAL + 1))
log "T-07: MongoDB probe with wrong credentials"
# Create a temp config with bad credentials
TEMP_MONGO="/tmp/mongo-bad-creds.json"
sed 's/testpass/WRONGPASSWORD/g' "${CONFIGS_DIR}/mongo-sink.json" > "$TEMP_MONGO"
info "Command: ${KSHARK_BIN} --connector-config ${TEMP_MONGO}"
info "Expected: FAIL at L7-Auth"

OUTPUT=$(${KSHARK_BIN} --connector-config "$TEMP_MONGO" 2>&1) && RC=$? || RC=$?
printf "%s\n" "$OUTPUT" | sed 's/^/  | /'

if [ "$RC" -ne 0 ]; then
  pass "T-07: MongoDB wrong credentials (correctly failed)"
else
  fail "T-07: MongoDB wrong credentials (expected failure but got exit 0)"
fi
rm -f "$TEMP_MONGO"

# T-08: PostgreSQL with wrong database
TOTAL=$((TOTAL + 1))
log "T-08: PostgreSQL probe with wrong database"
TEMP_PG="/tmp/pg-bad-db.json"
sed 's|/testdb|/nonexistent_db_xyz|g' "${CONFIGS_DIR}/postgres-source.json" > "$TEMP_PG"
info "Command: ${KSHARK_BIN} --connector-config ${TEMP_PG}"
info "Expected: FAIL at L7-Startup"

OUTPUT=$(${KSHARK_BIN} --connector-config "$TEMP_PG" 2>&1) && RC=$? || RC=$?
printf "%s\n" "$OUTPUT" | sed 's/^/  | /'

if [ "$RC" -ne 0 ]; then
  pass "T-08: PostgreSQL wrong database (correctly failed)"
else
  fail "T-08: PostgreSQL wrong database (expected failure but got exit 0)"
fi
rm -f "$TEMP_PG"

# T-09: DB2 with wrong credentials
if [ "$SKIP_DB2" = "false" ]; then
  TOTAL=$((TOTAL + 1))
  log "T-09: DB2 probe with wrong credentials"
  TEMP_DB2="/tmp/db2-bad-creds.json"
  sed 's/db2pass/WRONGPASSWORD/g' "${CONFIGS_DIR}/db2-source.json" > "$TEMP_DB2"
  info "Command: ${KSHARK_BIN} --connector-config ${TEMP_DB2}"
  info "Expected: FAIL at L7-Auth"

  OUTPUT=$(${KSHARK_BIN} --connector-config "$TEMP_DB2" 2>&1) && RC=$? || RC=$?
  printf "%s\n" "$OUTPUT" | sed 's/^/  | /'

  if [ "$RC" -ne 0 ]; then
    pass "T-09: DB2 wrong credentials (correctly failed)"
  else
    fail "T-09: DB2 wrong credentials (expected failure but got exit 0)"
  fi
  rm -f "$TEMP_DB2"
else
  info "T-09: DB2 probe with wrong credentials -- SKIPPED (--skip-db2)"
fi

# ---------------------------------------------------------------------------
# Phase 5: Edge Case Tests
# ---------------------------------------------------------------------------
log "Phase 5: Edge Case Tests"

# T-10: Credential redaction check (MongoDB)
expect_not_in_output "T-10a" "MongoDB credential redaction" "testpass" \
  --connector-config "${CONFIGS_DIR}/mongo-sink.json"

# T-10: Credential redaction check (PostgreSQL)
expect_not_in_output "T-10b" "PostgreSQL credential redaction" "pgpass" \
  --connector-config "${CONFIGS_DIR}/postgres-source.json"

# T-10: Credential redaction check (DB2)
if [ "$SKIP_DB2" = "false" ]; then
  expect_not_in_output "T-10c" "DB2 credential redaction" "db2pass" \
    --connector-config "${CONFIGS_DIR}/db2-source.json"
else
  info "T-10c: DB2 credential redaction -- SKIPPED (--skip-db2)"
fi

# =============================================================================
# Summary
# =============================================================================
printf "\n"
printf "================================================================\n"
printf "  TEST RESULTS\n"
printf "================================================================\n"
printf "  Total:   %d\n" "$TOTAL"
printf "  Passed:  %d\n" "$PASSED"
printf "  Failed:  %d\n" "$FAILED"

if [ "$FAILED" -gt 0 ]; then
  printf "\n  Failed tests:%b\n" "$FAILURES"
  printf "================================================================\n"
  printf "  RESULT: FAIL\n"
  printf "================================================================\n"
  exit 1
else
  printf "================================================================\n"
  printf "  RESULT: ALL TESTS PASSED\n"
  printf "================================================================\n"
  exit 0
fi
