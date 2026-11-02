#!/bin/sh
# Verify a locally built image without contacting Kafka or an external service.
set -eu

image=${1:-kshark:local}
volume=$(docker volume create)
cleanup() {
    docker volume rm "$volume" >/dev/null
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

# A new Docker volume inherits /app/reports ownership from the image.
# Keep the default image user: overriding it would hide UID/permission bugs.
docker run --rm --network none --read-only --tmpfs /tmp \
    --mount "type=volume,src=$volume,dst=/app/reports" \
    --entrypoint /bin/sh "$image" -ec '
    test "$(id -u)" = 65532
    test "$(id -g)" = 65532
    printf "bootstrap.servers=127.0.0.1:1\nsecurity.protocol=PLAINTEXT\n" > /tmp/client.properties
    chmod 600 /tmp/client.properties

    set +e
    kshark -props /tmp/client.properties -y -no-ai -diag=false \
        -timeout 5s -kafka-timeout 250ms -json smoke.json \
        -bundle=/app/reports/smoke.tar.gz
    status=$?
    set -e

    # A closed local port must fail at TCP layer, while artifacts still persist.
    if [ "$status" -ne 2 ]; then
        echo "Expected TCP failure exit 2, got $status" >&2
        exit 1
    fi
    test -s /app/reports/smoke.json
    test -n "$(find /app/reports -name "report_sha256_*.txt" -size +0c -print)"
    test -s /app/reports/smoke.tar.gz
    test -n "$(find /app/reports -name "kshark-*.log" -size +0c -print)"
    '

# A second container must see artifacts left by the first one.
docker run --rm --network none --read-only \
    --mount "type=volume,src=$volume,dst=/app/reports,readonly" \
    --entrypoint /bin/sh "$image" -ec '
    test -s /app/reports/smoke.json
    for checksum in /app/reports/report_sha256_*.txt; do
        # kshark stores timestamp, report path and digest as tab-separated fields.
        while read -r timestamp report digest; do
            test -n "$timestamp"
            actual=$(sha256sum "$report")
            test "${actual%% *}" = "$digest"
        done < "$checksum"
    done
    tar -tzf /app/reports/smoke.tar.gz | grep -q "/MANIFEST.md$"
    tar -tzf /app/reports/smoke.tar.gz | grep -q "/report.json$"
    '

echo "Container smoke passed: UID/GID 65532, read-only root, persistent JSON, checksum, log and bundle."
