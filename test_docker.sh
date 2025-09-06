#!/bin/bash

# test_docker.sh
#
# This script provides a simple way to test the kshark Docker image.
# It mounts the necessary configuration files from your local machine
# into the container and runs the kshark tool.
#
# Prerequisites:
# 1. A running Docker daemon.
# 2. A 'kshark:latest' Docker image built from the Dockerfile.
# 3. 'client.properties' and 'ai_config.json' files in the current directory.
#
# Usage:
# 1. Make the script executable: chmod +x test_docker.sh
# 2. Run the script: ./test_docker.sh

# Create a directory for the reports if it doesn't exist
mkdir -p reports

# Run the kshark container
docker run --rm -it \
  -v "$(pwd)/client.properties:/app/client.properties" \
  -v "$(pwd)/ai_config.json:/app/ai_config.json" \
  -v "$(pwd)/license.key:/app/license.key" \
  -v "$(pwd)/reports:/app/reports" \
  kshark:latest \
  --props client.properties \
  --topic partnersales \
  --json reports/report.json \
  --analyze
