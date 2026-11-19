#!/usr/bin/env bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
echo "Running unit tests inside Docker (no local Python needed)..."
docker compose build init
docker compose --profile test run --rm tests
