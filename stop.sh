#!/usr/bin/env bash
cd "$(dirname "${BASH_SOURCE[0]}")"
echo "Stopping TESSERA (data and Kafka topics are kept; run ./start.sh to resume)..."
docker compose stop
