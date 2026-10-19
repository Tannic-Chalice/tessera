#!/usr/bin/env bash
cd "$(dirname "${BASH_SOURCE[0]}")"
echo "===== containers ====="
docker compose ps
echo
echo "===== consumer groups (partition assignment + lag) ====="
for group in tessera-fastplane tessera-gate tessera-slowplane tessera-sink; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$group" 2>/dev/null || true
done
