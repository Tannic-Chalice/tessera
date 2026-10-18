#!/usr/bin/env bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
N="${1:-3}"
if ! [[ "$N" =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: $0 [positive-instance-count]" >&2
  exit 2
fi
echo "Scaling the fast plane to $N instance(s). Kafka rebalances the 6 partitions of gps.raw;"
echo "each instance restores the state of the partitions it receives from its checkpoint."
docker compose up -d --no-recreate --scale "fastplane=$N" fastplane
sleep 15
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tessera-fastplane
