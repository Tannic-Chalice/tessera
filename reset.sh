#!/usr/bin/env bash
cd "$(dirname "${BASH_SOURCE[0]}")"
echo "This removes all containers, Kafka topics and the TimescaleDB volume."
read -r -p "Type YES to continue: " ok
if [[ "$ok" != "YES" ]]; then
  echo "Cancelled."
  exit 0
fi
docker compose down -v --remove-orphans
echo "Done. Run ./start.sh for a fresh pipeline."
