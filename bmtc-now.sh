#!/usr/bin/env bash
# Real BMTC buses currently in TESSERA (from Kafka gps.raw). Usage: ./bmtc-now.sh [minutes]
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
MIN="${1:-3}"
if ! command -v docker >/dev/null 2>&1; then
  echo "[X] Docker is not installed or not on PATH."
  exit 1
fi
cat scripts/bmtc_now.py | docker compose exec -T producer python - "$MIN"
