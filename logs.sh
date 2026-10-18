#!/usr/bin/env bash
cd "$(dirname "${BASH_SOURCE[0]}")"
# Usage: ./logs.sh            (pipeline services)
#        ./logs.sh fastplane  (one service: producer fastplane gate slowplane sink dashboard kafka)
if (($# == 0)); then
  docker compose logs -f --tail 40 producer fastplane gate slowplane sink
else
  docker compose logs -f --tail 80 "$@"
fi
