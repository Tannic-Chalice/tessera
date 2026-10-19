#!/usr/bin/env bash
# REAL live data, Delhi only. The key is read from .env (OTD_API_KEY=...).
# Run ./reset.sh once when switching modes.
cd "$(dirname "${BASH_SOURCE[0]}")"
if ! grep -q '^OTD_API_KEY=.' .env 2>/dev/null; then
  read -r -p "Paste your Delhi OTD API key and press Enter: " OTD_API_KEY
  export OTD_API_KEY
fi
export SOURCE_MODE=delhi
exec ./start.sh
