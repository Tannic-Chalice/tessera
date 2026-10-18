#!/usr/bin/env bash
# Bengaluru only (simulated mixed traffic on real roads, no Delhi feed). Run ./reset.sh once when switching modes.
cd "$(dirname "${BASH_SOURCE[0]}")"
export SOURCE_MODE=synthetic
exec ./start.sh
