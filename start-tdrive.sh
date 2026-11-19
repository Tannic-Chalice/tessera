#!/usr/bin/env bash
# Benchmark mode: replay Microsoft T-Drive taxi traces (Beijing) instead of the live Bengaluru feed.
# 1. Download T-Drive from Microsoft Research and copy the .txt files into data/tdrive/
# 2. Run ./reset.sh once when switching between Bengaluru and T-Drive (clears data from the previous city)
# 3. Run this file. To go back to Bengaluru: ./reset.sh, then ./start.sh
cd "$(dirname "${BASH_SOURCE[0]}")"
export SOURCE_MODE=tdrive
export SPEEDUP=60
exec ./start.sh
