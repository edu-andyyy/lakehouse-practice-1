#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
python3 -u scripts/lakehouse.py all "$@" 2>&1 | tee results/run.log
