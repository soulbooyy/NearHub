#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
if [[ ! -x benchmark/.runtime/venv/bin/python ]]; then
  echo 'Run ./benchmark/scripts/bootstrap.sh first.' >&2
  exit 1
fi
exec benchmark/.runtime/venv/bin/python benchmark/scripts/bench.py prepare "$@"
