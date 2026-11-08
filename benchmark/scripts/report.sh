#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
exec benchmark/.runtime/venv/bin/python benchmark/scripts/bench.py report "$@"
