#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
umask 077
mkdir -p benchmark/.runtime
python3 -m venv benchmark/.runtime/venv
benchmark/.runtime/venv/bin/pip install -r benchmark/requirements.txt
benchmark/.runtime/venv/bin/python benchmark/scripts/bench.py init
# Official Apache binary + independently fetched SHA512; no system installation.
if [[ ! -x benchmark/.runtime/apache-jmeter-5.6.3/bin/jmeter ]]; then
  curl --fail --location --retry 3 https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-5.6.3.tgz -o benchmark/.runtime/jmeter.tgz
  curl --fail --location --retry 3 https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-5.6.3.tgz.sha512 -o benchmark/.runtime/jmeter.tgz.sha512
  python3 - <<'PY'
import hashlib, pathlib, re
p = pathlib.Path('benchmark/.runtime')
expected = re.search(r'\b[0-9a-fA-F]{128}\b', (p/'jmeter.tgz.sha512').read_text()).group().lower()
assert hashlib.sha512((p/'jmeter.tgz').read_bytes()).hexdigest() == expected, 'JMeter SHA512 mismatch'
PY
  tar -xzf benchmark/.runtime/jmeter.tgz -C benchmark/.runtime
fi
echo 'Ready. Next: ./benchmark/scripts/env.sh up && ./benchmark/scripts/env.sh start'
