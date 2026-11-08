#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
umask 077
mkdir -p benchmark/.runtime/validation
printf 'token,userId\n' > benchmark/.runtime/validation/empty.csv
# Empty CSV stops the only thread before any network request. Validate actual JMeter deserialization.
benchmark/.runtime/apache-jmeter-5.6.3/bin/jmeter -n \
  -t benchmark/jmeter/nearhub-seckill.jmx -q benchmark/jmeter/results.properties \
  -Jthreads=1 -Jrampup=0 -Jduration=1 \
  -Jtokens="$PWD/benchmark/.runtime/validation/empty.csv" \
  -Jclassifier="$PWD/benchmark/jmeter/classify.groovy" \
  -j benchmark/.runtime/validation/jmeter.log > benchmark/.runtime/validation/console.log 2>&1
if rg -q 'ERROR|Error in NonGUIDriver' benchmark/.runtime/validation/{jmeter.log,console.log}; then
  echo 'JMeter plan validation failed; see benchmark/.runtime/validation/' >&2
  exit 1
fi
java -cp 'benchmark/.runtime/apache-jmeter-5.6.3/lib/*:benchmark/.runtime/apache-jmeter-5.6.3/lib/ext/*' \
  groovy.ui.GroovyMain benchmark/tests/classification.groovy
printf 'JMX loaded with zero HTTP requests; response classifications verified.\n'
