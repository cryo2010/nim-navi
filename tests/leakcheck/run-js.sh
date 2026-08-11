#!/usr/bin/env bash
# Leak check for the navi/js backend for one SCENARIO.
#   SCENARIO = http1 http1s streamdown streamdownc sse ws wss
# `nim js` output cannot be run under Valgrind/ASan, so js_leak.nim samples the V8
# heap and the /proc/self/fd count around a long loop and asserts neither grows.
# Run under `node --expose-gc` so it can settle the heap before each sample.
set -euo pipefail

SCENARIO="${1:?usage: run-js.sh <scenario>}"
LEAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$LEAK_DIR/../.." && pwd)"
OUT="/tmp/navileak/js_leak.js"
mkdir -p /tmp/navileak

echo ">>> building js_leak ($SCENARIO)"
nim js --path:"$ROOT/src" -d:nodejs --hints:off -o:"$OUT" "$LEAK_DIR/js_leak.nim"

# shellcheck source=tests/leakcheck/run-server.sh
source "$LEAK_DIR/run-server.sh"
start_servers

# Node validates TLS against its own CA store; point it at our self-signed cert.
export NODE_EXTRA_CA_CERTS="$NAVI_LEAK_CERT"
export NAVI_LEAK_SCENARIO="$SCENARIO"
export NAVI_LEAK_ITERS="${NAVI_LEAK_ITERS:-100}"

node --expose-gc "$OUT"
echo "PASS: js $SCENARIO clean"
