#!/usr/bin/env bash
# Backend stress harness: start the Node.js TLS test server and build one stress
# client per backend (sync, asyncdispatch, chronos, js), then run each against the
# server for NAVI_STRESS_SECONDS. Each client drives several navi clients
# (concurrently on the async backends), every HTTP verb, and a persistent
# WebSocket, over TLS with a middleware. Any failure (bad status/echo, crash)
# fails the job (set -e).
#
# Usually run via `nimble stress` (Dockerized, so Node and chronos are present).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
secs="${NAVI_STRESS_SECONDS:-20}"
clients="${NAVI_STRESS_CLIENTS:-3}"
port="${NAVI_STRESS_PORT:-9443}"
host=127.0.0.1
work="$(mktemp -d)"
cert="$work/cert.pem"; key="$work/key.pem"

command -v openssl >/dev/null || { echo "openssl required"; exit 127; }
command -v node >/dev/null || { echo "node required (server.mjs + navi/js; Node 18+)"; exit 127; }

srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

# Self-signed cert. DNS:127.0.0.1 (not just the IP SAN) so chronos's BearSSL,
# which matches the connect host against dNSName SANs, accepts the loopback IP.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$key" -out "$cert" -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1" >/dev/null 2>&1

common="--path:$root/src -d:ssl -d:release --hints:off"
echo "building clients..."
nim c $common               -o:"$work/sync"   "$root/tests/stress/stress_sync.nim"
nim c $common               -o:"$work/ad"     "$root/tests/stress/stress_async.nim"
nim c $common -d:useChronos  -o:"$work/ch"    "$root/tests/stress/stress_async.nim"
nim js --path:"$root/src" -d:release --hints:off -o:"$work/js.js" "$root/tests/stress/stress_js.nim"

export NAVI_STRESS_CERT="$cert" NAVI_STRESS_KEY="$key"
export NAVI_STRESS_HOST="$host" NAVI_STRESS_PORT="$port"
export NAVI_STRESS_URL="https://$host:$port"
export NAVI_STRESS_SECONDS="$secs" NAVI_STRESS_CLIENTS="$clients"

# The server is Node.js (production-grade TLS/HTTP/WebSocket); the Nim clients
# are what's under test.
node "$root/tests/stress/server.mjs" >"$work/server.log" 2>&1 &
srv=$!

# Wait for the TLS listener to accept.
ready=""
for _ in $(seq 1 50); do
  if openssl s_client -connect "$host:$port" </dev/null >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "server did not start"; cat "$work/server.log"; exit 1; }

echo "== stress: ${secs}s per backend, ${clients} clients each, over TLS =="
"$work/sync"
"$work/ad"
"$work/ch"
NODE_EXTRA_CA_CERTS="$cert" node "$work/js.js"
# Raw fetch/WebSocket baseline (no navi) for comparison.
NODE_EXTRA_CA_CERTS="$cert" node "$root/tests/stress/reference.mjs"
echo "== all backends passed =="
