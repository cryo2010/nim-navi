#!/usr/bin/env bash
# navi/js WebSocket runtime test: run the navi/js client under Node against a
# native echo server (built from the sans-io core). Needs Node 22+ (global
# WebSocket) and a Nim toolchain. Fails if the client's asserts fail.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v node >/dev/null || { echo "node not found (need Node 22+ for global WebSocket)"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

port=9500
nim c --hints:off --path:"$root/src" -o:"$work/jsws_server" \
  "$root/tests/interop/jsws_server.nim"
nim js --hints:off --path:"$root/src" -o:"$work/jsws_client.js" \
  "$root/tests/interop/jsws_client.nim"

"$work/jsws_server" "$port" 2>"$work/srv.log" &
srv=$!
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && break
  sleep 0.1
done

node "$work/jsws_client.js"
