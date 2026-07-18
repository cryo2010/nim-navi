#!/usr/bin/env bash
# Run one native wss client end to end: build and start the wss echo server, run
# the chosen backend's client against it, then stop the server. Backs the nimble
# demoWssSync / demoWssAsync / demoWssChronos tasks.
#
#   bash examples/websocket/wss_demo.sh <sync|asyncdispatch|chronos>
#
# No mkcert needed: the native clients use verify:false, so they accept the
# self-signed cert the server generates on first run.
set -euo pipefail

backend="${1:?usage: wss_demo.sh <sync|asyncdispatch|chronos>}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
here="$root/examples/websocket"
client="$here/wss_${backend}.nim"
[ -f "$client" ] || { echo "no such client: wss_${backend}.nim"; exit 2; }

builddir="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$builddir"; }
trap cleanup EXIT

echo "building the wss echo server and the $backend client ..."
nim c -d:ssl -d:release --hints:off -o:"$builddir/server" "$here/wss_echo_server.nim"
nim c -d:ssl -d:release --hints:off -o:"$builddir/client" "$client"

"$builddir/server" >"$builddir/server.log" 2>&1 &
srv=$!
disown   # keep bash from printing a "Terminated" notice when we kill it below

ready=""
for _ in $(seq 1 50); do
  if grep -q "WSS echo server on" "$builddir/server.log" 2>/dev/null; then ready=1; break; fi
  sleep 0.1
done
if [ -z "$ready" ]; then
  echo "the wss echo server did not start:"; cat "$builddir/server.log"; exit 1
fi

echo
"$builddir/client"
