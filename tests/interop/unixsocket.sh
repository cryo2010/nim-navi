#!/usr/bin/env bash
# Unix domain socket transport: start an AF_UNIX HTTP server (echoes the Host
# header) and check navi dials it on the sync, asyncdispatch and chronos backends.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

work="$(mktemp -d)"
sock="$work/navi.sock"
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  rm -rf "$work"
}
trap cleanup EXIT

python3 "$root/tests/interop/uds_server.py" "$sock" >"$work/srv.log" 2>&1 &
pids+=($!)

ready=""
for _ in $(seq 1 50); do
  [ -S "$sock" ] && { ready=1; break; }
  sleep 0.1
done
[ -n "$ready" ] || { echo "UDS server did not start"; cat "$work/srv.log"; exit 1; }

export NAVI_UDS_PATH="$sock"

echo "== Unix domain socket: server on $sock =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/uds" "$root/tests/interop/unixsocket.nim"
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/uds_ad" "$root/tests/interop/unixsocket_async.nim"
nim c -r --hints:off -d:ssl -d:useChronos --path:"$root/src" -o:"$work/uds_ch" "$root/tests/interop/unixsocket_async.nim"
