#!/usr/bin/env bash
# Cancellation through the chronos TLS pump (#430): run a TLS server that
# completes the handshake, reads the request and then answers nothing, and check
# that a read timeout and a total-request deadline both reach the caller promptly
# and without the request being replayed on a second connection.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() {
  [ -n "$srv" ] && kill "$srv" 2>/dev/null || true
  cd "$root"          # Windows cannot remove the shell's own cwd
  navi_rmtree "$work"
}
trap cleanup EXIT

port=9461

navi_certgen "$work/key.pem" "$work/cert.pem" 127.0.0.1 "IP:127.0.0.1,DNS:127.0.0.1"

: > "$work/accepts.log"
python3 "$root/tests/interop/stall_tls_server.py" \
  "$work/cert.pem" "$work/key.pem" "$port" "$work/accepts.log" \
  >"$work/srv.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

ready=""
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && { ready=1; break; }
  sleep 0.1
done
[ -n "$ready" ] || { echo "stall TLS server did not start"; cat "$work/srv.log"; exit 1; }

export NAVI_STALL_URL="https://127.0.0.1:$port"
export NAVI_STALL_CA="$(navi_path "$work/cert.pem")"
export NAVI_STALL_LOG="$(navi_path "$work/accepts.log")"

echo "== TLS pump cancellation: stalling server on 127.0.0.1:$port =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/tls_stall" "$root/tests/interop/tls_stall.nim"
