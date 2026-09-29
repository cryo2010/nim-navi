#!/usr/bin/env bash
# A TLS write racing a connection close (issue #421). Start a TLS server that
# completes the handshake and then never reads, park a large SSL_write against it on
# the asyncdispatch backend, close the connection under the parked write, and check
# it raises "navi: connection closed" rather than calling SSL_write on the SSL that
# `freeConn` already freed. Before the fix the parked write resumed straight into
# SSL_write on the dangling pointer.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() {
  [ -n "$srv" ] && kill "$srv" 2>/dev/null || true
  cd "$root"
  navi_rmtree "$work"
}
trap cleanup EXIT
cd "$work"

port=9463
navi_certgen server.key server.pem 127.0.0.1 "IP:127.0.0.1"

python3 "$root/tests/interop/tls_deaf_server.py" "$port" \
  "$(navi_path "$work/server.pem")" "$(navi_path "$work/server.key")" \
  >"$work/srv.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

navi_wait_tls "127.0.0.1:$port" || { echo "deaf TLS server did not start"; cat "$work/srv.log"; exit 1; }

export NAVI_TWC_PORT="$port"

echo "== TLS write racing close on 127.0.0.1:$port =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/twc" "$root/tests/interop/tls_write_close.nim"
