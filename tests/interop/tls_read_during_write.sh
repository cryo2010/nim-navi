#!/usr/bin/env bash
# The chronos TLS pump keeps reading while its own write is in flight (#444):
# run a TLS server that greets the client and then stops reading, have the client
# start a 4 MiB write it cannot finish, and check that the greeting still reaches
# `readSome` instead of sitting behind the parked write.
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

port=9489            # unique across tests/interop: connect_abandon.sh holds 9476

navi_certgen "$work/key.pem" "$work/cert.pem" 127.0.0.1 "IP:127.0.0.1,DNS:127.0.0.1"

python3 "$root/tests/interop/slow_reader_tls_server.py" \
  "$work/cert.pem" "$work/key.pem" "$port" PING \
  >"$work/srv.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

ready=""
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && { ready=1; break; }
  sleep 0.1
done
[ -n "$ready" ] || { echo "slow-reader TLS server did not start"; cat "$work/srv.log"; exit 1; }

export NAVI_RDW_PORT="$port"
export NAVI_RDW_CA="$(navi_path "$work/cert.pem")"

echo "== chronos TLS pump: read during an in-flight write on 127.0.0.1:$port =="
nim c -r --hints:off -d:ssl --path:"$root/src" \
  -o:"$work/tls_read_during_write" "$root/tests/interop/tls_read_during_write.nim"
