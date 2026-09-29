#!/usr/bin/env bash
# Truncation of a body delimited by the connection close (#426): run a TLS server
# that answers with an un-framed body and then cuts the connection without a
# close_notify (a RST, and a bare FIN), and check that navi refuses the short body
# instead of returning it as a complete 200. The same server's clean endings, a
# short Content-Length body, a keep-alive pair and an `openssl s_server -www` page
# are the regression side: those must keep working, on all three native clients.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

work="$(mktemp -d)"
srv=""
ssrv=""
cleanup() {
  [ -n "$srv" ] && kill "$srv" 2>/dev/null || true
  [ -n "$ssrv" ] && kill "$ssrv" 2>/dev/null || true
  cd "$root"          # Windows cannot remove the shell's own cwd
  navi_rmtree "$work"
}
trap cleanup EXIT

port=9472
sport=9473

navi_certgen "$work/key.pem" "$work/cert.pem" 127.0.0.1 "IP:127.0.0.1,DNS:127.0.0.1"

python3 "$root/tests/interop/truncate_tls_server.py" \
  "$work/cert.pem" "$work/key.pem" "$port" >"$work/srv.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

ready=""
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && { ready=1; break; }
  sleep 0.1
done
[ -n "$ready" ] || { echo "truncating TLS server did not start"; cat "$work/srv.log"; exit 1; }

# The real-world until-close body: s_server's status page carries no Content-Length
# and the connection closes after each reply, so only the close_notify ends it.
openssl s_server -accept "$sport" -cert "$work/cert.pem" -key "$work/key.pem" -www \
  >"$work/ssrv.log" 2>&1 &
ssrv=$!
disown 2>/dev/null || true
navi_wait_tls "127.0.0.1:$sport" || { echo "s_server did not start"; cat "$work/ssrv.log"; exit 1; }

export NAVI_TRUNC_URL="https://127.0.0.1:$port"
export NAVI_TRUNC_SSRV="https://127.0.0.1:$sport"
export NAVI_TRUNC_CA="$(navi_path "$work/cert.pem")"

echo "== unclean TLS close on 127.0.0.1:$port (s_server on :$sport) =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/trunc_sync" \
  "$root/tests/interop/tls_truncate.nim"
nim c -r --hints:off -d:ssl -d:useAsync --path:"$root/src" -o:"$work/trunc_async" \
  "$root/tests/interop/tls_truncate.nim"
if nimble path chronos >/dev/null 2>&1; then
  nim c -r --hints:off -d:ssl -d:useChronos --path:"$root/src" -o:"$work/trunc_chronos" \
    "$root/tests/interop/tls_truncate.nim"
else
  echo "note: chronos not installed; skipping the chronos unclean-close leg"
fi
