#!/usr/bin/env bash
# Custom-CA (TlsConfig.caFile) interop for the chronos/BearSSL backend: generate
# a CA, sign a server cert with it, start an OpenSSL HTTPS server, and have navi/
# chronos verify that server against the CA. Tears everything down on exit.
#
# The server cert carries a DNS-type SAN "127.0.0.1" because BearSSL matches the
# requested name against dNSName SANs (not iPAddress SANs); connecting to the
# 127.0.0.1 literal then both matches the SAN and stays on IPv4 (localhost may
# resolve to ::1 first, where nothing is listening).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() {
  [ -n "$srv" ] && kill "$srv" 2>/dev/null || true
  cd "$root"          # Windows cannot remove the shell's own cwd
  navi_rmtree "$work"
}
trap cleanup EXIT
cd "$work"

port=9457

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout ca.key -out ca.pem -subj "$(navi_subj CN=navi-test-CA)" >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "$(navi_subj CN=127.0.0.1)" >/dev/null 2>&1
# A real file rather than <(...): the process substitution's /dev/fd path is
# meaningless to the native openssl.exe used under Git Bash.
printf "subjectAltName=DNS:127.0.0.1,DNS:localhost,IP:127.0.0.1" > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile san.ext \
  -out server.pem >/dev/null 2>&1

# -www answers a 200 HTML page on each connection.
openssl s_server -accept 127.0.0.1:"$port" -cert server.pem -key server.key \
  -www -quiet >"$work/s_server.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

# Wait until the server accepts TLS and validates against our CA.
ready=""
for _ in $(seq 1 50); do
  if echo | openssl s_client -connect "127.0.0.1:$port" -CAfile ca.pem 2>/dev/null \
       | grep -q "Verify return code: 0"; then ready=1; break; fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "s_server did not become ready on :$port"; cat "$work/s_server.log"; exit 1; }

export NAVI_CAFILE_URL="https://127.0.0.1:$port"
export NAVI_CAFILE_CA="$(navi_path "$work/ca.pem")"

nim c -r --hints:off -d:ssl -o:"$work/chronos_cafile" "$root/tests/interop/chronos_cafile.nim"
