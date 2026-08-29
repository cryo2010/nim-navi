#!/usr/bin/env bash
# In-memory CA bundle + SPKI pinning + custom verify callback for the sync
# (OpenSSL) backend. Generate a private CA, sign a server cert with it, start an
# OpenSSL HTTPS server, compute the server's SPKI pin, and check navi honors an
# in-memory caBundle, a matching/non-matching pin, and the verify callback. Tears
# everything down on exit.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() {
  [ -n "$srv" ] && kill "$srv" 2>/dev/null || true
  cd "$root"
  navi_rmtree "$work"
}
trap cleanup EXIT
cd "$work"

port=9459

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout ca.key -out ca.pem -subj "$(navi_subj CN=navi-test-CA)" >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "$(navi_subj CN=127.0.0.1)" >/dev/null 2>&1
printf "subjectAltName=DNS:127.0.0.1,DNS:localhost,IP:127.0.0.1" > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile san.ext -out server.pem >/dev/null 2>&1

# The server's SPKI pin: base64(SHA-256(DER SubjectPublicKeyInfo)) -- exactly what
# navi's peerSpkiPin computes via i2d_PUBKEY.
pin="$(openssl x509 -in server.pem -pubkey -noout \
  | openssl pkey -pubin -outform der 2>/dev/null \
  | openssl dgst -sha256 -binary | openssl base64)"

openssl s_server -accept 127.0.0.1:"$port" -cert server.pem -key server.key \
  -www -quiet >"$work/s_server.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

ready=""
for _ in $(seq 1 50); do
  if echo | openssl s_client -connect "127.0.0.1:$port" -CAfile ca.pem 2>/dev/null \
       | grep -q "Verify return code: 0"; then ready=1; break; fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "s_server did not become ready on :$port"; cat "$work/s_server.log"; exit 1; }

export NAVI_TLS_URL="https://127.0.0.1:$port"
export NAVI_TLS_CA="$(navi_path "$work/ca.pem")"
export NAVI_TLS_PIN="$pin"

echo "== TLS caBundle + SPKI pin + verify callback on 127.0.0.1:$port (pin=$pin) =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/tls_pin" "$root/tests/interop/tls_pin.nim"
