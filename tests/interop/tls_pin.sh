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

navi_certgen ca.key ca.pem navi-test-CA

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

# A session cached during the handshake of a peer navi then REJECTED (SPKI pin or
# verify callback) must not be re-offered to the origin on the next connect
# (#440). Same server, same CA and the same real pin; the test drives two
# sequential requests through one client and reads the session state out of the
# page `s_server -www` serves. Run on all three native backends and pinned to
# both TLS 1.2 (new-session callback inside the handshake) and TLS 1.3 (ticket
# after it).
export NAVI_REJ_URL="$NAVI_TLS_URL"
export NAVI_REJ_CA="$NAVI_TLS_CA"
export NAVI_REJ_PIN="$pin"

echo "== a rejected peer's TLS session is evicted, not re-offered (sync/asyncdispatch/chronos) =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/reject_sync" \
  "$root/tests/interop/tls_reject_resume.nim"
nim c -r --hints:off -d:ssl -d:naviAsync --path:"$root/src" -o:"$work/reject_async" \
  "$root/tests/interop/tls_reject_resume.nim"
if nimble path chronos >/dev/null 2>&1; then
  nim c -r --hints:off -d:ssl -d:naviChronos --path:"$root/src" -o:"$work/reject_chronos" \
    "$root/tests/interop/tls_reject_resume.nim"
else
  echo "note: chronos not installed; skipping the chronos rejection leg"
fi
