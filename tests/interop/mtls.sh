#!/usr/bin/env bash
# Mutual-TLS interop: generate a CA plus server and client certificates, start an
# OpenSSL server that requires a client certificate, and run navi's mTLS test
# against it. Tears everything down on exit.
#
#   openssl s_server -Verify 1 rejects any client that does not present a cert
#   signed by the CA, so this exercises navi's TlsConfig.certFile/keyFile.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT
cd "$work"

port=9455

# A CA that signs both the server and the client certificate.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout ca.key -out ca.pem -subj "/CN=navi-test-CA" >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "/CN=localhost" >/dev/null 2>&1
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") \
  -out server.pem >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
  -subj "/CN=navi-client" >/dev/null 2>&1
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -out client.pem >/dev/null 2>&1

# The same client credential re-encoded into every format navi accepts, so one
# server can validate them all: an encrypted PEM key, a PKCS#12 bundle, and a
# DER cert + key. (The PEM and in-memory paths reuse client.pem/client.key.)
pass="navi-secret"
openssl rsa -in client.key -out client.enc.key -aes256 -passout "pass:$pass" >/dev/null 2>&1
openssl pkcs12 -export -inkey client.key -in client.pem -certfile ca.pem \
  -passout "pass:$pass" -out client.p12 >/dev/null 2>&1
openssl x509 -in client.pem -outform DER -out client.der.crt >/dev/null 2>&1
openssl rsa -in client.key -outform DER -out client.der.key >/dev/null 2>&1

# -Verify 1 makes a client certificate mandatory; -www answers a 200 HTML page.
openssl s_server -accept "$port" -cert server.pem -key server.key \
  -CAfile ca.pem -Verify 1 -www -quiet >"$work/s_server.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

# Wait until the server accepts TLS.
ready=""
for _ in $(seq 1 50); do
  if echo | openssl s_client -connect "localhost:$port" \
       -cert client.pem -key client.key -CAfile ca.pem 2>/dev/null \
       | grep -q "Verify return code: 0"; then ready=1; break; fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "s_server did not become ready on :$port"; cat "$work/s_server.log"; exit 1; }

export NAVI_MTLS_URL="https://localhost:$port"
export NAVI_MTLS_CA="$work/ca.pem"
export NAVI_MTLS_CERT="$work/client.pem"
export NAVI_MTLS_KEY="$work/client.key"
export NAVI_MTLS_ENCKEY="$work/client.enc.key"
export NAVI_MTLS_P12="$work/client.p12"
export NAVI_MTLS_DERCERT="$work/client.der.crt"
export NAVI_MTLS_DERKEY="$work/client.der.key"
export NAVI_MTLS_PASS="$pass"

# Same test on every native backend (js does not present client certs). chronos
# now runs OpenSSL, so it presents client certificates like sync/asyncdispatch.
nim c -r --hints:off -d:ssl -o:"$work/mtls_sync"  "$root/tests/interop/mtls.nim"
nim c -r --hints:off -d:ssl -d:useAsync -o:"$work/mtls_async" "$root/tests/interop/mtls.nim"
if nimble path chronos >/dev/null 2>&1; then
  nim c -r --hints:off -d:ssl -d:useChronos -o:"$work/mtls_chronos" \
    "$root/tests/interop/mtls.nim"
else
  echo "note: chronos not installed; skipping the chronos mTLS leg"
fi
