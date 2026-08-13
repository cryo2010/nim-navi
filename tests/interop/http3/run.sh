#!/usr/bin/env bash
# Entrypoint for the tests/interop/http3 image. Starts Caddy as a local h3 origin
# and runs navi's HTTP/3 GET test (backend/quic -> h3client.c: ngtcp2 + nghttp3 +
# OpenSSL 3.5 QUIC), asserting a real h3 response from the origin.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
WORK=/work
mkdir -p "$WORK"

# Self-signed cert for Caddy (SAN localhost/127.0.0.1). OPENSSL_CONF=/dev/null
# because `make install_sw` does not install openssl.cnf (none is needed here).
OPENSSL_CONF=/dev/null "$OSSL/bin/openssl" req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  >/dev/null 2>&1

# Start the h3 origin (h3 on UDP 4433). Fatal if it fails: the test dials it.
caddy start --config "$DIR/Caddyfile" --adapter caddyfile >/tmp/caddy.log 2>&1 \
  || { echo "caddy failed to start"; cat /tmp/caddy.log; exit 1; }
echo "caddy: h3 origin up on udp/4433"
sleep 1

echo ">>> building and running the navi HTTP/3 GET test"
export NAVI_H3_CA="$WORK/cert.pem"   # the origin's CA, for the verified GET case
nim c --hints:off --path:"$ROOT/src" -d:naviHttp3 -o:/tmp/h3get_test "$DIR/h3get_test.nim"
/tmp/h3get_test
