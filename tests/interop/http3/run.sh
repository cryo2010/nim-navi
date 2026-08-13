#!/usr/bin/env bash
# Entrypoint for the tests/interop/http3 image. Starts Caddy as a local h3 origin
# and runs the binding-layer probe. The probe verifies the ngtcp2/nghttp3/OpenSSL
# QUIC FFI links and initializes; the full handshake + h3 GET driver (phase 2b)
# will dial the Caddy origin this harness starts.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK=/work
mkdir -p "$WORK"

# Self-signed cert for Caddy (SAN localhost/127.0.0.1), matching the leakcheck
# server's approach so a client can verify against cert.pem. OPENSSL_CONF=/dev/null
# because `make install_sw` does not install openssl.cnf (none is needed here).
OPENSSL_CONF=/dev/null "$OSSL/bin/openssl" req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  >/dev/null 2>&1

# Start the h3 origin (h3 on UDP 4433). Non-fatal if it fails; the probe below is
# link/init only and does not dial yet.
if caddy start --config "$DIR/Caddyfile" --adapter caddyfile >/tmp/caddy.log 2>&1; then
  echo "caddy: h3 origin up on udp/4433"
else
  echo "caddy: failed to start (non-fatal for the linkage probe)"; cat /tmp/caddy.log || true
fi

echo ">>> building and running the h3 binding probe"
nim c --hints:off -o:/tmp/h3probe "$DIR/probe.nim"
/tmp/h3probe
