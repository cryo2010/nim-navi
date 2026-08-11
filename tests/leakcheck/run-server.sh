#!/usr/bin/env bash
# Sourced by run.sh and run-js.sh. Generates a self-signed cert and starts the
# multi-protocol server twice: plaintext HTTP/1.1 on :8080 and TLS (h1 + h2 ALPN)
# on :8443. Exports NAVI_LEAK_BASE / NAVI_LEAK_BASE_TLS / NAVI_LEAK_CERT and
# installs an EXIT trap to stop both.
set -euo pipefail

LEAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK:-/tmp/navileak}"
mkdir -p "$WORK"

export NAVI_LEAK_CERT="$WORK/cert.pem"
KEY="$WORK/key.pem"
export NAVI_LEAK_BASE="http://localhost:8080"
export NAVI_LEAK_BASE_TLS="https://localhost:8443"

gen_cert() {
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -keyout "$KEY" -out "$NAVI_LEAK_CERT" \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    >/dev/null 2>&1
}

SRV_PLAIN="" ; SRV_TLS=""
stop_servers() {
  [ -n "$SRV_PLAIN" ] && kill "$SRV_PLAIN" 2>/dev/null || true
  [ -n "$SRV_TLS" ] && kill "$SRV_TLS" 2>/dev/null || true
}
trap stop_servers EXIT

wait_ready() {  # host:port  scheme
  for _ in $(seq 1 60); do
    if curl -sk --max-time 2 "$2://$1/get" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "server $2://$1 did not come up" >&2; return 1
}

start_servers() {
  gen_cert
  # Bind both loopback families: chronos may resolve "localhost" to ::1 first,
  # while sync/async use 127.0.0.1. The URL stays "localhost" (a DNS name) so TLS
  # hostname verification matches the cert's DNS:localhost SAN on every backend.
  ( cd "$LEAK_DIR" && exec hypercorn server:app \
      --bind 127.0.0.1:8080 --bind "[::1]:8080" ) &
  SRV_PLAIN=$!
  ( cd "$LEAK_DIR" && exec hypercorn server:app \
      --bind 127.0.0.1:8443 --bind "[::1]:8443" \
      --certfile "$NAVI_LEAK_CERT" --keyfile "$KEY" ) &
  SRV_TLS=$!
  wait_ready "localhost:8080" http
  wait_ready "localhost:8443" https
}
