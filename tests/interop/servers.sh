#!/usr/bin/env bash
# Multi-server HTTP/2 interop: start nginx, Caddy, and h2o (three independent h2
# stacks) over TLS via docker compose, then run navi's client (sync,
# asyncdispatch, chronos) against each. Complements the nghttpd suite in run.sh.
# Reproducible and offline (all in containers), so it can gate PRs.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
here="$root/tests/interop/servers"
compose="docker compose -f $here/docker-compose.yml"

command -v docker >/dev/null || { echo "docker required"; exit 127; }
command -v openssl >/dev/null || { echo "openssl required"; exit 127; }

work="$(mktemp -d)"
export NAVI_CERTDIR="$work/certs" NAVI_HTDOCS="$work/htdocs"
mkdir -p "$NAVI_CERTDIR" "$NAVI_HTDOCS"

cleanup() { $compose down -v >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT

# Self-signed cert. DNS:127.0.0.1 (not just the IP SAN) so chronos's BearSSL,
# which matches the connect host against dNSName SANs, accepts the loopback IP.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$NAVI_CERTDIR/key.pem" -out "$NAVI_CERTDIR/cert.pem" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1" >/dev/null 2>&1
printf 'navi interop\n' > "$NAVI_HTDOCS/hello.txt"
head -c 262144 /dev/urandom > "$NAVI_HTDOCS/large.bin"

# name=url pairs (127.0.0.1, not localhost, to avoid IPv6/IPv4 ambiguity).
servers="nginx=https://127.0.0.1:8443 caddy=https://127.0.0.1:8445 h2o=https://127.0.0.1:8444"

$compose up -d

# Wait for each server to accept TLS and negotiate h2 over ALPN.
for kv in $servers; do
  port="${kv##*:}"
  ready=""
  for _ in $(seq 1 60); do
    if echo | openssl s_client -alpn h2 -connect "127.0.0.1:$port" 2>/dev/null \
         | grep -q "ALPN protocol: h2"; then ready=1; break; fi
    sleep 0.5
  done
  [ -n "$ready" ] || { echo "server on :$port not ready"; $compose logs; exit 1; }
done

export NAVI_INTEROP_CERT="$NAVI_CERTDIR/cert.pem"
export NAVI_SERVERS="$servers"
common="--path:$root/src -d:ssl --hints:off"
echo "== multi-server interop: nginx, Caddy, h2o =="
nim c -r $common               "$root/tests/interop/servers_sync.nim"
nim c -r $common               "$root/tests/interop/servers_async.nim"
nim c -r $common -d:useChronos "$root/tests/interop/servers_async.nim"
echo "== multi-server interop passed =="
