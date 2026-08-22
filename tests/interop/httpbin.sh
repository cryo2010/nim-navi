#!/usr/bin/env bash
# httpbin functionality interop: start a local httpbin behind Caddy (TLS + h2)
# and run navi's client against the full breadth of httpbin's endpoints. Fully
# offline and reproducible -- the container is never published to the host, so
# this can never hit the public httpbin.org.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
here="$root/tests/interop/httpbin"
compose="docker compose -f $here/docker-compose.yml"

command -v docker >/dev/null || { echo "docker required"; exit 127; }
command -v openssl >/dev/null || { echo "openssl required"; exit 127; }

work="$(mktemp -d)"
export NAVI_CERTDIR="$work/certs"
mkdir -p "$NAVI_CERTDIR"

cleanup() { $compose down -v >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$NAVI_CERTDIR/key.pem" -out "$NAVI_CERTDIR/cert.pem" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1" >/dev/null 2>&1
# openssl writes the key 0600; over a Linux bind mount that keeps host perms Caddy
# could not read it. Throwaway 1-day self-signed key, so make it world-readable.
chmod 644 "$NAVI_CERTDIR"/*.pem

$compose up -d

# Ready only once a real request round-trips through Caddy to httpbin (a 200 also
# proves the backend is up, not just that Caddy is listening).
url="https://127.0.0.1:8447"
ready=""
for _ in $(seq 1 60); do
  if [ "$(curl -sk -o /dev/null -w '%{http_code}' "$url/get" 2>/dev/null || true)" = "200" ]; then
    ready=1; break
  fi
  sleep 0.5
done
[ -n "$ready" ] || { echo "httpbin (via Caddy) not ready"; $compose logs; exit 1; }

export NAVI_INTEROP_CERT="$NAVI_CERTDIR/cert.pem"
export NAVI_HTTPBIN_URL="$url"
common="--path:$root/src -d:ssl --hints:off"
test="$root/tests/interop/httpbin_test.nim"
echo "== httpbin functionality: methods, bodies, auth, redirects, decompression, cookies, streaming =="

# Native backends: one shared test compiled three ways (all negotiate h2 with
# Caddy now that chronos runs OpenSSL and ALPN too).
nim c -r $common               "$test"
nim c -r $common -d:useAsync   "$test"
nim c -r $common -d:useChronos "$test"

# js backend under Node: fetch ignores cfg.tls.caFile, so Node trusts the cert via
# NODE_EXTRA_CA_CERTS. Skipped (not failed) when node is unavailable.
if command -v node >/dev/null; then
  nim js -d:nodejs --path:"$root/src" --hints:off -o:"$work/httpbin_js.js" \
    "$root/tests/interop/httpbin_js.nim"
  NODE_EXTRA_CA_CERTS="$NAVI_INTEROP_CERT" node "$work/httpbin_js.js"
else
  echo "node not found; skipping the js backend"
fi

echo "== httpbin interop passed (all backends) =="
