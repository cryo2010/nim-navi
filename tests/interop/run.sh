#!/usr/bin/env bash
# Start nghttpd (nghttp2 reference server) over TLS+h2 with a small static site,
# run navi's interop tests against it, and tear everything down.
#
#   nghttpd from: apt `nghttp2-server` (CI) or brew `nghttp2` (local).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
port="${NGHTTPD_PORT:-18443}"

command -v nghttpd >/dev/null || {
  echo "nghttpd not found; install nghttp2-server (apt) or nghttp2 (brew)"; exit 127; }
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

# self-signed cert for localhost (also exercises navi's caFile verification)
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$work/key.pem" -out "$work/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" >/dev/null 2>&1

mkdir -p "$work/htdocs"
printf 'hello from nghttpd\n' > "$work/htdocs/small.txt"
head -c 262144 /dev/urandom > "$work/htdocs/large.bin"

nghttpd -d "$work/htdocs" --echo-upload "$port" "$work/key.pem" "$work/cert.pem" \
  >"$work/nghttpd.log" 2>&1 &
srv=$!
disown 2>/dev/null || true   # don't let the shell print a "Terminated" job notice
                             # when the cleanup trap kills the server on exit

# wait until nghttpd accepts TLS and negotiates h2 over ALPN
ready=""
for _ in $(seq 1 50); do
  if echo | openssl s_client -alpn h2 -connect "localhost:$port" 2>/dev/null \
       | grep -q "ALPN protocol: h2"; then ready=1; break; fi
  sleep 0.2
done
[ -n "$ready" ] || {
  echo "nghttpd did not become ready on :$port"; cat "$work/nghttpd.log"; exit 1; }

export NAVI_INTEROP_URL="https://localhost:$port"
export NAVI_INTEROP_CERT="$work/cert.pem"

nim c -r --hints:off -o:"$work/sync"  "$root/tests/interop/nghttpd_sync.nim"
nim c -r --hints:off -o:"$work/async" "$root/tests/interop/nghttpd_async.nim"
