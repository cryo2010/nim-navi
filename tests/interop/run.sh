#!/usr/bin/env bash
# Start nghttpd (nghttp2 reference server) over TLS+h2 with a small static site,
# run navi's interop tests against it, and tear everything down. A second nghttpd
# runs with frame padding (-b) so the tests exercise navi's PADDED-flag handling
# against a real reference peer.
#
#   nghttpd from: apt `nghttp2-server` (CI) or brew `nghttp2` (local).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
port="${NGHTTPD_PORT:-18443}"
padded_port="${NGHTTPD_PADDED_PORT:-18444}"

command -v nghttpd >/dev/null || {
  echo "nghttpd not found; install nghttp2-server (apt) or nghttp2 (brew)"; exit 127; }
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

work="$(mktemp -d)"
srv=""; srv_padded=""
cleanup() {
  [ -n "$srv" ] && kill "$srv" 2>/dev/null || true
  [ -n "$srv_padded" ] && kill "$srv_padded" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

# Wait until nghttpd accepts TLS and negotiates h2 over ALPN on the given port.
wait_ready() {
  local p="$1"
  for _ in $(seq 1 50); do
    if echo | openssl s_client -alpn h2 -connect "localhost:$p" 2>/dev/null \
         | grep -q "ALPN protocol: h2"; then return 0; fi
    sleep 0.2
  done
  echo "nghttpd did not become ready on :$p"; cat "$work"/nghttpd*.log; exit 1
}

# self-signed cert for localhost (also exercises navi's caFile verification)
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$work/key.pem" -out "$work/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" >/dev/null 2>&1

mkdir -p "$work/htdocs"
printf 'hello from nghttpd\n' > "$work/htdocs/small.txt"
head -c 262144 /dev/urandom > "$work/htdocs/large.bin"

# -m 2: a deliberately low MAX_CONCURRENT_STREAMS so a parallel batch larger
# than 2 must be queued by the client (streams beyond the cap would otherwise be
# refused/reset). Exercises the sync `parallel` and async mux stream queuing.
nghttpd -m 2 -d "$work/htdocs" --echo-upload "$port" "$work/key.pem" "$work/cert.pem" \
  >"$work/nghttpd.log" 2>&1 &
srv=$!

# -b 255: pad HEADERS and DATA frames with up to 255 bytes, so navi must strip
# the PADDED pad-length byte and trailing padding (RFC 9113 6.1/6.2). Without
# that, the stray bytes corrupt HPACK / the response body.
# --trailer: send a trailing HEADERS block after DATA; navi must not surface it
# as a response header (RFC 9113 8.1). Exercises the same edge server both ways.
nghttpd -b 255 --trailer 'x-navi-trailer: seen' \
  -d "$work/htdocs" --echo-upload "$padded_port" "$work/key.pem" "$work/cert.pem" \
  >"$work/nghttpd-padded.log" 2>&1 &
srv_padded=$!
disown -a 2>/dev/null || true   # don't let the shell print a "Terminated" job notice
                                # when the cleanup trap kills the servers on exit

wait_ready "$port"
wait_ready "$padded_port"

export NAVI_INTEROP_URL="https://localhost:$port"
export NAVI_INTEROP_PADDED_URL="https://localhost:$padded_port"
export NAVI_INTEROP_CERT="$work/cert.pem"

nim c -r --hints:off -o:"$work/sync"  "$root/tests/interop/nghttpd_sync.nim"
nim c -r --hints:off -o:"$work/async" "$root/tests/interop/nghttpd_async.nim"
