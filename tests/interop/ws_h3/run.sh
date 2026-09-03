#!/bin/sh
# WebSocket-over-HTTP/3 Extended CONNECT (RFC 9220) interop: an aioquic h3 server
# that advertises SETTINGS_ENABLE_CONNECT_PROTOCOL and echoes WebSocket frames,
# driven by the navi async and chronos clients with config.http = {H3}. Needs the
# -d:naviHttp3 toolchain (OpenSSL 3.5 + ngtcp2 + nghttp3), python3 + aioquic, and
# openssl; run it in the h3 Docker image (tests/stress/Dockerfile.h3). Example:
#   docker run --rm --entrypoint bash -v "$PWD":/src:ro -w /work navi-stress-h3 \
#     -c 'cp -r /src/* /work/; pip3 install --break-system-packages aioquic; \
#         bash tests/interop/ws_h3/run.sh'
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$here/../../.." && pwd)
port=${WS_PORT:-4433}
tmp=$(mktemp -d)
trap 'kill "${srv:-0}" 2>/dev/null || true; rm -rf "$tmp"' EXIT

env -u LD_LIBRARY_PATH openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$tmp/key.pem" -out "$tmp/cert.pem" -days 1 -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1" >/dev/null 2>&1

WS_PORT="$port" WS_CERT="$tmp/cert.pem" WS_KEY="$tmp/key.pem" \
  python3 "$here/server.py" >"$tmp/srv.out" 2>&1 & srv=$!
i=0; until grep -q WS_H3_SERVER_READY "$tmp/srv.out" 2>/dev/null; do
  i=$((i+1)); [ "$i" -gt 200 ] && { echo "server did not start"; cat "$tmp/srv.out"; exit 1; }
  sleep 0.1
done

for backend in client client_chronos; do
  nim c --hints:off --threads:on -d:ssl -d:naviHttp3 --path:"$root/src" \
    -o:"$tmp/$backend" "$here/$backend.nim"
  echo "[$backend]"; WS_PORT="$port" "$tmp/$backend"
done
