#!/bin/sh
# WebSocket-over-HTTP/3 Extended CONNECT (RFC 9220) interop, in two halves:
#
#   1. an aioquic h3 server that advertises SETTINGS_ENABLE_CONNECT_PROTOCOL and
#      echoes WebSocket frames -- the normal path, driven by the navi async, chronos
#      and sync clients with config.http = {H3};
#   2. a second aioquic server on the next port whose SETTINGS does NOT enable the
#      Extended CONNECT protocol -- navi must refuse to open the tunnel, fast and
#      with a ProtocolError naming the missing setting (issue #393), on all three.
#
# Needs the -d:naviHttp3 toolchain (OpenSSL 3.5 + ngtcp2 + nghttp3), python3 +
# aioquic, and openssl; run it in the h3 Docker image (tests/stress/Dockerfile.h3).
# Example:
#   docker run --rm --entrypoint bash -v "$PWD":/src:ro -w /work navi-stress-h3 \
#     -c 'cp -r /src/* /work/; pip3 install --break-system-packages aioquic; \
#         bash tests/interop/ws_h3/run.sh'
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$here/../../.." && pwd)
port=${WS_PORT:-4433}
noconnect_port=$((port + 1))
tmp=$(mktemp -d)
trap 'kill "${srv:-0}" "${srv_nc:-0}" 2>/dev/null || true; rm -rf "$tmp"' EXIT

env -u LD_LIBRARY_PATH openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$tmp/key.pem" -out "$tmp/cert.pem" -days 1 -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1" >/dev/null 2>&1

wait_ready() {   # $1 = server log
  i=0
  until grep -q WS_H3_SERVER_READY "$1" 2>/dev/null; do
    i=$((i+1)); [ "$i" -gt 200 ] && { echo "server did not start"; cat "$1"; exit 1; }
    sleep 0.1
  done
}

WS_PORT="$port" WS_CERT="$tmp/cert.pem" WS_KEY="$tmp/key.pem" \
  python3 "$here/server.py" >"$tmp/srv.out" 2>&1 & srv=$!
wait_ready "$tmp/srv.out"

# The same server with SETTINGS_ENABLE_CONNECT_PROTOCOL=0 (it still answers a
# CONNECT with 200, so a client that skips the gate is caught rather than masked).
WS_PORT="$noconnect_port" WS_ENABLE_CONNECT=0 WS_CERT="$tmp/cert.pem" \
  WS_KEY="$tmp/key.pem" python3 "$here/server.py" >"$tmp/srv_nc.out" 2>&1 & srv_nc=$!
wait_ready "$tmp/srv_nc.out"

for backend in client client_chronos client_sync; do
  nim c --hints:off --threads:on -d:ssl -d:naviHttp3 --path:"$root/src" \
    -o:"$tmp/$backend" "$here/$backend.nim"
  echo "[$backend]"; WS_PORT="$port" "$tmp/$backend"
done

for backend in client_noconnect client_noconnect_chronos client_noconnect_sync; do
  nim c --hints:off --threads:on -d:ssl -d:naviHttp3 --path:"$root/src" \
    -o:"$tmp/$backend" "$here/$backend.nim"
  echo "[$backend]"; WS_PORT="$noconnect_port" "$tmp/$backend"
done
