#!/usr/bin/env bash
# File-streaming interop for one (protocol, direction) combination.
#   streaming.sh <http1|http2> <upload|download>
# Stands up a server that speaks the requested protocol -- nghttpd for http/2, a
# local HTTP/1.1 server (streaming_server.nim) for http/1.1 -- then runs the sync
# client, which asserts the transfer used that protocol and hash-matches the
# original file. One shared random payload backs both directions.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
proto="${1:?usage: streaming.sh <http1|http2> <upload|download>}"
dir="${2:?usage: streaming.sh <http1|http2> <upload|download>}"
command -v openssl >/dev/null || { echo "openssl required"; exit 127; }

work="$(mktemp -d)"; srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

head -c 3145728 /dev/urandom > "$work/payload.bin"   # 3 MiB, > the h2 flow-control window

if [ "$proto" = "http2" ]; then
  command -v nghttpd >/dev/null || { echo "nghttpd required (nghttp2-server)"; exit 127; }
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -keyout "$work/key.pem" -out "$work/cert.pem" -subj "/CN=127.0.0.1" \
    -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" >/dev/null 2>&1
  mkdir -p "$work/htdocs"; cp "$work/payload.bin" "$work/htdocs/download"
  port=9443
  nghttpd -m 2 -d "$work/htdocs" --echo-upload "$port" \
    "$work/key.pem" "$work/cert.pem" >"$work/srv.log" 2>&1 &
  srv=$!
  url="https://127.0.0.1:$port"; cert="$work/cert.pem"; ver="HTTP/2"
  ready=""
  for _ in $(seq 1 60); do
    echo | openssl s_client -alpn h2 -connect "127.0.0.1:$port" 2>/dev/null \
      | grep -q "ALPN protocol: h2" && { ready=1; break; }
    sleep 0.2
  done
else
  port=9444
  # Compile first, then run the binary in the background -- so $srv is the server
  # process itself (killable on cleanup), not a `nim c -r` wrapper that would
  # orphan the running server and leave the port bound for the next run.
  nim c --path:"$root/src" --hints:off -o:"$work/h1srv" \
    "$root/tests/interop/streaming_server.nim" >"$work/srv.log" 2>&1
  NAVI_STREAM_FILE="$work/payload.bin" NAVI_STREAM_PORT="$port" \
    "$work/h1srv" >>"$work/srv.log" 2>&1 &
  srv=$!
  url="http://127.0.0.1:$port"; cert=""; ver="HTTP/1.1"
  ready=""
  for _ in $(seq 1 120); do
    curl -fsS "$url/download" -o /dev/null 2>/dev/null && { ready=1; break; }
    sleep 0.25
  done
fi
[ -n "$ready" ] || { echo "server not ready ($proto)"; cat "$work/srv.log"; exit 1; }

export NAVI_STREAM_URL="$url" NAVI_STREAM_DIR="$dir" NAVI_STREAM_PROTO="$ver" \
       NAVI_STREAM_CERT="$cert" NAVI_STREAM_FILE="$work/payload.bin"
echo "== file streaming: $proto $dir over $ver =="
nim c -r --path:"$root/src" -d:ssl --hints:off "$root/tests/interop/streaming_client.nim"
