#!/bin/sh
# WebSocket-over-HTTP/2 Extended CONNECT (RFC 8441) interop: a Node h2 TLS server
# that advertises SETTINGS_ENABLE_CONNECT_PROTOCOL and echoes WebSocket frames,
# driven by the navi async and chronos clients with config.http = {H2}. Needs
# `node`, `openssl`, and a Nim toolchain (chronos installed); run it in
# Linux/Docker (navi's TLS can't dlopen libcrypto on a bare macOS host). Example:
# nimlang/nim image + `apt-get install nodejs openssl` + `nimble install chronos`.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$here/../../.." && pwd)
port=${WS_PORT:-8443}
tmp=$(mktemp -d)
trap 'kill "${srv:-0}" 2>/dev/null || true; rm -rf "$tmp"' EXIT

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$tmp/key.pem" -out "$tmp/cert.pem" \
  -days 1 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1" >/dev/null 2>&1

( cd "$tmp" && cp "$here/server.js" . && WS_PORT="$port" node server.js ) & srv=$!
i=0; until [ "$i" -gt 100 ]; do i=$((i+1)); sleep 0.1; done

for backend in client client_chronos; do
  nim c --hints:off --threads:on -d:ssl --path:"$root/src" -o:"$tmp/$backend" "$here/$backend.nim"
  echo "[$backend]"; WS_PORT="$port" "$tmp/$backend"
done
