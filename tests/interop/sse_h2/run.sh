#!/bin/sh
# SSE-over-HTTP/2 connection-sharing interop (#466): a Node h2 TLS origin that
# counts its h2 sessions, driven by navi's sync, asyncdispatch and chronos clients, so
# the claims about which connection a stream rides, and about the per-stream response
# size cap, can be checked against a real server.
# Needs `node`, `openssl`, and a Nim toolchain (chronos installed); run it in
# Linux/Docker (navi's TLS can't dlopen libcrypto on a bare macOS host). Example:
# nimlang/nim image + `apt-get install nodejs openssl` + `nimble install chronos`.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$here/../../.." && pwd)
port=${SSE_PORT:-8453}
gap=${SSE_GAP_MS:-3000}
tmp=$(mktemp -d)
trap 'if [ -n "${srv:-}" ]; then kill "$srv" 2>/dev/null || true; fi; rm -rf "$tmp"' EXIT

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$tmp/key.pem" -out "$tmp/cert.pem" \
  -days 1 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1" >/dev/null 2>&1

# `exec` so $srv is node's own pid, not the subshell's: killing the subshell would
# leave node holding the port and the next run would fail to bind it.
( cd "$tmp" && cp "$here/server.js" . \
  && exec env SSE_PORT="$port" SSE_GAP_MS="$gap" node server.js ) & srv=$!

# Wait for the origin to accept a TCP connection, rather than sleeping a fixed span:
# a loaded box can take longer than the sleep did, and then the clients fail on a
# connection refused that looks like a navi bug. node is already a dependency here.
probe='const s=require("net").connect(Number(process.argv[1]),"127.0.0.1");
s.on("connect",()=>{s.destroy();process.exit(0)});s.on("error",()=>process.exit(1));'
i=0
while :; do
  if node -e "$probe" "$port" 2>/dev/null; then break; fi
  i=$((i+1))
  if [ "$i" -gt 150 ]; then echo "no h2 origin on 127.0.0.1:$port" >&2; exit 1; fi
  sleep 0.1
done

for backend in client_sync client client_chronos; do
  nim c --hints:off --threads:on -d:ssl --path:"$root/src" -o:"$tmp/$backend" "$here/$backend.nim"
  echo "[$backend]"; SSE_PORT="$port" "$tmp/$backend"
done
