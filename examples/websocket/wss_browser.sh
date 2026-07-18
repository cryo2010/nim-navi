#!/usr/bin/env bash
# One command for the browser wss demo: generate a browser-trusted cert with
# mkcert, build the navi/js page and the wss echo server, and serve them. Then
# open http://localhost:8000/wss_index.html.
#
# Requires mkcert and python3. `mkcert -install` trusts a local CA in your
# browser -- a one-time step (it may prompt for your password the first time);
# it must run on the host, since your browser reads the host's trust store.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
here="$root/examples/websocket"
command -v mkcert >/dev/null || {
  echo "mkcert not found. Install it (e.g. 'brew install mkcert' / 'apt install mkcert') and re-run."
  exit 127; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

certdir="$here/.certs"
mkdir -p "$certdir"
mkcert -install
( cd "$certdir" && [ -f localhost+1.pem ] || mkcert localhost 127.0.0.1 )

echo "building the page (nim js) and the wss echo server ..."
nim js --path:"$root/src" -d:release --hints:off -o:"$here/navi_ws.js" "$here/js.nim"
nim c -d:ssl -d:release --hints:off -o:"$certdir/wss_echo_server" "$here/wss_echo_server.nim"

srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; }
trap cleanup EXIT

NAVI_WSS_CERT="$certdir/localhost+1.pem" NAVI_WSS_KEY="$certdir/localhost+1-key.pem" \
  "$certdir/wss_echo_server" >"$certdir/server.log" 2>&1 &
srv=$!
sleep 1

echo
echo "  Open  http://localhost:8000/wss_index.html  in your browser."
echo "  Type a message; it echoes back over wss with no cert warnings."
echo "  (Ctrl-C to stop.)"
echo
cd "$here"
python3 -m http.server 8000
