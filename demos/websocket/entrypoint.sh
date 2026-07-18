#!/usr/bin/env bash
# Start the echo server, run each native client once (printing its round trip),
# then serve the navi/js page so a browser can run the same round trip.
set -euo pipefail

# Bind all interfaces so the published port reaches the browser on the host
# (Docker forwards to the container's external interface, not loopback).
export NAVI_WS_HOST=0.0.0.0
/app/echo_server >/app/server.log 2>&1 &   # quiet; clients print their own output
sleep 1

echo "======================================================================"
echo "  Native WebSocket clients (connect, send a message, receive the echo)"
echo "======================================================================"
for client in sync asyncdispatch chronos; do
  echo "--- $client ---"
  "/app/$client"
  echo
done

echo "======================================================================"
echo "  navi/js: open  http://localhost:8000/  in your browser"
echo "  The page runs navi compiled to JavaScript and echoes over WebSocket."
echo "  Press Ctrl-C to stop."
echo "======================================================================"
cd /app/web
exec python3 -m http.server 8000
