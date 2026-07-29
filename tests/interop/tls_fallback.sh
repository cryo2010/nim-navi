#!/usr/bin/env bash
# Handshake-aware address fallback (sync backend): stand up a good TLS server on
# 127.0.0.1 and a dead endpoint (accepts TCP, then drops it so the TLS handshake
# fails) on a second loopback address, both on the same port, and check navi
# falls through the dead address to the good one.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v openssl >/dev/null || { echo "openssl required"; exit 127; }
command -v python3 >/dev/null || { echo "python3 required"; exit 127; }

work="$(mktemp -d)"
good=""; bad=""
cleanup() {
  [ -n "$good" ] && kill "$good" 2>/dev/null || true
  [ -n "$bad" ]  && kill "$bad"  2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

port=9466
# A second loopback address for the dead endpoint: 127.0.0.2 exists out of the
# box on Linux; on macOS only 127.0.0.1 is configured, so fall back to ::1.
badip="127.0.0.2"
python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.2",0)); s.close()' 2>/dev/null || badip="::1"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$work/key.pem" -out "$work/cert.pem" -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=DNS:127.0.0.1,IP:127.0.0.1" >/dev/null 2>&1

# Good TLS server on 127.0.0.1: completes the handshake and answers 200.
python3 - "$work/cert.pem" "$work/key.pem" "$port" >/dev/null 2>&1 <<'PY' &
import ssl, socket, sys
cert, key, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain(cert, key)
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", port)); s.listen()
while True:
    c, _ = s.accept()
    try:
        c = ctx.wrap_socket(c, server_side=True)
        c.recv(4096)
        c.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi")
    except Exception:
        pass
    finally:
        try: c.close()
        except Exception: pass
PY
good=$!

# Dead endpoint on the second address: accept the TCP connection, then close it
# so the TLS handshake fails.
python3 - "$badip" "$port" >/dev/null 2>&1 <<'PY' &
import socket, sys
ip, port = sys.argv[1], int(sys.argv[2])
fam = socket.AF_INET6 if ":" in ip else socket.AF_INET
s = socket.socket(fam); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((ip, port)); s.listen()
while True:
    c, _ = s.accept(); c.close()
PY
bad=$!

# Wait for the good server to accept TLS.
ready=""
for _ in $(seq 1 60); do
  if echo | openssl s_client -connect "127.0.0.1:$port" 2>/dev/null | grep -q "BEGIN CERTIFICATE"; then
    ready=1; break
  fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "good TLS server not ready on 127.0.0.1:$port"; exit 1; }

export NAVI_FB_CERT="$work/cert.pem" NAVI_FB_PORT="$port" NAVI_FB_BADIP="$badip"
echo "== TLS handshake-aware address fallback (sync); dead endpoint = $badip =="
nim c -r --path:"$root/src" -d:ssl --hints:off "$root/tests/interop/tls_fallback.nim"
