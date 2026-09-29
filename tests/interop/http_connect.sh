#!/usr/bin/env bash
# HTTP CONNECT proxy tunnelling for the three native clients. Starts a TLS origin
# and three CONNECT proxies that reply in the shapes a one-recv reader gets wrong:
# a 200 split across two TCP segments, a 200 padded past a single 1 KiB read, and
# a 407 refusal. navi must tunnel through the first two and report the proxy
# status line (not a TLS error) for the third. Tears everything down on exit.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

work="$(mktemp -d)"
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  cd "$root"
  navi_rmtree "$work"
}
trap cleanup EXIT

origin=9463
split=9464
big=9465
deny=9467

navi_certgen "$work/key.pem" "$work/cert.pem" 127.0.0.1 "DNS:localhost,IP:127.0.0.1"

# The TLS origin behind the tunnel: -www answers 200 on every path.
openssl s_server -accept 127.0.0.1:"$origin" -cert "$work/cert.pem" \
  -key "$work/key.pem" -www -quiet >"$work/s_server.log" 2>&1 &
pids+=($!)
disown 2>/dev/null || true

MODE=split python3 "$root/tests/interop/http_connect_proxy.py" "$split" >"$work/split.log" 2>&1 &
pids+=($!)
MODE=big python3 "$root/tests/interop/http_connect_proxy.py" "$big" >"$work/big.log" 2>&1 &
pids+=($!)
MODE=deny python3 "$root/tests/interop/http_connect_proxy.py" "$deny" >"$work/deny.log" 2>&1 &
pids+=($!)

navi_wait_tls "127.0.0.1:$origin" || {
  echo "s_server did not become ready on :$origin"; cat "$work/s_server.log"; exit 1; }
for pr in "$split" "$big" "$deny"; do
  ready=""
  for _ in $(seq 1 50); do
    if python3 -c "import socket,sys; socket.create_connection(('127.0.0.1',$pr),0.3).close()" 2>/dev/null; then
      ready=1; break
    fi
    sleep 0.1
  done
  [ -n "$ready" ] || { echo "proxy on :$pr did not start"; exit 1; }
done

export NAVI_CONNECT_TARGET="https://127.0.0.1:$origin/"
export NAVI_CONNECT_CA="$(navi_path "$work/cert.pem")"
export NAVI_CONNECT_SPLIT="http://127.0.0.1:$split"
export NAVI_CONNECT_BIG="http://127.0.0.1:$big"
export NAVI_CONNECT_DENY="http://127.0.0.1:$deny"

echo "== CONNECT: origin :$origin, split-reply :$split, oversized-reply :$big, 407 :$deny =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/http_connect" "$root/tests/interop/http_connect.nim"
# The async backends supply their own read primitive to the shared CONNECT
# driver (asyncdispatch recv, chronos readOnce); exercise both loops.
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/http_connect_ad" "$root/tests/interop/http_connect_async.nim"
nim c -r --hints:off -d:ssl -d:useChronos --path:"$root/src" -o:"$work/http_connect_ch" "$root/tests/interop/http_connect_async.nim"
