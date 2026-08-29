#!/usr/bin/env bash
# SOCKS5 proxy support on the sync backend: start a plain HTTP origin and two
# SOCKS5 proxies (one no-auth, one requiring username/password), then check navi
# tunnels through, authenticates, and rejects wrong credentials. Tears everything
# down on exit.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

work="$(mktemp -d)"
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  rm -rf "$work"
}
trap cleanup EXIT

origin=9460
noauth=9461
authp=9462

# A plain HTTP origin that answers 200 on every path. Use --directory (not a
# subshell) so $! is the python process itself and cleanup can reap it.
printf 'ok' > "$work/index.html"
python3 -m http.server "$origin" --bind 127.0.0.1 --directory "$work" >/dev/null 2>&1 &
pids+=($!)

# A no-auth SOCKS5 proxy, and a second one requiring user:pass.
python3 "$root/tests/interop/socks5_proxy.py" "$noauth" >"$work/noauth.log" 2>&1 &
pids+=($!)
SOCKS_USER=alice SOCKS_PASS=s3cret \
  python3 "$root/tests/interop/socks5_proxy.py" "$authp" >"$work/auth.log" 2>&1 &
pids+=($!)

# Wait for the origin and both proxies to be listening.
for pr in "$origin" "$noauth" "$authp"; do
  ready=""
  for _ in $(seq 1 50); do
    if python3 -c "import socket,sys; socket.create_connection(('127.0.0.1',$pr),0.3).close()" 2>/dev/null; then
      ready=1; break
    fi
    sleep 0.1
  done
  [ -n "$ready" ] || { echo "service on :$pr did not start"; exit 1; }
done

export NAVI_SOCKS_TARGET="http://127.0.0.1:$origin/"
export NAVI_SOCKS_NOAUTH="socks5://127.0.0.1:$noauth"
export NAVI_SOCKS_AUTH="socks5://alice:s3cret@127.0.0.1:$authp"
export NAVI_SOCKS_AUTH_BAD="socks5://alice:wrong@127.0.0.1:$authp"

echo "== SOCKS5: origin :$origin, no-auth proxy :$noauth, auth proxy :$authp =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/socks5" "$root/tests/interop/socks5.nim"
# The async backends have their own SOCKS read loops; exercise both.
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/socks5_ad" "$root/tests/interop/socks5_async.nim"
nim c -r --hints:off -d:ssl -d:useChronos --path:"$root/src" -o:"$work/socks5_ch" "$root/tests/interop/socks5_async.nim"
