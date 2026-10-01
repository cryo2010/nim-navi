#!/usr/bin/env bash
# A connect abandoned on `connectMs` must STOP (#443): run a deaf TCP listener on
# each loopback family on the SAME port, so `localhost` is a two-address Happy
# Eyeballs pool whose winner never answers the ClientHello, then let navi's connect
# time out mid-handshake. Exactly ONE connection may ever reach the listeners:
# before the fix the abandoned asyncdispatch `establish` dropped the stalled address
# and re-raced the other one with a fresh TCP connect and a fresh SSL_CTX/handshake,
# behind the back of a caller that had already received TimeoutError.
#
# Needs a dual-homed localhost (127.0.0.1 + ::1) and python3. TLS-capable host only.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"   # navi_rmtree
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

work="$(mktemp -d)"
s4=""
s6=""
cleanup() {
  [ -n "$s4" ] && kill "$s4" 2>/dev/null || true
  [ -n "$s6" ] && kill "$s6" 2>/dev/null || true
  cd "$root"          # Windows cannot remove the shell's own cwd
  navi_rmtree "$work"
}
trap cleanup EXIT

port=9476
log="$work/accepts.log"
: > "$log"

python3 "$root/tests/interop/deaf_tcp_server.py" 4 "$port" "$log" >"$work/s4.log" 2>&1 &
s4=$!
python3 "$root/tests/interop/deaf_tcp_server.py" 6 "$port" "$log" >"$work/s6.log" 2>&1 &
s6=$!
disown 2>/dev/null || true

for which in 4 6; do
  ready=""
  for _ in $(seq 1 50); do
    grep -q ready "$work/s$which.log" 2>/dev/null && { ready=1; break; }
    sleep 0.1
  done
  [ -n "$ready" ] || {
    echo "the deaf listener for IPv$which did not start on :$port"
    cat "$work/s$which.log"; exit 1; }
done

export NAVI_ABANDON_PORT="$port"

# One leg per backend that races the pool inside a single establish future. Each
# leg must leave exactly one accept behind, so the log is truncated between them.
run_leg() {
  local name="$1"; shift
  : > "$log"
  echo "== abandoned connect ($name), deaf localhost:$port =="
  nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/abandon_$name" "$@" \
    "$root/tests/interop/connect_abandon.nim"
  sleep 0.5
  local n
  n="$(wc -l < "$log" | tr -d ' ')"
  echo "connections that reached the origin ($name): $n"
  cat "$log"
  [ "$n" = "1" ] || {
    echo "FAIL ($name): the abandoned connect made $n connection(s), expected 1"
    exit 1; }
}

run_leg async
# The chronos leg is the control (structured cancellation), and only runs where the
# package is installed; a real regression there must still fail CI when it is.
if nimble path chronos >/dev/null 2>&1; then
  run_leg chronos -d:useChronos
else
  echo "note: chronos not installed; skipping the chronos abandoned-connect leg"
fi

echo "== abandoned connect: passed =="
