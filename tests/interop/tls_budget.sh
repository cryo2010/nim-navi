#!/usr/bin/env bash
# The sync backend's establishment and read budgets as single wall clocks (#442).
# Stands up three TLS servers with stall_tls_server.py:
#   * a handshake-deaf one (accepts the TCP connection, never answers the
#     ClientHello), so a bounded handshake can only be ended by navi's own budget
#     and must surface as navi's TimeoutError with the connect wording;
#   * one that goes silent after the request and then, late in the client's read
#     budget, writes a bare TLS record header, so the readiness wait is woken by
#     bytes that carry no application data and SSL_read has to recv again -- the
#     overshoot this issue is about;
#   * a healthy one, as the control for both handshake paths.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }

work="$(mktemp -d)"
deaf=""
partial=""
good=""
cleanup() {
  for pid in "$deaf" "$partial" "$good"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  cd "$root"          # Windows cannot remove the shell's own cwd
  navi_rmtree "$work"
}
trap cleanup EXIT

deaf_port=9486
partial_port=9487
good_port=9488
# When the partial record lands, relative to the request reaching the server. Well
# inside the 2000 ms read budget the test arms, and late enough that re-arming the
# receive timeout with the FULL budget (the defect) roughly doubles the stall.
stall_ms=1700

navi_certgen "$work/key.pem" "$work/cert.pem" 127.0.0.1 "IP:127.0.0.1,DNS:127.0.0.1"

start_server() {
  ## start_server <name> <port> <mode> <delayms>; sets $pid_out to the child pid.
  local name="$1" port="$2" mode="$3" delay="$4"
  : > "$work/$name.accepts"
  python3 "$root/tests/interop/stall_tls_server.py" \
    "$work/cert.pem" "$work/key.pem" "$port" "$work/$name.accepts" \
    "$mode" "$delay" >"$work/$name.log" 2>&1 &
  pid_out=$!
  disown 2>/dev/null || true
  local _
  for _ in $(seq 1 50); do
    grep -q ready "$work/$name.log" 2>/dev/null && return 0
    sleep 0.1
  done
  echo "$name TLS server did not start"; cat "$work/$name.log"; exit 1
}

start_server deaf    "$deaf_port"    hsdeaf  0;           deaf="$pid_out"
start_server partial "$partial_port" partial "$stall_ms"; partial="$pid_out"
start_server good    "$good_port"    answer  0;           good="$pid_out"

export NAVI_BUDGET_CA="$(navi_path "$work/cert.pem")"
export NAVI_BUDGET_DEAF="$deaf_port"
export NAVI_BUDGET_PARTIAL="$partial_port"
export NAVI_BUDGET_GOOD="$good_port"
export NAVI_BUDGET_STALL="$stall_ms"

echo "== sync establishment + read budgets: deaf :$deaf_port partial :$partial_port good :$good_port =="
nim c -r --hints:off -d:ssl --path:"$root/src" \
  -o:"$work/tls_budget" "$root/tests/interop/tls_budget.nim"
