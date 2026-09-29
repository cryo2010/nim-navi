#!/usr/bin/env bash
# Unix domain socket transport: start an AF_UNIX HTTP server (echoes the Host
# header) and check navi dials it on the sync, asyncdispatch and chronos backends.
# Then a second AF_UNIX server serving TLS (openssl s_server) for the chronos
# Unix-socket TLS path and its failed-verification teardown (issue #420). Then, on
# Linux, a third AF_UNIX server serving TLS with an untrusted self-signed
# cert: every handshake fails, and the asyncdispatch client must reclaim the fd, the
# SSL and its unshared SSL_CTX each time rather than leaking one set per attempt
# (issue #427).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v python3 >/dev/null || { echo "python3 not found"; exit 127; }
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

work="$(mktemp -d)"
sock="$work/navi.sock"
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  rm -rf "$work"
}
trap cleanup EXIT

python3 "$root/tests/interop/uds_server.py" "$sock" >"$work/srv.log" 2>&1 &
pids+=($!)

ready=""
for _ in $(seq 1 50); do
  [ -S "$sock" ] && { ready=1; break; }
  sleep 0.1
done
[ -n "$ready" ] || { echo "UDS server did not start"; cat "$work/srv.log"; exit 1; }

export NAVI_UDS_PATH="$sock"

echo "== Unix domain socket: server on $sock =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/uds" "$root/tests/interop/unixsocket.nim"
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/uds_ad" "$root/tests/interop/unixsocket_async.nim"
nim c -r --hints:off -d:ssl -d:useChronos --path:"$root/src" -o:"$work/uds_ch" "$root/tests/interop/unixsocket_async.nim"

# --- TLS over the Unix socket ------------------------------------------------
# A second server, this one TLS, so the chronos backend's Unix-socket TLS path is
# exercised end to end. The certificate is valid for uds.test only, so asking for
# any other host must surface the verification failure (and must not hand back a
# live, unverified session): the #420 regression guard.
tlssock="$work/navi-tls.sock"

navi_certgen "$work/ca.key" "$work/ca.pem" navi-uds-CA
openssl req -newkey rsa:2048 -nodes -keyout "$work/server.key" -out "$work/server.csr" \
  -subj "$(navi_subj CN=uds.test)" >/dev/null 2>&1
printf "subjectAltName=DNS:uds.test" > "$work/san.ext"
openssl x509 -req -in "$work/server.csr" -CA "$work/ca.pem" -CAkey "$work/ca.key" \
  -CAcreateserial -days 1 -extfile "$work/san.ext" -out "$work/server.pem" >/dev/null 2>&1

# -www answers a 200 HTML page on each connection and then closes it. The bind
# path is RELATIVE (via a subshell cd): openssl's -unix rejects an absolute path
# much shorter than sun_path allows ("Result too large for supplied buffer" from
# bio_addr.c), and a $(mktemp -d) path is already over that limit. Only the bind
# is constrained; the client still dials the absolute path.
(cd "$work" && exec openssl s_server -unix "$(basename "$tlssock")" \
  -cert server.pem -key server.key -www -quiet) >"$work/s_server.log" 2>&1 &
pids+=($!)

ready=""
for _ in $(seq 1 50); do
  [ -S "$tlssock" ] && { ready=1; break; }
  sleep 0.1
done
[ -n "$ready" ] || { echo "TLS UDS server did not start"; cat "$work/s_server.log"; exit 1; }

export NAVI_UDS_TLS_PATH="$tlssock"
export NAVI_UDS_TLS_CA="$(navi_path "$work/ca.pem")"

echo "== TLS over Unix domain socket: server on $tlssock =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/uds_tls_ch" "$root/tests/interop/unixsocket_tls.nim"

# --- Untrusted TLS over a second Unix socket: teardown on a failed handshake (#427)
# The fd-count assertion reads /proc/self/fd, so this leg is Linux only.
if [ ! -d /proc/self/fd ]; then
  echo "== skipping the unix TLS teardown check (no /proc/self/fd) =="
  exit 0
fi

tlssock="$work/navi-tls-leak.sock"
navi_certgen "$work/uds.key" "$work/uds.pem" uds.test "DNS:uds.test"

python3 "$root/tests/interop/uds_server.py" "$tlssock" \
  "$(navi_path "$work/uds.pem")" "$(navi_path "$work/uds.key")" \
  >"$work/srv_tls.log" 2>&1 &
pids+=($!)

ready=""
for _ in $(seq 1 50); do
  [ -S "$tlssock" ] && { ready=1; break; }
  sleep 0.1
done
[ -n "$ready" ] || { echo "UDS TLS server did not start"; cat "$work/srv_tls.log"; exit 1; }

export NAVI_UDS_TLS_LEAK_PATH="$tlssock"

echo "== Unix domain socket + untrusted TLS: teardown on handshake failure =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/uds_tls_leak" "$root/tests/interop/unixsocket_tls_leak.nim"
