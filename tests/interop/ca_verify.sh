#!/usr/bin/env bash
# Custom-CA (TlsConfig.caFile) verification for the sync (OpenSSL) backend:
# generate a CA, sign a server cert with it, start an OpenSSL HTTPS server, and
# check navi verifies that server against the CA -- and rejects it without the CA
# (our private root is not in the system trust store). Tears everything down on
# exit. The 127.0.0.1 literal is matched against the certificate's iPAddress SAN,
# so both the chain and the identity check are exercised.
#
# Two more servers cover the certificate *identity* rules on every native
# backend: one whose subject CN is `localhost` while its only dNSName SAN is not
# (a CN must never rescue a SAN mismatch), and one carrying a partial wildcard
# alongside an ordinary one (`fo*.example.com` must not match, `*.wild.…` must).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

work="$(mktemp -d)"
srv=""
srv_mismatch=""
srv_wild=""
cleanup() {
  for p in "$srv" "$srv_mismatch" "$srv_wild"; do
    [ -n "$p" ] && kill "$p" 2>/dev/null || true
  done
  cd "$root"          # Windows cannot remove the shell's own cwd
  navi_rmtree "$work"
}
trap cleanup EXIT
cd "$work"

port=9458
mismatch_port=9482
wild_port=9483

navi_certgen ca.key ca.pem navi-test-CA

openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "$(navi_subj CN=127.0.0.1)" >/dev/null 2>&1
# A real file rather than <(...): the process substitution's /dev/fd path is
# meaningless to the native openssl.exe used under Git Bash.
printf "subjectAltName=DNS:127.0.0.1,DNS:localhost,IP:127.0.0.1" > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile san.ext \
  -out server.pem >/dev/null 2>&1

# -www answers a 200 HTML page on each connection.
openssl s_server -accept 127.0.0.1:"$port" -cert server.pem -key server.key \
  -www -quiet >"$work/s_server.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

# Wait until the server accepts TLS and validates against our CA.
ready=""
for _ in $(seq 1 50); do
  if echo | openssl s_client -connect "127.0.0.1:$port" -CAfile ca.pem 2>/dev/null \
       | grep -q "Verify return code: 0"; then ready=1; break; fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "s_server did not become ready on :$port"; cat "$work/s_server.log"; exit 1; }

export NAVI_CAFILE_URL="https://127.0.0.1:$port"
export NAVI_CAFILE_CA="$(navi_path "$work/ca.pem")"

echo "== private-CA verification: server signed by a private CA on 127.0.0.1:$port =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/ca_verify" "$root/tests/interop/ca_verify.nim"

# TLS session resumption, on all three native backends against the same server:
# `s_server -www` names the session state of the connection it answers on in the
# page it serves, and closes that connection afterwards, so a second request from
# the same client is a second handshake that can only report "Reused" if the
# cached session was presented and accepted (#431).
echo "== TLS session resumption on the second connection (sync/asyncdispatch/chronos) =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/resume_sync" "$root/tests/interop/tls_resume.nim"
nim c -r --hints:off -d:ssl -d:naviAsync --path:"$root/src" -o:"$work/resume_async" "$root/tests/interop/tls_resume.nim"
nim c -r --hints:off -d:ssl -d:naviChronos --path:"$root/src" -o:"$work/resume_chronos" "$root/tests/interop/tls_resume.nim"
# --- certificate identity: CN must not rescue a SAN mismatch ----------------
# CN=localhost, but the only SAN names a host we never ask for (and there is no
# iPAddress SAN at all). Chain-valid against the same CA, so only the identity
# check can reject it.
openssl req -newkey rsa:2048 -nodes -keyout mismatch.key -out mismatch.csr \
  -subj "$(navi_subj CN=localhost)" >/dev/null 2>&1
printf "subjectAltName=DNS:not-the-host.example" > mismatch.ext
openssl x509 -req -in mismatch.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile mismatch.ext \
  -out mismatch.pem >/dev/null 2>&1

openssl s_server -accept "$mismatch_port" -cert mismatch.pem -key mismatch.key \
  -www -quiet >"$work/s_server_mismatch.log" 2>&1 &
srv_mismatch=$!
disown 2>/dev/null || true
navi_wait_tls "localhost:$mismatch_port" \
  || { echo "s_server did not become ready on :$mismatch_port"; cat "$work/s_server_mismatch.log"; exit 1; }

export NAVI_HOSTV_CA="$NAVI_CAFILE_CA"
export NAVI_HOSTV_GOOD="https://127.0.0.1:$port"
export NAVI_HOSTV_MISMATCH="https://localhost:$mismatch_port"
export NAVI_HOSTV_MISMATCH_IP="https://127.0.0.1:$mismatch_port"

echo "== certificate identity: SAN mismatch with a matching CN, on every backend =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/host_verify_sync" \
  "$root/tests/interop/host_verify.nim"
nim c -r --hints:off -d:ssl -d:useAsync --path:"$root/src" -o:"$work/host_verify_async" \
  "$root/tests/interop/host_verify.nim"
if nimble path chronos >/dev/null 2>&1; then
  nim c -r --hints:off -d:ssl -d:useChronos --path:"$root/src" -o:"$work/host_verify_chronos" \
    "$root/tests/interop/host_verify.nim"
else
  echo "note: chronos not installed; skipping the chronos identity leg"
fi

# --- certificate identity: partial wildcards ---------------------------------
openssl req -newkey rsa:2048 -nodes -keyout wild.key -out wild.csr \
  -subj "$(navi_subj CN=navi-wildcard)" >/dev/null 2>&1
printf "subjectAltName=DNS:fo*.example.com,DNS:*.wild.example.com" > wild.ext
openssl x509 -req -in wild.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile wild.ext \
  -out wild.pem >/dev/null 2>&1

openssl s_server -accept 127.0.0.1:"$wild_port" -cert wild.pem -key wild.key \
  -www -quiet >"$work/s_server_wild.log" 2>&1 &
srv_wild=$!
disown 2>/dev/null || true
navi_wait_tls "127.0.0.1:$wild_port" \
  || { echo "s_server did not become ready on :$wild_port"; cat "$work/s_server_wild.log"; exit 1; }

export NAVI_HOSTV_WILD_PORT="$wild_port"

echo "== certificate identity: partial wildcards are rejected on :$wild_port =="
nim c -r --hints:off -d:ssl --path:"$root/src" -o:"$work/host_wildcard" \
  "$root/tests/interop/host_wildcard.nim"
