#!/usr/bin/env bash
# Mutual-TLS interop: generate a CA plus server and client certificates, start an
# OpenSSL server that requires a client certificate, and run navi's mTLS test
# against it. Tears everything down on exit.
#
#   openssl s_server -Verify 1 rejects any client that does not present a cert
#   signed by the CA, so this exercises navi's TlsConfig.certFile/keyFile.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

work="$(mktemp -d)"
srv=""
srv_chain=""
cleanup() {
  for p in "$srv" "$srv_chain"; do
    [ -n "$p" ] && kill "$p" 2>/dev/null || true
  done
  cd "$root"          # Windows cannot remove the shell's own cwd
  navi_rmtree "$work"
}
trap cleanup EXIT
cd "$work"

port=9455
chain_port=9484

# A CA that signs both the server and the client certificate.
navi_certgen ca.key ca.pem navi-test-CA

openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "$(navi_subj CN=localhost)" >/dev/null 2>&1
# A real file rather than <(...): the process substitution's /dev/fd path is
# meaningless to the native openssl.exe used under Git Bash.
printf "subjectAltName=DNS:localhost,IP:127.0.0.1" > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile san.ext \
  -out server.pem >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
  -subj "$(navi_subj CN=navi-client)" >/dev/null 2>&1
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -out client.pem >/dev/null 2>&1

# The same client credential re-encoded into every format navi accepts, so one
# server can validate them all: an encrypted PEM key, a PKCS#12 bundle, a DER cert +
# key, an encrypted PKCS#8 DER key, and a PEM key with text before its boundary.
# (The PEM and in-memory paths reuse client.pem/client.key.)
pass="navi-secret"
openssl rsa -in client.key -out client.enc.key -aes256 -passout "pass:$pass" >/dev/null 2>&1
openssl pkcs12 -export -inkey client.key -in client.pem -certfile ca.pem \
  -passout "pass:$pass" -out client.p12 >/dev/null 2>&1
openssl x509 -in client.pem -outform DER -out client.der.crt >/dev/null 2>&1
openssl rsa -in client.key -outform DER -out client.der.key >/dev/null 2>&1
# An encrypted PKCS#8 DER key (EncryptedPrivateKeyInfo). OpenSSL's
# SSL_CTX_use_PrivateKey_file(SSL_FILETYPE_ASN1) cannot decrypt this shape and never
# sees the passphrase, so navi decodes DER keys itself now (#436).
openssl pkcs8 -topk8 -in client.key -outform DER -v2 aes-256-cbc \
  -passout "pass:$pass" -out client.der.enc.key >/dev/null 2>&1
# A PEM key whose first character is '0' -- the ASCII form of the ASN.1 SEQUENCE tag
# the old sniff tested for. RFC 7468 5.2 allows explanatory text before the
# "-----BEGIN" boundary and OpenSSL's PEM readers skip it, so this is still a valid
# PEM file; navi must not route it to the DER loader (#436).
{ printf '0 explanatory text before the PEM boundary\n'; cat client.key; } \
  > client.zero.key

# -Verify 1 makes a client certificate mandatory; -www answers a 200 HTML page.
openssl s_server -accept "$port" -cert server.pem -key server.key \
  -CAfile ca.pem -Verify 1 -www -quiet >"$work/s_server.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

# Wait until the server accepts TLS.
ready=""
for _ in $(seq 1 50); do
  if echo | openssl s_client -connect "localhost:$port" \
       -cert client.pem -key client.key -CAfile ca.pem 2>/dev/null \
       | grep -q "Verify return code: 0"; then ready=1; break; fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "s_server did not become ready on :$port"; cat "$work/s_server.log"; exit 1; }

# --- two-tier client PKI: root -> issuing CA -> client ----------------------
# The common corporate shape. The .p12 carries the issuing CA through -certfile,
# and the second server trusts only the root (-CAfile ca.pem -Verify 2), so the
# handshake succeeds only if navi presents the intermediate along with its leaf.
printf "basicConstraints=critical,CA:TRUE,pathlen:0\nkeyUsage=critical,keyCertSign,cRLSign" > sub.ext
openssl req -newkey rsa:2048 -nodes -keyout sub.key -out sub.csr \
  -subj "$(navi_subj CN=navi-issuing-CA)" >/dev/null 2>&1
openssl x509 -req -in sub.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile sub.ext -out sub.pem >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout client2.key -out client2.csr \
  -subj "$(navi_subj CN=navi-client-2tier)" >/dev/null 2>&1
openssl x509 -req -in client2.csr -CA sub.pem -CAkey sub.key -CAcreateserial -days 1 \
  -out client2.pem >/dev/null 2>&1
openssl pkcs12 -export -inkey client2.key -in client2.pem -certfile sub.pem \
  -passout "pass:$pass" -out client2.p12 >/dev/null 2>&1

# -verify_return_error matters: without it s_server's default verify callback
# only logs a failed client-chain build and serves the page anyway.
openssl s_server -accept "$chain_port" -cert server.pem -key server.key \
  -CAfile ca.pem -Verify 2 -verify_return_error -www -quiet \
  >"$work/s_server_chain.log" 2>&1 &
srv_chain=$!
disown 2>/dev/null || true
# Probe with a credential the server trusts (the root-signed client.pem): with
# -Verify plus -verify_return_error a bare probe is aborted before s_client prints
# the server certificate on some OpenSSL builds, so it would never look ready.
navi_wait_tls "localhost:$chain_port" -cert client.pem -key client.key -CAfile ca.pem \
  || { echo "s_server did not become ready on :$chain_port"; cat "$work/s_server_chain.log"; exit 1; }

export NAVI_MTLS_CHAIN_URL="https://localhost:$chain_port"
export NAVI_MTLS_P12_CHAIN="$(navi_path "$work/client2.p12")"

export NAVI_MTLS_URL="https://localhost:$port"
export NAVI_MTLS_CA="$(navi_path "$work/ca.pem")"
export NAVI_MTLS_CERT="$(navi_path "$work/client.pem")"
export NAVI_MTLS_KEY="$(navi_path "$work/client.key")"
export NAVI_MTLS_ENCKEY="$(navi_path "$work/client.enc.key")"
export NAVI_MTLS_P12="$(navi_path "$work/client.p12")"
export NAVI_MTLS_DERCERT="$(navi_path "$work/client.der.crt")"
export NAVI_MTLS_DERKEY="$(navi_path "$work/client.der.key")"
export NAVI_MTLS_DERENCKEY="$(navi_path "$work/client.der.enc.key")"
export NAVI_MTLS_ZEROKEY="$(navi_path "$work/client.zero.key")"
export NAVI_MTLS_PASS="$pass"

# Same test on every native backend (js does not present client certs). chronos
# now runs OpenSSL, so it presents client certificates like sync/asyncdispatch.
nim c -r --hints:off -d:ssl -o:"$work/mtls_sync"  "$root/tests/interop/mtls.nim"
nim c -r --hints:off -d:ssl -d:useAsync -o:"$work/mtls_async" "$root/tests/interop/mtls.nim"
if nimble path chronos >/dev/null 2>&1; then
  nim c -r --hints:off -d:ssl -d:useChronos -o:"$work/mtls_chronos" \
    "$root/tests/interop/mtls.nim"
else
  echo "note: chronos not installed; skipping the chronos mTLS leg"
fi

# clearTlsSecrets: the credential is wiped out of a LIVE client and every request
# is made afterwards, so a server that mandates a client certificate proves the
# eagerly built contexts are what keeps mTLS working (#438). Same three backends.
echo "== clearTlsSecrets keeps mTLS working after the wipe =="
nim c -r --hints:off -d:ssl -o:"$work/clearsec_sync" \
  "$root/tests/interop/tls_clear_secrets.nim"
nim c -r --hints:off -d:ssl -d:useAsync -o:"$work/clearsec_async" \
  "$root/tests/interop/tls_clear_secrets.nim"
if nimble path chronos >/dev/null 2>&1; then
  nim c -r --hints:off -d:ssl -d:useChronos -o:"$work/clearsec_chronos" \
    "$root/tests/interop/tls_clear_secrets.nim"
else
  echo "note: chronos not installed; skipping the chronos clearTlsSecrets leg"
fi

# An encrypted key with no configured passphrase must fail fast. OpenSSL's default
# PEM callback prompts on /dev/tty and falls back to stdin, so re-run the sync
# binary with stdin held open by a pipe nobody ever writes to: a prompt would block
# there forever, and the timeout turns that into a failure instead of a hung job.
if command -v timeout >/dev/null 2>&1 && command -v mkfifo >/dev/null 2>&1; then
  echo "== encrypted key with no passphrase must not prompt =="
  mkfifo "$work/stdin.fifo"
  sleep 60 > "$work/stdin.fifo" &          # holds the write end open, sends nothing
  holder=$!
  rc=0
  timeout 30 "$work/mtls_sync" < "$work/stdin.fifo" >"$work/noprompt.log" 2>&1 || rc=$?
  kill "$holder" 2>/dev/null || true
  if [ "$rc" -eq 124 ]; then
    echo "FAIL: the client blocked on a PEM passphrase prompt"; cat "$work/noprompt.log"; exit 1
  elif [ "$rc" -ne 0 ]; then
    echo "FAIL: the no-prompt run exited $rc"; cat "$work/noprompt.log"; exit 1
  fi
  echo "no-prompt run passed (stdin was an open pipe)"
else
  echo "note: timeout/mkfifo unavailable; skipping the PEM no-prompt leg"
fi
