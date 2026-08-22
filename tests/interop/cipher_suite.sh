#!/usr/bin/env bash
# Cipher-suite selection enforcement: an openssl s_server pinned to one TLS 1.2
# cipher on one port and one TLS 1.3 ciphersuite on another. navi must honor
# TlsConfig.ciphers / cipherSuites -- a name the server doesn't offer fails the
# handshake. Needs an openssl that speaks TLS 1.3 (OpenSSL 1.1.1+).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v openssl >/dev/null || { echo "openssl required"; exit 127; }

work="$(mktemp -d)"
s12=""; s13=""
cleanup() {
  [ -n "$s12" ] && kill "$s12" 2>/dev/null || true
  [ -n "$s13" ] && kill "$s13" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

p12=9480; p13=9481
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$work/key.pem" -out "$work/cert.pem" -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1" >/dev/null 2>&1

# One server offers only the AES256 TLS 1.2 cipher; the other only the AES256
# TLS 1.3 ciphersuite.
openssl s_server -key "$work/key.pem" -cert "$work/cert.pem" -accept "$p12" \
  -tls1_2 -cipher 'ECDHE-RSA-AES256-GCM-SHA384' -www -quiet >/dev/null 2>&1 &
s12=$!
openssl s_server -key "$work/key.pem" -cert "$work/cert.pem" -accept "$p13" \
  -tls1_3 -ciphersuites 'TLS_AES_256_GCM_SHA384' -www -quiet >/dev/null 2>&1 &
s13=$!

for port in "$p12" "$p13"; do
  ready=""
  for _ in $(seq 1 60); do
    if echo | openssl s_client -connect "127.0.0.1:$port" 2>/dev/null | grep -q "BEGIN CERT"; then
      ready=1; break
    fi
    sleep 0.2
  done
  [ -n "$ready" ] || { echo "TLS server not ready on 127.0.0.1:$port"; exit 1; }
done

export NAVI_CS_PORT12="$p12" NAVI_CS_PORT13="$p13"
echo "== cipher selection: TLS 1.2/AES256 on $p12, TLS 1.3/AES256 on $p13 =="
nim c -r --path:"$root/src" -d:ssl --hints:off "$root/tests/interop/cipher_suite.nim"
# chronos now runs OpenSSL, so it honors cipher selection too.
if nimble path chronos >/dev/null 2>&1; then
  nim c -r --path:"$root/src" -d:ssl --hints:off "$root/tests/interop/cipher_suite_chronos.nim"
else
  echo "note: chronos not installed; skipping the chronos cipher-suite leg"
fi
