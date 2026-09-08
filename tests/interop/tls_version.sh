#!/usr/bin/env bash
# TLS version pinning enforcement: stand up an `openssl s_server` that speaks only
# TLS 1.2 on one port and only TLS 1.3 on another, then check navi's
# minVersion/maxVersion are honored (a pin excluding the server's version fails the
# handshake). Needs an openssl that supports TLS 1.3 (OpenSSL 1.1.1+).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"
command -v openssl >/dev/null || { echo "openssl required"; exit 127; }

work="$(mktemp -d)"
s12=""; s13=""
cleanup() {
  [ -n "$s12" ] && kill "$s12" 2>/dev/null || true
  [ -n "$s13" ] && kill "$s13" 2>/dev/null || true
  navi_rmtree "$work"
}
trap cleanup EXIT

p12=9470; p13=9471
navi_certgen "$work/key.pem" "$work/cert.pem" 127.0.0.1 "IP:127.0.0.1"

# -www answers a GET with a status page; -tls1_2 / -tls1_3 pin the server version.
openssl s_server -key "$work/key.pem" -cert "$work/cert.pem" \
  -accept "$p12" -tls1_2 -www -quiet >/dev/null 2>&1 &
s12=$!
openssl s_server -key "$work/key.pem" -cert "$work/cert.pem" \
  -accept "$p13" -tls1_3 -www -quiet >/dev/null 2>&1 &
s13=$!

for spec in "$p12:-tls1_2" "$p13:-tls1_3"; do
  port="${spec%%:*}"; ver="${spec##*:}"
  navi_wait_tls "127.0.0.1:$port" "$ver" || { echo "TLS server not ready on 127.0.0.1:$port"; exit 1; }
done

export NAVI_TV_PORT12="$p12" NAVI_TV_PORT13="$p13"
echo "== TLS version pinning: TLS 1.2 server on $p12, TLS 1.3 server on $p13 =="
nim c -r --path:"$root/src" -d:ssl --hints:off "$root/tests/interop/tls_version.nim"
# chronos now runs OpenSSL, so it honors the version pins (incl. TLS 1.3) too.
if nimble path chronos >/dev/null 2>&1; then
  nim c -r --path:"$root/src" -d:ssl --hints:off "$root/tests/interop/tls_version_chronos.nim"
else
  echo "note: chronos not installed; skipping the chronos TLS version leg"
fi
