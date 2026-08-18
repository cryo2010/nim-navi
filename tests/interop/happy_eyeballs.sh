#!/usr/bin/env bash
# Happy Eyeballs (RFC 8305): stand up a good TLS server on 127.0.0.1 and hand navi
# a blackholed first address (192.0.2.1, RFC 5737 TEST-NET-1, whose SYN is dropped
# so the connect hangs). navi must race the addresses and reach the good one in
# ~the attempt delay rather than stalling on the blackhole. Needs openssl.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v openssl >/dev/null || { echo "openssl required"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

port=9475
blackhole=192.0.2.1
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$work/key.pem" -out "$work/cert.pem" -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1" >/dev/null 2>&1

openssl s_server -key "$work/key.pem" -cert "$work/cert.pem" \
  -accept "$port" -www -quiet >/dev/null 2>&1 &
srv=$!

ready=""
for _ in $(seq 1 60); do
  if echo | openssl s_client -connect "127.0.0.1:$port" 2>/dev/null | grep -q "BEGIN CERT"; then
    ready=1; break
  fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "good TLS server not ready on 127.0.0.1:$port"; exit 1; }

export NAVI_HE_CERT="$work/cert.pem" NAVI_HE_PORT="$port" NAVI_HE_BLACKHOLE="$blackhole"
echo "== Happy Eyeballs: blackhole $blackhole + good 127.0.0.1:$port =="
nim c -r --path:"$root/src" -d:ssl --hints:off "$root/tests/interop/happy_eyeballs.nim"
nim c -r --path:"$root/src" -d:ssl --hints:off "$root/tests/interop/happy_eyeballs_async.nim"
nim c -r --path:"$root/src" --hints:off "$root/tests/interop/happy_eyeballs_chronos.nim"
