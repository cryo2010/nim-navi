#!/usr/bin/env bash
# Entrypoint for the tests/interop/http3 image. Starts Caddy as a local h3 origin
# and runs navi's HTTP/3 GET test (backend/quic -> h3client.c: ngtcp2 + nghttp3 +
# OpenSSL 3.5 QUIC), asserting a real h3 response from the origin.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
WORK=/work
mkdir -p "$WORK"

# Self-signed cert for Caddy (SAN localhost/127.0.0.1). OPENSSL_CONF=/dev/null
# because `make install_sw` does not install openssl.cnf (none is needed here).
OPENSSL_CONF=/dev/null "$OSSL/bin/openssl" req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  >/dev/null 2>&1

# A large, compressible body for the /big route (shared with the test via BIG).
export BIG
BIG=$(printf 'navi%.0s' $(seq 1 250))   # 1000 bytes, gzips well

# Start the h3 origin (h3 on UDP 4433). Fatal if it fails: the test dials it.
caddy start --config "$DIR/Caddyfile" --adapter caddyfile >/tmp/caddy.log 2>&1 \
  || { echo "caddy failed to start"; cat /tmp/caddy.log; exit 1; }
echo "caddy: h3 origin up on udp/4433"
sleep 1

echo ">>> building and running the navi HTTP/3 GET test"
export NAVI_H3_CA="$WORK/cert.pem"   # the origin's CA, for the verified GET case
# The origin leaf's SPKI pin, in the canonical HPKP form (base64 SHA-256 of the
# SubjectPublicKeyInfo). Derived with the openssl CLI so the pin tests check navi's
# own derivation against the standard recipe rather than against itself.
NAVI_H3_PIN=$(OPENSSL_CONF=/dev/null "$OSSL/bin/openssl" x509 -in "$WORK/cert.pem" -pubkey -noout \
  | OPENSSL_CONF=/dev/null "$OSSL/bin/openssl" pkey -pubin -outform der \
  | OPENSSL_CONF=/dev/null "$OSSL/bin/openssl" dgst -sha256 -binary \
  | OPENSSL_CONF=/dev/null "$OSSL/bin/openssl" base64)
export NAVI_H3_PIN
echo "origin SPKI pin: $NAVI_H3_PIN"
nim c --hints:off --path:"$ROOT/src" -d:naviHttp3 -o:/tmp/h3get_test "$DIR/h3get_test.nim"
/tmp/h3get_test

echo ">>> building and running the h3 SPKI pin / verifyCallback test (sync client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/pin_test "$DIR/pin_test.nim"
/tmp/pin_test

echo ">>> building and running the h3 SPKI pin test (asyncdispatch client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/pin_async_test "$DIR/pin_async_test.nim"
/tmp/pin_async_test

echo ">>> building and running the h3 SPKI pin test (chronos client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/pin_chronos_test "$DIR/pin_chronos_test.nim"
/tmp/pin_chronos_test

echo ">>> building and running the h3 IP-literal origin test (sync client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/iphost_test "$DIR/iphost_test.nim"
/tmp/iphost_test

echo ">>> building and running the h3 IP-literal origin test (asyncdispatch client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/iphost_async_test "$DIR/iphost_async_test.nim"
/tmp/iphost_async_test

echo ">>> building and running the h3 IP-literal origin test (chronos client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/iphost_chronos_test "$DIR/iphost_chronos_test.nim"
/tmp/iphost_chronos_test

echo ">>> building and running the h3 ALPN gate test (#445)"
# The peer this probe needs is a QUIC listener that completes the handshake and then
# selects no ALPN protocol at all. Caddy cannot play that part (it always selects h3)
# and OpenSSL's own QUIC server is the compliant side of the exchange, so the listener
# is aioquic with no `alpn_protocols` configured, on udp/4434 with the origin's cert.
python3 "$DIR/noalpn_server.py" "$WORK/cert.pem" "$WORK/key.pem" 4434 \
  >/tmp/noalpn.log 2>&1 &
NOALPN_PID=$!
trap 'kill "$NOALPN_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do
  if grep -q listening /tmp/noalpn.log; then break; fi
  sleep 0.1
done
if ! grep -q listening /tmp/noalpn.log; then
  echo "the no-ALPN QUIC listener failed to start"; cat /tmp/noalpn.log; exit 1
fi
echo "aioquic: no-ALPN QUIC listener up on udp/4434"
export NAVI_H3_NOALPN_LOG=/tmp/noalpn.log   # the probe reads the close code back off it
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/alpn_test "$DIR/alpn_test.nim"
/tmp/alpn_test

# The async and chronos openers drive the handshake with their own loops and tear the
# connection down from their own except arms, so each needs its own leg: only the C
# check is shared, and what makes the refusal useful is the pre-submit classification
# every opener has to get right. They reuse the listener above (still running).
echo ">>> ... and the same gate on the asyncdispatch opener"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/alpn_async_test "$DIR/alpn_async_test.nim"
/tmp/alpn_async_test

echo ">>> ... and the same gate on the chronos opener"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/alpn_chronos_test "$DIR/alpn_chronos_test.nim"
/tmp/alpn_chronos_test

kill "$NOALPN_PID" 2>/dev/null || true
trap - EXIT

echo ">>> building and running the transparent h3 dispatch test (sync client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/dispatch_test "$DIR/dispatch_test.nim"
/tmp/dispatch_test

echo ">>> building and running the transparent h3 dispatch test (asyncdispatch client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/dispatch_async_test "$DIR/dispatch_async_test.nim"
/tmp/dispatch_async_test

echo ">>> building and running the transparent h3 dispatch test (chronos client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/dispatch_chronos_test "$DIR/dispatch_chronos_test.nim"
/tmp/dispatch_chronos_test

echo ">>> building and running the h3 request-trailers test (sync client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/trailers_test "$DIR/trailers_test.nim"
/tmp/trailers_test

echo ">>> building and running the h3 request-trailers test (asyncdispatch client)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/trailers_async_test "$DIR/trailers_async_test.nim"
/tmp/trailers_async_test

echo ">>> building and running the h3 multiplexing test (concurrent streams)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/dispatch_mux_test "$DIR/dispatch_mux_test.nim"
/tmp/dispatch_mux_test

echo ">>> building and running the h3 streaming test (stream()/each over h3)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/stream_async_test "$DIR/stream_async_test.nim"
/tmp/stream_async_test

echo ">>> building and running the h3 streaming test (chronos backend)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/stream_chronos_test "$DIR/stream_chronos_test.nim"
/tmp/stream_chronos_test

echo ">>> building and running the h3 SSL_CTX cache test (sharing, release, bound)"
# A PKCS#12 client credential for the benchmark below: decoding one is the slowest
# part of building the context, which is what #454 stopped doing per connection.
if OPENSSL_CONF=/dev/null "$OSSL/bin/openssl" pkcs12 -export -out "$WORK/client.p12" \
     -inkey "$WORK/key.pem" -in "$WORK/cert.pem" -passout pass:navi >/dev/null 2>&1; then
  export NAVI_H3_P12="$WORK/client.p12" NAVI_H3_P12_PASS=navi
else
  echo "note: could not build a PKCS#12 bundle; benchmarking without a credential"
fi
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/ctxcache_test "$DIR/ctxcache_test.nim"
/tmp/ctxcache_test
echo ">>> ... and again with the cache off (NAVI_H3_CTX_CACHE=0), the before picture"
NAVI_H3_CTX_CACHE=0 /tmp/ctxcache_test

echo ">>> building and running the h3 leak check (fd + heap growth)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -o:/tmp/leak_test "$DIR/leak_test.nim"
/tmp/leak_test

echo ">>> building and running the h3 sanitizer check (ASan + UBSan)"
nim c --hints:off --path:"$ROOT/src" -d:ssl -d:naviHttp3 -d:useMalloc \
  --passC:"-fsanitize=address,undefined -fno-omit-frame-pointer" \
  --passL:"-fsanitize=address,undefined" -o:/tmp/leak_asan "$DIR/leak_test.nim"
ASAN_OPTIONS=detect_leaks=0:abort_on_error=1:print_stacktrace=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1:suppressions="$ROOT/tests/leakcheck/navi.ubsan.supp" \
  /tmp/leak_asan
