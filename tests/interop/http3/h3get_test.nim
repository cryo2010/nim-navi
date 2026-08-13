## Interop test: navi performs a real HTTP/3 GET against the local Caddy origin
## and enforces TLS verification. Exercises backend/quic (ngtcp2 + nghttp3 +
## OpenSSL 3.5) end to end. Built and run by run.sh inside the toolchain image.
import std/[strutils, os]
import navi/backend/quic

echo "ngtcp2 ", ngtcp2VersionStr(), "  nghttp3 ", nghttp3VersionStr()
let ca = getEnv("NAVI_H3_CA")
doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

# 1. A verified GET against the origin's CA succeeds.
let r = h3Get("localhost", 4433, sni = "localhost", path = "/", caFile = ca)
echo "verified h3Get -> HTTP/3 ", r.status
echo r.body
doAssert r.status == 200, "expected 200, got " & $r.status
doAssert "hello from http/3" in r.body, "unexpected body: " & r.body
echo "ok: verified GET"

# 2. Verification against the system trust store rejects the self-signed cert.
var rejected = false
try:
  discard h3Get("localhost", 4433, sni = "localhost")
except QuicError:
  rejected = true
doAssert rejected, "self-signed cert must be rejected without its CA"
echo "ok: untrusted cert rejected"

# 3. verify = false is an explicit opt-out and still works.
let r2 = h3Get("localhost", 4433, sni = "localhost", verify = false)
doAssert r2.status == 200
echo "ok: verify=false opt-out"

echo "NAVI HTTP/3 VERIFY OK"
