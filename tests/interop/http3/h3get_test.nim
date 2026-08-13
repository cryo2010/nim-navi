## Interop test: navi performs a real HTTP/3 GET against the local Caddy origin,
## exercising backend/quic (ngtcp2 + nghttp3 + OpenSSL 3.5) end to end. Built and
## run by run.sh inside the tests/interop/http3 toolchain image.
import std/strutils
import navi/backend/quic

echo "ngtcp2 ", ngtcp2VersionStr(), "  nghttp3 ", nghttp3VersionStr()
let r = h3Get("localhost", 4433, sni = "localhost", path = "/")
echo "navi h3Get -> HTTP/3 ", r.status
echo r.body
doAssert r.status == 200, "expected 200, got " & $r.status
doAssert "hello from http/3" in r.body, "unexpected body: " & r.body
echo "NAVI HTTP/3 GET OK"
