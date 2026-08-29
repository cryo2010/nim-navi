## End-to-end HTTP/3 request trailers (sync client). A POST whose request carries a
## trailing HEADERS section must produce a well-formed h3 stream that the Caddy origin
## accepts: it echoes the body and answers 200 over h3 (a malformed trailer section
## would make the server RST the stream, forcing an h2 fallback or an error). Caddy
## does not surface trailers, so res.trailers is empty; the receive path is exercised
## by the h2 unit tests and the symmetric nghttp3 recv_trailer callback.
## Built with -d:ssl -d:naviHttp3.
import std/os
import std/strutils
import navi

let ca = getEnv("NAVI_H3_CA")
doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

var cfg = initNaviConfig()
cfg.tls.caFile = ca
cfg.http = {H1, H2, H3}
let api = newNavi(cfg)

discard api.get("https://localhost:4433/")   # h2 first, to learn the Alt-Svc h3 upgrade

# 1. A buffered body plus request trailers, over h3.
let r1 = api.request(POST, "https://localhost:4433/echo", body = "trailer-body",
                     trailers = initHeaders([("x-checksum", "abc123"),
                                             ("x-rows", "7")]))
doAssert r1.status == 200, "POST+trailers status " & $r1.status
doAssert r1.httpVersion == "HTTP/3", "POST+trailers should stay h3, got " & r1.httpVersion
doAssert r1.body == "echo:trailer-body", "POST+trailers echo mismatch: " & r1.body
doAssert r1.trailers.len == 0, "Caddy sends no response trailers"
echo "POST with request trailers over ", r1.httpVersion, " accepted by the origin"

# 2. Request trailers with no body: the trailing HEADERS section rides on its own.
let r2 = api.request(PUT, "https://localhost:4433/echo",
                     trailers = initHeaders([("grpc-status", "0")]))
doAssert r2.status == 200, "PUT trailers-only status " & $r2.status
doAssert r2.httpVersion == "HTTP/3", "PUT trailers-only should stay h3, got " & r2.httpVersion
echo "trailers-only request over ", r2.httpVersion, " accepted by the origin"

api.close()
echo "NAVI HTTP/3 REQUEST TRAILERS OK"
