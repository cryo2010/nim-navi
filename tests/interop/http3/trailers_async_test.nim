## End-to-end HTTP/3 request trailers over the asyncdispatch client (shared, mux-based
## h3 connection). Mirrors trailers_test.nim: a request with a trailing HEADERS section
## must produce a stream the Caddy origin accepts and echoes over h3.
## Built with -d:ssl -d:naviHttp3.
import std/[os, strutils, asyncdispatch]
import navi/asyncdispatch

let ca = getEnv("NAVI_H3_CA")
doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.caFile = ca
  cfg.http = {H1, H2, H3}
  let api = newNavi(cfg)

  discard await api.get("https://localhost:4433/")   # h2 first, to learn Alt-Svc

  let r1 = await api.request(POST, "https://localhost:4433/echo", body = "async-trl",
                             trailers = initHeaders([("x-checksum", "deadbeef")]))
  doAssert r1.status == 200, "async POST+trailers status " & $r1.status
  doAssert r1.httpVersion == "HTTP/3", "async POST+trailers should stay h3, got " & r1.httpVersion
  doAssert r1.body == "echo:async-trl", "async echo mismatch: " & r1.body
  doAssert r1.trailers.len == 0
  echo "async POST with request trailers over ", r1.httpVersion, " accepted"

  await api.close()
  echo "NAVI HTTP/3 REQUEST TRAILERS (async) OK"

waitFor main()
