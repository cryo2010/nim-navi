## End-to-end transparent HTTP/3 dispatch on the asyncdispatch backend: the async
## client talks to Caddy over h2 first (learning Alt-Svc), then transparently
## upgrades subsequent requests to HTTP/3, driven by the asyncdispatch event loop.
## Built with -d:ssl -d:naviHttp3.
import std/[os, strutils, asyncdispatch]
import navi/asyncdispatch

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"
  var cfg = initNaviConfig()
  cfg.tls.caFile = ca
  cfg.http = {H1, H2, H3}
  let api = newNavi(cfg)

  let r1 = await api.get("https://localhost:4433/")
  doAssert r1.status == 200, "first GET status " & $r1.status
  doAssert r1.httpVersion == "HTTP/2", "first GET should be h2, got " & r1.httpVersion
  echo "first GET over ", r1.httpVersion

  let r2 = await api.get("https://localhost:4433/")
  doAssert r2.status == 200
  doAssert r2.httpVersion == "HTTP/3", "second GET should upgrade to h3, got " & r2.httpVersion
  doAssert "hello from http/3" in r2.body
  echo "second GET transparently over ", r2.httpVersion

  let r3 = await api.post("https://localhost:4433/echo", body = "async-body-7")
  doAssert r3.status == 200 and r3.httpVersion == "HTTP/3"
  doAssert r3.body == "echo:async-body-7", "POST echo mismatch: " & r3.body
  echo "POST over ", r3.httpVersion, " echoed the uploaded body"

  await api.close()
  echo "NAVI HTTP/3 ASYNC OK"

waitFor main()
