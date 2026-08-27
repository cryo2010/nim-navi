## End-to-end streaming over HTTP/3 on the asyncdispatch backend: after an h2 warm-up
## teaches the client Alt-Svc, a `stream()` request rides HTTP/3 and its body is
## pulled incrementally (readChunk/each) and decoded, proving the h3 streaming path
## (not just buffered request()). Built with -d:ssl -d:naviHttp3.
import std/[os, strutils, asyncdispatch]
import navi/asyncdispatch

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"
  let big = getEnv("BIG")
  doAssert big.len > 0, "BIG must hold the /big plaintext"
  var cfg = initNaviConfig()
  cfg.tls.caFile = ca
  cfg.http = {H1, H2, H3}
  let api = newNavi(cfg)

  # Warm up over h2 so the client learns the origin's h3 Alt-Svc endpoint.
  let warm = await api.get("https://localhost:4433/")
  doAssert warm.httpVersion == "HTTP/2", "warm-up should be h2, got " & warm.httpVersion

  # Stream /big over h3: Caddy gzips it, so this also exercises the streamed decode.
  let s = await api.stream(GET, "https://localhost:4433/big")
  doAssert s.status == 200, "stream status " & $s.status
  doAssert s.httpVersion == "HTTP/3",
    "stream() should ride h3, got " & s.httpVersion
  var body = ""
  s.each(chunk):
    body.add chunk
  doAssert body == big,
    "streamed h3 body mismatch: got " & $body.len & " bytes, want " & $big.len
  echo "stream() over ", s.httpVersion, " delivered ", body.len, " decoded bytes intact"

  # A short body streamed over h3 (single chunk / bodyless-friendly path).
  let s2 = await api.stream(GET, "https://localhost:4433/")
  doAssert s2.httpVersion == "HTTP/3"
  var b2 = ""
  s2.each(chunk): b2.add chunk
  doAssert "hello from http/3" in b2, "short stream body: " & b2
  echo "second stream() over ", s2.httpVersion

  await api.close()
  echo "NAVI HTTP/3 STREAMING OK"

waitFor main()
