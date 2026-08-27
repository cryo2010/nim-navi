## End-to-end streaming over HTTP/3 on the chronos backend: after an h2 warm-up
## teaches the client Alt-Svc, a `stream()` request rides HTTP/3 and its body is
## pulled incrementally (each) and decoded, proving the chronos h3 streaming path.
## Built with -d:ssl -d:naviHttp3.
import std/os
import pkg/chronos
import navi/chronos

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
    "stream() should ride h3 on chronos, got " & s.httpVersion
  var body = ""
  s.each(chunk):
    body.add chunk
  doAssert body == big,
    "streamed h3 body mismatch: got " & $body.len & " bytes, want " & $big.len
  echo "chronos stream() over ", s.httpVersion, " delivered ", body.len, " decoded bytes"

  await api.close()
  echo "NAVI HTTP/3 CHRONOS STREAMING OK"

waitFor main()
