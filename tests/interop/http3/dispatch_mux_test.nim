## HTTP/3 multiplexing on the asyncdispatch backend: after establishing one h3
## connection, fire many concurrent requests and assert they all succeed over the
## SAME connection (proving they share it as concurrent QUIC streams).
import std/[os, strutils, asyncdispatch]
import navi/asyncdispatch

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  let expectedBig = getEnv("BIG")
  doAssert ca.len > 0 and expectedBig.len > 0
  var cfg = initNaviConfig()
  cfg.tls.caFile = ca
  cfg.http = {H1, H2, H3}
  let api = newNavi(cfg)

  discard await api.get("https://localhost:4433/")           # h2, learn Alt-Svc
  let warm = await api.get("https://localhost:4433/big")     # h3, establish conn
  doAssert warm.httpVersion == "HTTP/3"
  doAssert api.h3ConnCount == 1, "expected 1 h3 conn, got " & $api.h3ConnCount

  const N = 12
  var futs: seq[Future[Response]]
  for i in 0 ..< N:
    futs.add api.get("https://localhost:4433/big")
  let results = await all(futs)
  for r in results:
    doAssert r.status == 200 and r.httpVersion == "HTTP/3", "bad concurrent result"
    doAssert r.body == expectedBig, "concurrent body mismatch"
  doAssert api.h3ConnCount == 1,
    "concurrent requests should share ONE h3 conn, got " & $api.h3ConnCount
  echo N, " concurrent GETs multiplexed over ", api.h3ConnCount, " h3 connection"

  # Cold-burst regression (#242): a FRESH client whose h3 cache is empty. All N
  # first requests want h3 and race the connect at once; without the pendingH3
  # coalescing each would open -- and leak -- its own QUIC connection. They must
  # coalesce onto ONE. (The warm burst above cannot catch this: it reuses a conn.)
  block:
    let cold = newNavi(cfg)
    discard await cold.get("https://localhost:4433/")          # h2 only: learn Alt-Svc
    doAssert cold.h3ConnCount == 0, "h3 cache should be empty before the cold burst"
    var burst: seq[Future[Response]]
    for i in 0 ..< N:
      burst.add cold.get("https://localhost:4433/big")         # all cold -> h3, at once
    let cr = await all(burst)
    for r in cr:
      doAssert r.status == 200 and r.httpVersion == "HTTP/3", "bad cold-burst result"
    doAssert cold.h3ConnCount == 1,
      "cold burst must coalesce onto ONE h3 conn, got " & $cold.h3ConnCount
    echo "cold burst of ", N, " coalesced onto ", cold.h3ConnCount, " h3 connection"
    await cold.close()

  await api.close()
  echo "NAVI HTTP/3 MUX OK"

waitFor main()
