## Rate-limit middleware: a token bucket paces outgoing requests to a sustained
## rate. Here `perSec = 5, burst = 1` lets one request go immediately, then meters
## the next four ~200ms apart, so five concurrent GETs take roughly 800ms overall.
##
##   nim c -r examples/middleware/rateLimit.nim

import std/times                  # epochTime; navi/asyncdispatch re-exports the rest
import navi/asyncdispatch          # brings Future / waitFor / all along
import navi/asyncdispatch/mw
import ./server

proc main() {.async.} =
  var state: ServerState
  startServer(port = 9702, state = addr state)   # no delay; we measure client pacing

  var cfg = initNaviConfig()
  cfg.middleware = @[mw.rateLimit(perSec = 5, burst = 1)]
  let api = newNavi(cfg)
  let url = "http://127.0.0.1:9702/x"

  let t0 = epochTime()
  var futures: seq[Future[Response]]
  for _ in 0 ..< 5: futures.add api.get(url)
  discard await all(futures)
  let elapsedMs = int((epochTime() - t0) * 1000)

  echo "5 requests at 5/sec (burst 1) took ", elapsedMs, " ms"
  doAssert elapsedMs >= 600, "the bucket should have throttled the burst"
  echo "ok"

waitFor main()
