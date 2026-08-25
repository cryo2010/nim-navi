## Concurrency-limit middleware: cap the number of simultaneous in-flight requests.
## Six GETs are launched at once against a server that holds each for 150ms and
## records its peak concurrency; `mw.concurrencyLimit(2)` keeps that peak at 2 by
## parking the rest on a queue until a slot frees.
##
##   nim c -r examples/middleware/concurrencyLimit.nim

import navi/asyncdispatch          # brings Future / waitFor / all along
import navi/asyncdispatch/mw
import ./server

proc main() {.async.} =
  var state: ServerState
  startServer(port = 9703, state = addr state, delayMs = 150)

  var cfg = initNaviConfig()
  cfg.middleware = @[mw.concurrencyLimit(maxInFlight = 2)]
  let api = newNavi(cfg)
  let url = "http://127.0.0.1:9703/x"

  var futures: seq[Future[Response]]
  for _ in 0 ..< 6: futures.add api.get(url)
  discard await all(futures)

  echo "6 requests, cap 2 -> peak concurrency seen at the server: ", state.peak.load
  doAssert state.peak.load <= 2, "no more than 2 requests should be in flight at once"
  echo "ok"

waitFor main()
