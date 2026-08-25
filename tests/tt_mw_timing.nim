## Deterministic timing validation of the rateLimit and concurrency middleware,
## using checkmate's time travel: under `checkmate --time-travel` the async
## `sleep` is instant but the virtual clock advances, so we can assert real pacing
## and in-flight caps in milliseconds without wall-clock flakiness or slow tests.
##
## The tests short-circuit with a canned-response middleware, so no server or
## socket is involved -- only the middleware chain and the (virtual) clock. Under
## a plain `nimble test` (no time travel) they skip, since real sleeps would make
## them slow and flaky; the checkmate CI step runs them for real.

import std/[unittest, times]
import navi/asyncdispatch
import navi/asyncdispatch/mw
import checkmate                     # timeTravelActive

# Reply immediately without calling `next`, so nothing goes over the wire.
proc canned(status: int): NaviMiddleware =
  result = proc(ctx: NaviContext): Future[void] {.async.} =
    ctx.res = initResponse(status, "OK", "HTTP/1.1", initHeaders(), "ok")

# Simulate `workMs` of in-flight work and record the peak simultaneous count.
proc workSim(peak, cur: ref int, workMs: int): NaviMiddleware =
  result = proc(ctx: NaviContext): Future[void] {.async.} =
    inc cur[]
    if cur[] > peak[]: peak[] = cur[]
    await sleep(workMs)              # virtual under time travel
    dec cur[]
    await ctx.next()

proc fireN(api: Navi, n: int): Future[void] {.async.} =
  var futs: seq[Future[Response]]
  for _ in 0 ..< n: futs.add api.get("http://mw.test/")
  for f in futs: discard await f

suite "rateLimit pacing (time travel)":
  test "five requests at 10/s (burst 1) span ~400ms of virtual time":
    if not timeTravelActive():
      skip()                        # meaningful only under `checkmate --time-travel`
    else:
      var cfg = initNaviConfig()
      cfg.middleware = @[rateLimit(perSec = 10, burst = 1), canned(200)]
      let api = newNavi(cfg)
      let t0 = epochTime()
      waitFor fireN(api, 5)          # 1 immediate, then 4 paced at 100ms each
      let elapsed = epochTime() - t0
      check elapsed >= 0.39 and elapsed <= 0.42

  test "an unlimited rate adds no delay":
    if not timeTravelActive():
      skip()
    else:
      var cfg = initNaviConfig()
      cfg.middleware = @[rateLimit(perSec = 0), canned(200)]
      let api = newNavi(cfg)
      let t0 = epochTime()
      waitFor fireN(api, 20)
      check epochTime() - t0 < 0.01

suite "concurrency cap (time travel)":
  test "at most `max` requests are in flight at once":
    if not timeTravelActive():
      skip()
    else:
      let peak = new(int)
      let cur = new(int)
      var cfg = initNaviConfig()
      cfg.middleware = @[concurrencyLimit(2), workSim(peak, cur, 50), canned(200)]
      let api = newNavi(cfg)
      let t0 = epochTime()
      waitFor fireN(api, 6)
      check peak[] == 2              # never more than 2 simultaneously past the gate
      # 6 requests in waves of 2, each 50ms of work -> ~150ms of virtual time
      check (epochTime() - t0) >= 0.145 and (epochTime() - t0) <= 0.16
