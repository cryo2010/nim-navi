# Async middleware seam, `include`d by navi/<backend>/mw.nim for the three async
# backends (asyncdispatch, chronos, js). Defines the three backend templates the
# shared body expects, then pulls in the factories. The includer imports its
# backend entry first, so NaviContext / NaviMiddleware / next / sleep / Future /
# newFuture are in scope; navi guarantees the same {.async.} closure source
# compiles on all three (chronos's gcsafe/raises burden is discharged in `next`).
#
# Not a standalone module -- do not `nim check` this file directly; it is compiled
# as part of each navi/<backend>/mw.nim.

when not defined(js):
  import std/deques         # the concurrency limiter's waiter queue (native only)

template mkMw(ctx, body: untyped) =
  result = (proc(ctx: NaviContext) {.async.} = body)
template chainNext(ctx: untyped) = await ctx.next()
template paceSleep(ms: untyped) = await sleep(ms)

when not defined(js):
  proc concurrencyLimit*(maxInFlight: int): NaviMiddleware =
    ## Cap concurrent in-flight requests at `maxInFlight`; excess requests park on
    ## a FIFO queue until a slot frees. (Native async backends only -- on js the
    ## platform manages fetch concurrency.)
    var inFlight = 0
    var waiters = initDeque[Future[void]]()
    result = proc(ctx: NaviContext) {.async.} =
      if maxInFlight > 0 and inFlight >= maxInFlight:
        let w = newFuture[void]("navi.mw.concurrencyLimit")
        waiters.addLast(w)
        await w
      inc inFlight
      try:
        await ctx.next()
      finally:
        dec inFlight
        while waiters.len > 0:
          let w = waiters.popFirst()
          if not w.finished: (w.complete(); break)

include navi/private/mw_common
