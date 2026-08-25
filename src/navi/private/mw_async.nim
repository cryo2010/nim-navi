# Shared async middleware factories, `include`d by navi/<backend>/mw.nim for the
# three async backends (asyncdispatch, chronos, js). The includer imports its
# backend entry first, so NaviContext / NaviMiddleware / next / sleep / Future /
# newFuture are in scope; navi guarantees the same {.async.} closure source
# compiles on all three (chronos's gcsafe/raises burden is discharged in `next`).
#
# Not a standalone module -- do not `nim check` this file directly; it is compiled
# as part of each navi/<backend>/mw.nim.

import std/base64
when not defined(js):
  import std/deques         # the concurrency limiter's waiter queue (native only)
import navi/private/mw/[httpcache, ratelimit]
export httpcache.CacheStore, httpcache.newCacheStore

proc cache*(store = newCacheStore()): NaviMiddleware =
  ## Serve fresh responses from `store`, revalidate stale ones (If-None-Match /
  ## If-Modified-Since, refreshing on 304), and store cacheable responses. GET/HEAD
  ## only; honors Cache-Control no-store/no-cache/private and Vary. Buffered
  ## `request()` only -- streamed responses bypass middleware and are not cached.
  result = proc(ctx: NaviContext) {.async.} =
    let lk = store.lookup(ctx.req)
    case lk.kind
    of fFresh:
      ctx.res = lk.toResponse                       # short-circuit: no request
    of fStale:
      for (k, v) in lk.revalidationHeaders: ctx.req.headers[k] = v
      try:
        await ctx.next()
      except HttpError as e:
        # A 304 to our conditional request is the success case, but navi's
        # throw-on-non-2xx fires first; convert it. Other errors still propagate.
        if e.response.status == 304: ctx.res = e.response
        else: raise
      if ctx.res.status == 304:
        ctx.res = store.refreshOn304(ctx.req, ctx.res)
      else:
        store.storeResponse(ctx.req, ctx.res)
    of fMiss:
      await ctx.next()
      store.storeResponse(ctx.req, ctx.res)

proc rateLimit*(perSec: float, burst = 0): NaviMiddleware =
  ## Token-bucket throttle: at most `perSec` requests/second sustained, up to
  ## `burst` at once (default ceil(perSec)). Over budget, the request awaits its
  ## turn via the backend's async `sleep`.
  let bucket = newTokenBucket(perSec, burst)
  result = proc(ctx: NaviContext) {.async.} =
    let delayMs = bucket.take()
    if delayMs > 0: await sleep(delayMs)
    await ctx.next()

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

proc bearer*(token: string): NaviMiddleware =
  ## Set `Authorization: Bearer <token>` on every request.
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["authorization"] = "Bearer " & token
    await ctx.next()

proc basic*(user, pass: string): NaviMiddleware =
  ## Set `Authorization: Basic <base64(user:pass)>` on every request.
  let cred = "Basic " & encode(user & ":" & pass)
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["authorization"] = cred
    await ctx.next()
