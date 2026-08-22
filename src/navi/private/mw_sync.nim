# Sync middleware factories, `include`d by navi/mw.nim (the sync client). Mirrors
# private/mw_async.nim without `await` (the sync `next()`/`sleep()` are blocking).
# Kept as a separate fragment because the sync NaviMiddleware is a plain
# `proc(ctx)` with no Future, unlike the async trio.
#
# Not a standalone module -- compiled as part of navi/mw.nim.

import std/[base64, times, os]
import navi/private/mw/[httpcache, ratelimit]
export httpcache.CacheStore, httpcache.newCacheStore

proc cache*(store = newCacheStore()): NaviMiddleware =
  ## Serve fresh responses from `store`, revalidate stale ones (refreshing on
  ## 304), and store cacheable responses. GET/HEAD only; honors Cache-Control and
  ## Vary. Buffered `request()` only.
  result = proc(ctx: NaviContext) =
    let lk = store.lookup(ctx.req)
    case lk.kind
    of fFresh:
      ctx.res = lk.toResponse
    of fStale:
      for (k, v) in lk.revalidationHeaders: ctx.req.headers[k] = v
      try:
        ctx.next()
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
      ctx.next()
      store.storeResponse(ctx.req, ctx.res)

proc rateLimit*(perSec: float, burst = 0): NaviMiddleware =
  ## Token-bucket throttle; over budget, blocks the thread via `sleep` until the
  ## request's turn (sync client is single-threaded, so this paces it directly).
  let bucket = newTokenBucket(perSec, burst)
  result = proc(ctx: NaviContext) =
    let delayMs = bucket.take()
    if delayMs > 0: os.sleep(delayMs)   # blocking; the sync client is serial
    ctx.next()

proc concurrency*(maxInFlight: int): NaviMiddleware =
  ## No-op on the sync client: requests are already serial, so there is nothing to
  ## limit. Provided for source-compatibility with the async backends.
  discard maxInFlight
  result = proc(ctx: NaviContext) = ctx.next()

proc bearer*(token: string): NaviMiddleware =
  ## Set `Authorization: Bearer <token>` on every request.
  result = proc(ctx: NaviContext) =
    ctx.req.headers["authorization"] = "Bearer " & token
    ctx.next()

proc basic*(user, pass: string): NaviMiddleware =
  ## Set `Authorization: Basic <base64(user:pass)>` on every request.
  let cred = "Basic " & encode(user & ":" & pass)
  result = proc(ctx: NaviContext) =
    ctx.req.headers["authorization"] = cred
    ctx.next()

proc logging*(sink: proc(line: string) {.gcsafe, raises: [CatchableError].} = nil):
    NaviMiddleware =
  ## Log `VERB url -> status (Nms)` after each request. Defaults to `echo`.
  result = proc(ctx: NaviContext) =
    let t0 = epochTime()
    ctx.next()
    let ms = int((epochTime() - t0) * 1000)
    let line = $ctx.req.verb & " " & $ctx.req.url & " -> " & $ctx.res.status &
               " (" & $ms & "ms)"
    if sink != nil: sink(line) else: echo line
