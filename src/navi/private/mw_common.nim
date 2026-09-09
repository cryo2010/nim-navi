# Shared middleware factories, `include`d by private/mw_sync (the sync client) and
# private/mw_async (the asyncdispatch / chronos / js async trio). Each includer
# defines three seam templates before the include, so cache / rateLimit / bearer /
# basic can be written once here regardless of backend:
#
#   mkMw(ctx, body)   assign `body` to `result` as a NaviMiddleware proc literal
#                     (sync: a plain `proc(ctx)`; async: a `proc(ctx) {.async.}`)
#   chainNext(ctx)    invoke the next middleware (sync: `ctx.next()`; async:
#                     `await ctx.next()`)
#   paceSleep(ms)     pause `ms` milliseconds (sync: a blocking `os.sleep`; async:
#                     the backend's `await sleep`)
#
# The includers keep only what genuinely differs between backends: the
# `concurrencyLimit` factory (a no-op on the serial sync client, a Future-parked
# FIFO on the native async backends, absent on js) and the extra imports each seam
# needs (`std/os` for the sync sleep, `std/deques` for the async waiter queue).
#
# Not a standalone module -- do not `nim check` this file directly; it is compiled
# as part of navi/mw.nim and each navi/<backend>/mw.nim.

import std/base64
import navi/private/mw/[httpcache, ratelimit]
export httpcache.CacheStore, httpcache.newCacheStore

proc cache*(store = newCacheStore()): NaviMiddleware =
  ## Serve fresh responses from `store`, revalidate stale ones (If-None-Match /
  ## If-Modified-Since, refreshing on 304), and store cacheable responses. GET/HEAD
  ## only; honors Cache-Control no-store/no-cache/private and Vary. Buffered
  ## `request()` only -- streamed responses bypass middleware and are not cached.
  mkMw(ctx):
    let lk = store.lookup(ctx.req)
    case lk.kind
    of fFresh:
      ctx.res = lk.toResponse                       # short-circuit: no request
    of fStale:
      for (k, v) in lk.revalidationHeaders: ctx.req.headers[k] = v
      try:
        chainNext(ctx)
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
      chainNext(ctx)
      store.storeResponse(ctx.req, ctx.res)

proc rateLimit*(perSec: float, burst = 0): NaviMiddleware =
  ## Token-bucket throttle: at most `perSec` requests/second sustained, up to
  ## `burst` at once (default ceil(perSec)). Over budget, the request waits its
  ## turn via `paceSleep` -- a blocking sleep on the serial sync client, an async
  ## sleep on the async backends.
  let bucket = newTokenBucket(perSec, burst)
  mkMw(ctx):
    let delayMs = bucket.take()
    if delayMs > 0: paceSleep(delayMs)
    chainNext(ctx)

proc bearer*(token: string): NaviMiddleware =
  ## Set `Authorization: Bearer <token>` on every request.
  mkMw(ctx):
    ctx.req.headers["authorization"] = "Bearer " & token
    chainNext(ctx)

proc basic*(user, pass: string): NaviMiddleware =
  ## Set `Authorization: Basic <base64(user:pass)>` on every request.
  let cred = "Basic " & encode(user & ":" & pass)
  mkMw(ctx):
    ctx.req.headers["authorization"] = cred
    chainNext(ctx)
