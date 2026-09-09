# Sync middleware seam, `include`d by navi/mw.nim (the sync client). Defines the
# three backend templates the shared body expects, then pulls in the factories.
# The sync NaviMiddleware is a plain `proc(ctx)` with no Future, and its `next()`
# / `sleep()` are blocking -- unlike the async trio, which shares private/mw_async.
#
# Not a standalone module -- compiled as part of navi/mw.nim.

import std/os

template mkMw(ctx, body: untyped) =
  result = (proc(ctx: NaviContext) = body)
template chainNext(ctx: untyped) = ctx.next()
template paceSleep(ms: untyped) = os.sleep(ms)   # blocking; the sync client is serial

proc concurrencyLimit*(maxInFlight: int): NaviMiddleware =
  ## No-op on the sync client: requests are already serial, so there is nothing to
  ## limit. Provided for source-compatibility with the async backends.
  discard maxInFlight
  result = proc(ctx: NaviContext) = ctx.next()

include navi/private/mw_common
