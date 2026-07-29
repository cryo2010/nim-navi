## Compile-only fixture (see js_fallback_async.nim): the same library-style module
## on navi/chronos, which also falls back to navi/js under `nim js` -- without
## pulling in the chronos package (its native impl is not compiled on js).
import navi/chronos

proc bearer(token: string): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["authorization"] = "Bearer " & token
    await ctx.next()

proc fetchThing*(url: string): Future[Response] {.async.} =
  var cfg = initNaviConfig()
  cfg.middleware = @[bearer("secret")]
  let api = newNavi(cfg)
  return await api.get(url, params = @{"q": "x"})
