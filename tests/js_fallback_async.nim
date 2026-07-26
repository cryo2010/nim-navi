## Compile-only fixture (no `test_` prefix, so tests/run.sh does not run it): a
## library-style module written on navi/asyncdispatch must also build under
## `nim js`, where the entry falls back to navi/js. Exercises the portable pieces
## -- a capturing `{.async.}` middleware that awaits `ctx.next()`, config, params.
import navi/asyncdispatch

proc bearer(token: string): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["authorization"] = "Bearer " & token
    await ctx.next()

proc fetchThing*(url: string): Future[Response] {.async.} =
  var cfg = newNaviConfig()
  cfg.middleware = @[bearer("secret")]
  let api = newNavi(cfg)
  return await api.get(url, params = @{"q": "x"})
