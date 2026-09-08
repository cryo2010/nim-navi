## Shared spec for the batteries middleware, written once and instantiated on each
## async backend: test_mw_async.nim and test_mw_chronos.nim each import their
## backend + mw module, define `mwBackendName`/`mwCachePort`, then `include` this.
## A test added here therefore runs under BOTH backends -- in particular chronos's
## gcsafe / strict-raises checks -- instead of silently covering only one (which is
## the whole reason the second file exists). Not named `t*`/`test_*`, so the runner
## does not try to compile it standalone.

suite "cache middleware (" & mwBackendName & ", end to end)":
  test "a fresh response is served from cache without a second request":
    var count = 0
    var c = CacheSrv(port: mwCachePort, count: addr count, requests: 1, maxAge: 300)
    var th: Thread[CacheSrv]
    startCache(th, c)
    var cfg = initNaviConfig()
    cfg.middleware = @[cache()]
    let api = newNavi(cfg)
    let url = "http://127.0.0.1:" & $mwCachePort & "/x"
    check (waitFor api.get(url)).body == "payload"
    check (waitFor api.get(url)).body == "payload"   # cache hit; no 2nd connection
    joinThread(th)
    check count == 1

suite mwBackendName & " factory instantiation":
  test "every async factory builds a NaviMiddleware":
    var cfg = initNaviConfig()
    cfg.middleware = @[
      cache(), rateLimit(10), concurrencyLimit(4),
      bearer("t"), basic("u", "p")]
    check cfg.middleware.len == 5
