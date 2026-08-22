## Batteries middleware on the chronos client: cache end to end and instantiation
## of every async factory (chronos enforces gcsafe/strict-raises, so this proves
## the shared async source is accepted there too).

import unittest
import navi/chronos
import navi/chronos/mw
import ./support                     # CacheSrv / startCache

suite "cache middleware (chronos, end to end)":
  test "a fresh response is served from cache without a second request":
    var count = 0
    var c = CacheSrv(port: 9140, count: addr count, requests: 1, maxAge: 300)
    var th: Thread[CacheSrv]
    startCache(th, c)
    var cfg = initNaviConfig()
    cfg.middleware = @[cache()]
    let api = newNavi(cfg)
    let url = "http://127.0.0.1:9140/x"
    check (waitFor api.get(url)).body == "payload"
    check (waitFor api.get(url)).body == "payload"   # cache hit; no 2nd connection
    joinThread(th)
    check count == 1

suite "chronos factory instantiation":
  test "every async factory builds a NaviMiddleware":
    var cfg = initNaviConfig()
    cfg.middleware = @[
      cache(), rateLimit(10), concurrency(4),
      bearer("t"), basic("u", "p"), logging()]
    check cfg.middleware.len == 6
