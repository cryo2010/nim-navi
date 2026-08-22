## Batteries middleware: cache + rate-limit cores (unit) and the cache middleware
## end to end on the sync client, plus sync factory instantiation.

import unittest
import std/os
import navi
import navi/mw                                   # sync factories: cache/rateLimit/...
import navi/core/[request, response, headers, url]
import navi/private/mw/[httpcache, ratelimit]    # the pure cores, for unit tests
import ./support                                 # CacheSrv / startCache

proc req(verb: HttpVerb, url: string, hdrs: openArray[(string, string)] = []): Request =
  Request(verb: verb, url: parseUrl(url), headers: initHeaders(hdrs))

proc resp(status: int, hdrs: openArray[(string, string)], body: string): Response =
  initResponse(status, "OK", "HTTP/1.1", initHeaders(hdrs), body)

suite "cache core (RFC 9111 subset)":
  test "a fresh entry is a hit and serves the stored body":
    let s = newCacheStore()
    s.storeResponse(req(GET, "http://x/a"), resp(200, {"cache-control": "max-age=60"}, "hi"))
    let lk = s.lookup(req(GET, "http://x/a"))
    check lk.kind == fFresh
    check lk.toResponse.body == "hi"

  test "a missing entry is a miss":
    check newCacheStore().lookup(req(GET, "http://x/none")).kind == fMiss

  test "a stale entry with a validator is revalidatable":
    let s = newCacheStore()
    s.storeResponse(req(GET, "http://x/b"),
      resp(200, {"cache-control": "max-age=0", "etag": "\"v1\""}, "hi"))
    let lk = s.lookup(req(GET, "http://x/b"))
    check lk.kind == fStale
    check ("if-none-match", "\"v1\"") in lk.revalidationHeaders

  test "a stale entry without a validator is not usable (miss)":
    let s = newCacheStore()
    s.storeResponse(req(GET, "http://x/c"), resp(200, {"cache-control": "max-age=0"}, "hi"))
    check s.lookup(req(GET, "http://x/c")).kind == fMiss

  test "no-store is never cached":
    let s = newCacheStore()
    s.storeResponse(req(GET, "http://x/d"), resp(200, {"cache-control": "no-store"}, "hi"))
    check s.lookup(req(GET, "http://x/d")).kind == fMiss

  test "non-GET/HEAD requests are not cached":
    let s = newCacheStore()
    s.storeResponse(req(POST, "http://x/e"), resp(200, {"cache-control": "max-age=60"}, "hi"))
    check s.lookup(req(POST, "http://x/e")).kind == fMiss

  test "Vary keys the entry on the named request headers":
    let s = newCacheStore()
    s.storeResponse(req(GET, "http://x/f", {"accept": "application/json"}),
      resp(200, {"cache-control": "max-age=60", "vary": "Accept"}, "json"))
    check s.lookup(req(GET, "http://x/f", {"accept": "application/json"})).kind == fFresh
    check s.lookup(req(GET, "http://x/f", {"accept": "text/xml"})).kind == fMiss

  test "a 304 refresh renews freshness and serves the stored body":
    let s = newCacheStore()
    s.storeResponse(req(GET, "http://x/g"),
      resp(200, {"cache-control": "max-age=0", "etag": "\"v1\""}, "stored"))
    check s.lookup(req(GET, "http://x/g")).kind == fStale
    let served = s.refreshOn304(req(GET, "http://x/g"),
      resp(304, {"cache-control": "max-age=60", "etag": "\"v1\""}, ""))
    check served.body == "stored"
    check s.lookup(req(GET, "http://x/g")).kind == fFresh   # renewed

suite "rate-limit core (token bucket)":
  test "burst tokens go through immediately, then requests are paced":
    let b = newTokenBucket(perSec = 100, burst = 2)
    check b.take() == 0            # burst 1
    check b.take() == 0            # burst 2
    check b.take() > 0             # empty: must wait (~10ms at 100/s)

  test "perSec 0 means unlimited":
    let b = newTokenBucket(perSec = 0)
    for _ in 0 ..< 5: check b.take() == 0

# --- end-to-end cache middleware on the sync client ---------------------------

suite "cache middleware (sync, end to end)":
  test "a fresh response is served from cache without a second request":
    var count = 0
    var c = CacheSrv(port: 9110, count: addr count, requests: 1, maxAge: 300)
    var th: Thread[CacheSrv]
    startCache(th, c)
    var cfg = initNaviConfig()
    cfg.middleware = @[cache()]
    let api = newNavi(cfg)
    let url = "http://127.0.0.1:9110/x"
    check api.get(url).body == "payload"
    check api.get(url).body == "payload"     # served from cache; no 2nd connection
    joinThread(th)
    check count == 1

  test "a stale response is revalidated and refreshed on 304":
    var count = 0
    var c = CacheSrv(port: 9111, count: addr count, requests: 2, maxAge: 0, etag: "\"v1\"")
    var th: Thread[CacheSrv]
    startCache(th, c)
    var cfg = initNaviConfig()
    cfg.middleware = @[cache()]
    let api = newNavi(cfg)
    let url = "http://127.0.0.1:9111/x"
    check api.get(url).body == "payload"     # stored (stale: max-age=0 + ETag)
    let r2 = api.get(url)                     # revalidates -> 304 -> serve stored
    check r2.status == 200
    check r2.body == "payload"
    joinThread(th)
    check count == 2

  test "a no-store response is not cached":
    var count = 0
    var c = CacheSrv(port: 9112, count: addr count, requests: 2, noStore: true)
    var th: Thread[CacheSrv]
    startCache(th, c)
    var cfg = initNaviConfig()
    cfg.middleware = @[cache()]
    let api = newNavi(cfg)
    let url = "http://127.0.0.1:9112/x"
    discard api.get(url)
    discard api.get(url)                      # not cached: both hit the server
    joinThread(th)
    check count == 2

suite "sync factory instantiation":
  test "every sync factory builds a NaviMiddleware":
    var cfg = initNaviConfig()
    cfg.middleware = @[
      cache(), rateLimit(10), concurrency(4),
      bearer("t"), basic("u", "p"), logging()]
    check cfg.middleware.len == 6
