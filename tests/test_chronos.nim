## End-to-end test of the chronos entry module.

import unittest
import std/strutils
import pkg/chronos
import navi/chronos
import navi/core/pool      # for pool.idleCount in the streaming lifecycle tests
import ./support

suite "chronos entry end to end":
  test "the chronos client should return a parsed response for a GET to localhost":
    const port = 9212
    var th: Thread[ServerCtx]
    startServer(th, port)

    let api = newNavi()
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.ok
    check res.data["ok"].getBool()
    joinThread(th)

  test "stream should expose headers before the body and deliver it via each":
    const port = 9213
    var th: Thread[ServerCtx]
    startServer(th, port)  # responds with {"ok":true}

    # `api`/`key` are passed as parameters (not captured): chronos's async macro
    # rejects an async proc that closes over a GC'd local as "not GC-safe".
    proc run(api: Navi): Future[(int, string)] {.async.} =
      let res = await api.stream(GET, "http://127.0.0.1:" & $port & "/")
      var collected = ""
      res.each(chunk): collected.add chunk
      return (res.status, collected)     # status is read before the body was drained
    let (status, body) = waitFor run(newNavi())
    check status == 200
    check body == """{"ok":true}"""
    joinThread(th)

  test "stream should return the connection to the pool after a full drain":
    const port = 9214
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 2, accepts = addr accepts)

    proc run(api: Navi, key: string): Future[(string, int, string)] {.async.} =
      var got = ""
      let res = await api.stream(GET, key & "/")
      res.each(chunk): got.add chunk
      let idle = api.pool.idleCount(key)          # returned after a full drain
      let second = await api.get(key & "/")       # ...and reused
      return (got, idle, second.body)
    let (got, idle, second) = waitFor run(newNavi(), "http://127.0.0.1:" & $port)
    check got == "n=0"
    check idle == 1
    check second == "n=1"
    joinThread(th)
    check accepts == 1                            # both requests used the one connection

  test "stream should close (not pool) the connection when the drain fails":
    const port = 9215
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 1, accepts = addr accepts)

    proc run(api: Navi, key: string): Future[(bool, int)] {.async.} =
      var raised = false
      let res = await api.stream(GET, key & "/")
      try:
        res.each(chunk): raise newException(ValueError, "consumer failed")
      except ValueError: raised = true
      return (raised, api.pool.idleCount(key))
    let (raised, idle) = waitFor run(newNavi(), "http://127.0.0.1:" & $port)
    check raised                                  # the error propagates out of each
    check idle == 0                               # a failed drain closes, never pools
    joinThread(th)

  test "cancel should abort an in-flight request":
    const port = 9211
    var th: Thread[ServerCtx]
    startHang(th, port)  # accepts, reads the request, never replies

    proc run(api: Navi, url: string): Future[void] {.async.} =
      let tok = newCancelToken()
      let f = api.get(url, cancel = tok)
      await sleepAsync(50.milliseconds)  # let the request reach the hung server
      tok.cancel()
      discard await f
    var msg = ""
    try:
      waitFor run(newNavi(), "http://127.0.0.1:" & $port & "/")
    except RequestCancelledError as e: msg = e.msg
    check "cancelled" in msg
    joinThread(th)

  test "closure middleware should capture config and modify the request":
    const port = 9210
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)  # echoes each request header back as x-echo-<name>

    proc bearerMw(token: string): NaviMiddleware =
      result = proc(ctx: NaviContext) {.async.} =        # plain {.async.}, no raises spec
        ctx.req.headers["authorization"] = "Bearer " & token  # captures token
        await ctx.next()

    var cfg = initNaviConfig()
    cfg.middleware = @[bearerMw("captured-42")]
    let api = newNavi(cfg)
    let res = waitFor api.post("http://127.0.0.1:" & $port & "/", body = "x")
    check res.headers.get("x-echo-authorization") == "Bearer captured-42"
    joinThread(th)

suite "chronos TLS config":
  # chronos now runs OpenSSL, so cipher selection and TLS 1.3 are honored rather
  # than rejected as they were under BearSSL. (Real negotiation is covered by the
  # TLS parity tests against a live server.)
  test "the chronos backend should surface an invalid cipher from OpenSSL":
    var cfg = initNaviConfig()
    cfg.tls.ciphers = "NO-SUCH-CIPHER"
    let api = newNavi(cfg)
    var msg = ""
    try:
      discard waitFor api.get("https://127.0.0.1:1/")
    except CatchableError as e: msg = e.msg
    check "cipher" in msg   # reached OpenSSL's cipher-list check, not a blanket reject

  test "the chronos backend should no longer reject tls13":
    var cfg = initNaviConfig()
    cfg.tls.minVersion = tls13
    let api = newNavi(cfg)
    var msg = ""
    try:
      discard waitFor api.get("https://127.0.0.1:1/")
    except CatchableError as e: msg = e.msg
    # a refused connection to :1, not the old "tls13 is unavailable" rejection
    check "tls13 is unavailable" notin msg
