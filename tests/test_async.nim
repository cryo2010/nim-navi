## End-to-end test of the asyncdispatch entry module.

import unittest
import std/[asyncdispatch, strutils]
import navi/asyncdispatch
import navi/core/pool      # for pool.idleCount in the streaming lifecycle tests
import ./support

suite "asyncdispatch entry end to end":
  test "the async client should return a parsed response for a GET to localhost":
    const port = 8972
    var th: Thread[ServerCtx]
    startServer(th, port)

    let api = newNavi()
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.ok
    check res.data["ok"].getBool()
    joinThread(th)

  test "the async client should retry with async backoff and then succeed":
    const port = 8992
    var th: Thread[ServerCtx]
    startRetry(th, port, failures = 1)

    let api = newNavi()
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.body == "recovered"
    joinThread(th)

  test "cancel should abort an in-flight request":
    const port = 8968
    var th: Thread[ServerCtx]
    startHang(th, port)  # accepts, reads the request, never replies

    let api = newNavi()
    proc run(): Future[void] {.async.} =
      let tok = newCancelToken()
      let f = api.get("http://127.0.0.1:" & $port & "/", cancel = tok)
      await sleepAsync(50)                # let the request reach the hung server
      tok.cancel()
      discard await f
    var msg = ""
    try:
      waitFor run()
    except RequestCancelledError as e: msg = e.msg
    check "cancelled" in msg
    joinThread(th)

  test "closure middleware should capture config and modify the request":
    const port = 8967
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)  # echoes each request header back as x-echo-<name>

    proc bearerMw(token: string): NaviMiddleware =
      result = proc(ctx: NaviContext) {.async.} =
        ctx.req.headers["authorization"] = "Bearer " & token  # captures token
        await ctx.next()

    var cfg = initNaviConfig()
    cfg.middleware = @[bearerMw("captured-42")]
    let api = newNavi(cfg)
    let res = waitFor api.post("http://127.0.0.1:" & $port & "/", body = "x")
    check res.headers.get("x-echo-authorization") == "Bearer captured-42"
    joinThread(th)

  test "stream should expose headers before the body and deliver it via each":
    const port = 8998
    var th: Thread[ServerCtx]
    startServer(th, port)  # responds with {"ok":true}

    let api = newNavi()
    proc run(): Future[(int, string)] {.async.} =
      let res = await api.stream(GET, "http://127.0.0.1:" & $port & "/")
      var collected = ""
      res.each(chunk): collected.add chunk
      return (res.status, collected)     # status is read before the body was drained
    let (status, body) = waitFor run()
    check status == 200
    check body == """{"ok":true}"""
    joinThread(th)

  test "stream should return the connection to the pool after a full drain":
    const port = 8999
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 2, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    proc run(): Future[(string, int, string)] {.async.} =
      var got = ""
      let res = await api.stream(GET, key & "/")
      res.each(chunk): got.add chunk
      let idle = api.pool.idleCount(key)          # returned after a full drain
      let second = await api.get(key & "/")       # ...and reused
      return (got, idle, second.body)
    let (got, idle, second) = waitFor run()
    check got == "n=0"
    check idle == 1
    check second == "n=1"
    joinThread(th)
    check accepts == 1                            # both requests used the one connection

  test "readChunk should deliver the body in order and end by pooling the connection":
    const port = 9002
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 2, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    proc run(): Future[(string, int, string)] {.async.} =
      var body = ""
      let res = await api.stream(GET, key & "/")
      while true:                                 # break-friendly pull loop
        let c = await res.readChunk()
        if c.len == 0: break
        body.add c
      let idle = api.pool.idleCount(key)          # returned after a full read
      let second = await api.get(key & "/")       # ...and reused
      return (body, idle, second.body)
    let (body, idle, second) = waitFor run()
    check body == "n=0"
    check idle == 1
    check second == "n=1"
    joinThread(th)
    check accepts == 1

  test "stream should close (not pool) the connection when the drain fails":
    const port = 9001
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 1, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    proc run(): Future[(bool, int)] {.async.} =
      var raised = false
      let res = await api.stream(GET, key & "/")
      try:
        res.each(chunk): raise newException(ValueError, "consumer failed")
      except ValueError: raised = true
      return (raised, api.pool.idleCount(key))
    let (raised, idle) = waitFor run()
    check raised                                  # the error propagates out of each
    check idle == 0                               # a failed drain closes, never pools
    joinThread(th)
