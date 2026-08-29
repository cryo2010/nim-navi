## End-to-end test of the asyncdispatch entry module.

import unittest
import std/[asyncdispatch, strutils]
import navi/asyncdispatch
import navi/core/pool      # for pool.idleCount in the streaming lifecycle tests
import ./support

suite "asyncdispatch entry end to end":
  test "the async client should return a parsed response for a GET to localhost":
    const port = 9202
    var th: Thread[ServerCtx]
    startServer(th, port)

    let api = newNavi()
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.ok
    check res.data["ok"].getBool()
    joinThread(th)

  test "the async client should retry with async backoff and then succeed":
    const port = 9203
    var th: Thread[ServerCtx]
    startRetry(th, port, failures = 1)

    let api = newNavi()
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.body == "recovered"
    joinThread(th)

  test "cancel should abort an in-flight request":
    var port = 0
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
    const port = 9200
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

  test "a non-idempotent request is replayed on a fresh connection when the pooled one was closed before any response":
    var port = 0
    var accepts = 0
    var closed1 = false
    var th: Thread[StaleCtx]
    startStalePooled(th, port, addr closed1, addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    check (waitFor api.get(key & "/")).status == 200   # conn 1, then pooled
    check api.pool.idleCount(key) == 1
    while not closed1: discard                          # server closed the pooled conn

    let r = waitFor api.request(POST, key & "/submit", body = "data")
    check r.status == 200
    check r.body == "replayed:data"                    # served on the fresh connection
    joinThread(th)
    check accepts == 2

  test "a buffered request raises on a premature close mid-body":
    const port = 9263
    var th: Thread[ServerCtx]
    startTruncated(th, port, bodyBytes = 10)           # declares CL 100, sends 10, closes
    var cfg = initNaviConfig()
    cfg.retry.limit = 0
    let api = newNavi(cfg)
    expect IOError:
      discard waitFor api.get("http://127.0.0.1:" & $port & "/")
    joinThread(th)

  test "a streaming request raises (not hangs) on a premature close mid-body":
    const port = 9264
    var th: Thread[ServerCtx]
    startTruncated(th, port, bodyBytes = 10)
    let api = newNavi()
    proc run(): Future[bool] {.async.} =
      let handle = await api.stream(GET, "http://127.0.0.1:" & $port & "/")
      check handle.status == 200
      try:
        handle.each(chunk): discard                     # must raise, not spin the loop
      except IOError: return true
      return false
    check waitFor run()
    joinThread(th)

  test "stream should expose headers before the body and deliver it via each":
    const port = 9204
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
    var port = 0
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
    var port = 0
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

  test "sse should parse events over the async backend":
    const port = 9208
    let payload = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n" &
                  "Connection: close\r\n\r\nevent: e\ndata: a\n\ndata: b\nid: 3\n\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)
    proc run(): Future[seq[SseEvent]] {.async.} =
      let s = await newNavi().sse("http://127.0.0.1:" & $port & "/", reconnect = false)
      var events: seq[SseEvent]
      s.each(ev): events.add ev
      return events
    let events = waitFor run()
    joinThread(th)
    check events.len == 2
    check events[0].event == "e" and events[0].data == "a"
    check events[1].data == "b" and events[1].id == "3"

  test "stream should close (not pool) the connection when the drain fails":
    var port = 0
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
