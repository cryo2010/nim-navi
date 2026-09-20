## End-to-end test of the asyncdispatch entry module.

import unittest
import std/[asyncdispatch, strutils, json]
import navi/asyncdispatch
import navi/core/pool      # for pool.idleCount in the streaming lifecycle tests
import navi/core/response as naviresp  # navi's TimeoutError (std/net also defines one)
import std/[monotimes, times]
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
    waitFlag(addr closed1)                              # server closed the pooled conn

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
      # verb-as-argument full-control form (the `api.stream.get` view is sugar over it):
      # kept here so the underlying `stream(client, verb, ...)` layer keeps runtime coverage.
      let handle = await api.stream(GET, "http://127.0.0.1:" & $port & "/")
      check handle.status == 200
      try:
        handle.each(chunk): discard                     # must raise, not spin the loop
      except IOError: return true
      return false
    check waitFor run()
    joinThread(th)

  test "stream open is bounded by totalMs (a wedged server trips TimeoutError, #358)":
    # The server accepts and reads the request but never sends response headers, so
    # openStreamConn would block reading them forever. With totalMs configured, the
    # OPEN phase must be guarded (as the buffered path is) and trip TimeoutError near
    # the bound instead of hanging on the server's ~600ms hold.
    var port = 0
    var th: Thread[ServerCtx]
    startHang(th, port)                    # accepts, reads the request, never replies
    var cfg = initNaviConfig()
    cfg.timeouts.total = 150               # tight total deadline
    let api = newNavi(cfg)
    proc run(): Future[(bool, int)] {.async.} =
      let t0 = getMonoTime()
      var raised = false
      try:
        discard await api.stream.get("http://127.0.0.1:" & $port & "/")
      except naviresp.TimeoutError:
        raised = true
      return (raised, (getMonoTime() - t0).inMilliseconds.int)
    let (raised, elapsed) = waitFor run()
    check raised                           # bounded, not a forever-hang
    check elapsed < 500                    # fired near the 150ms bound, not the 600ms hold
    joinThread(th)

  test "a per-attempt timeout is retried while the total budget allows it (#375)":
    # Exercises the async per-attempt guard: each attempt stalls (server never
    # replies), the inner guard fires at ~attempt ms, and because total is unbounded
    # and GET is retryable, the timeout is retried up to the limit -- a fresh
    # connection each time. The server therefore sees limit+1 attempts, then the
    # final per-attempt TimeoutError propagates (an attempt timeout is retryable,
    # unlike the terminal outer `total` guard).
    var port = 0
    var count = 0
    var th: Thread[ServerCtx]
    startHangCount(th, port, conns = 3, count = addr count)

    var cfg = initNaviConfig()
    cfg.retry.limit = 2
    cfg.timeouts.attempt = 150
    cfg.timeouts.total = 0
    let api = newNavi(cfg)
    proc run(): Future[bool] {.async.} =
      try:
        discard await api.get("http://127.0.0.1:" & $port & "/")
      except naviresp.TimeoutError:
        return true
      return false
    check waitFor run()
    check count == 3                       # retried per attempt: limit(2) + 1
    joinThread(th)

  test "stream should expose headers before the body and deliver it via each":
    const port = 9204
    var th: Thread[ServerCtx]
    startServer(th, port)  # responds with {"ok":true}

    let api = newNavi()
    proc run(): Future[(int, string)] {.async.} =
      let res = await api.stream.get("http://127.0.0.1:" & $port & "/")
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
      let res = await api.stream.get(key & "/")
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
      let res = await api.stream.get(key & "/")
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
      let res = await api.stream.get(key & "/")
      try:
        res.each(chunk): raise newException(ValueError, "consumer failed")
      except ValueError: raised = true
      return (raised, api.pool.idleCount(key))
    let (raised, idle) = waitFor run()
    check raised                                  # the error propagates out of each
    check idle == 0                               # a failed drain closes, never pools
    joinThread(th)

  test "a closure-iterator body should stream and reassemble over async":
    const port = 9209
    var th: Thread[ServerCtx]
    startUploadEcho(th, port)
    let parts = @["hello ", "", "streaming ", "world"]
    proc run(): Future[Response] {.async.} =
      let it = iterator (): string {.closure.} =
        for p in parts: yield p
      return await newNavi().request(POST, "http://127.0.0.1:" & $port & "/", body = it)
    let res = waitFor run()
    check res.status == 200
    check res.body == "hello streaming world"
    joinThread(th)

  test "a catch-all object body should be JSON over async":
    const port = 9210
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)
    let res = waitFor newNavi().post("http://127.0.0.1:" & $port & "/",
                                     body = (name: "ada", age: 36))
    check res.body == """{"name":"ada","age":36}"""
    check res.headers.get("x-echo-content-type") == "application/json"
    joinThread(th)

  test "an async body producer should stream a chunked upload (awaiting between chunks)":
    const port = 9220
    var th: Thread[ServerCtx]
    startUploadEcho(th, port)
    proc run(): Future[Response] {.async.} =
      let parts = @["alpha ", "beta ", "gamma"]
      var i = 0
      proc getChunks(): Future[string] {.async.} =
        if i >= parts.len: return ""
        let p = parts[i]
        inc i
        await sleepAsync(1)          # producing a chunk itself awaits
        return p
      return await newNavi().put("http://127.0.0.1:" & $port & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 200
    check res.body == "alpha beta gamma"
    joinThread(th)

  test "an async producer should pipe a streaming download into an upload":
    # The flagship pipe: stream() a body from one server and feed it, chunk by chunk,
    # into a put() on another, so a download becomes an upload in constant memory.
    const srcPort = 9221
    const dstPort = 9222
    var srcTh, dstTh: Thread[ServerCtx]
    let payload = "the quick brown fox jumps over the lazy dog"
    startRaw(srcTh, srcPort, "HTTP/1.1 200 OK\r\nContent-Length: " & $payload.len &
             "\r\nConnection: close\r\n\r\n" & payload)
    startUploadEcho(dstTh, dstPort)
    proc run(): Future[Response] {.async.} =
      let api = newNavi()
      let sr = await api.stream.get("http://127.0.0.1:" & $srcPort & "/")
      return await api.put("http://127.0.0.1:" & $dstPort & "/",
        body = proc(): Future[string] {.async.} = return await sr.readChunk())
    let res = waitFor run()
    check res.status == 200
    check res.body == payload
    joinThread(srcTh)
    joinThread(dstTh)

  test "an async producer PUT should not be retried (non-replayable body)":
    var port = 0
    var count = 0
    var th: Thread[ServerCtx]
    start503Once(th, port, addr count)
    proc run(): Future[Response] {.async.} =
      var i = 0
      proc getChunks(): Future[string] {.async.} =
        if i >= 2: return ""
        inc i
        return "x"
      # PUT is retryable by method and 503 is a retryable status, but a streamed
      # (non-rewindable) body must be sent once and never replayed: the client
      # returns the 503, and the server sees exactly one request.
      var cfg = initNaviConfig()
      cfg.throwHttpErrors = false
      return await newNavi(cfg).put("http://127.0.0.1:" & $port & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 503
    check count == 1
    joinThread(th)
