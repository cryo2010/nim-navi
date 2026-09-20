## End-to-end test of the chronos entry module.

import unittest
import std/[strutils, json]
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
      let res = await api.stream.get("http://127.0.0.1:" & $port & "/")
      var collected = ""
      res.each(chunk): collected.add chunk
      return (res.status, collected)     # status is read before the body was drained
    let (status, body) = waitFor run(newNavi())
    check status == 200
    check body == """{"ok":true}"""
    joinThread(th)

  test "stream should return the connection to the pool after a full drain":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 2, accepts = addr accepts)

    proc run(api: Navi, key: string): Future[(string, int, string)] {.async.} =
      var got = ""
      let res = await api.stream.get(key & "/")
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
    const port = 9265
    var th: Thread[ServerCtx]
    startTruncated(th, port, bodyBytes = 10)
    var cfg = initNaviConfig()
    cfg.retry.limit = 0
    let api = newNavi(cfg)
    expect IOError:
      discard waitFor api.get("http://127.0.0.1:" & $port & "/")
    joinThread(th)

  test "a streaming request raises (not hangs) on a premature close mid-body":
    const port = 9266
    var th: Thread[ServerCtx]
    startTruncated(th, port, bodyBytes = 10)
    proc run(api: Navi): Future[(int, bool)] {.async.} =
      # verb-as-argument full-control form (the `api.stream.get` view is sugar over it):
      # kept here so the underlying `stream(client, verb, ...)` layer keeps runtime coverage.
      let handle = await api.stream(GET, "http://127.0.0.1:" & $port & "/")
      let st = handle.status
      var raised = false
      try:
        handle.each(chunk): discard                     # must raise, not spin the loop
      except CatchableError: raised = true
      return (st, raised)
    let (st, raised) = waitFor run(newNavi())
    check st == 200
    check raised
    joinThread(th)

  test "stream should close (not pool) the connection when the drain fails":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 1, accepts = addr accepts)

    proc run(api: Navi, key: string): Future[(bool, int)] {.async.} =
      var raised = false
      let res = await api.stream.get(key & "/")
      try:
        res.each(chunk): raise newException(ValueError, "consumer failed")
      except ValueError: raised = true
      return (raised, api.pool.idleCount(key))
    let (raised, idle) = waitFor run(newNavi(), "http://127.0.0.1:" & $port)
    check raised                                  # the error propagates out of each
    check idle == 0                               # a failed drain closes, never pools
    joinThread(th)

  test "cancel should abort an in-flight request":
    var port = 0
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

  test "a closure-iterator body should stream and reassemble over chronos":
    const port = 9250
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

  test "a catch-all object body should be JSON over chronos":
    const port = 9251
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)
    let res = waitFor newNavi().post("http://127.0.0.1:" & $port & "/",
                                     body = (name: "ada", age: 36))
    check res.body == """{"name":"ada","age":36}"""
    check res.headers.get("x-echo-content-type") == "application/json"
    joinThread(th)

  test "an async body producer should stream a chunked upload (awaiting between chunks)":
    const port = 9252
    var th: Thread[ServerCtx]
    startUploadEcho(th, port)
    proc run(): Future[Response] {.async.} =
      # `parts`/`idx` are locals of `run` (a ref for the counter): a chronos async
      # proc rejects capturing a suite-scope global as "not GC-safe".
      let parts = @["alpha ", "beta ", "gamma"]
      let idx = new int
      proc getChunks(): Future[string] {.async.} =
        if idx[] >= parts.len: return ""
        let p = parts[idx[]]
        inc idx[]
        await sleepAsync(1.milliseconds)   # producing a chunk itself awaits
        return p
      return await newNavi().put("http://127.0.0.1:" & $port & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 200
    check res.body == "alpha beta gamma"
    joinThread(th)

  test "an async producer should pipe a streaming download into an upload":
    const srcPort = 9253
    const dstPort = 9254
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
    let url = "http://127.0.0.1:" & $port & "/"
    proc run(u: string): Future[Response] {.async.} =
      let idx = new int
      proc getChunks(): Future[string] {.async.} =
        if idx[] >= 2: return ""
        inc idx[]
        return "x"
      var cfg = initNaviConfig()
      cfg.throwHttpErrors = false
      return await newNavi(cfg).put(u, body = getChunks)
    let res = waitFor run(url)
    check res.status == 503
    check count == 1
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
