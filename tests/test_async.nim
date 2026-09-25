## End-to-end test of the asyncdispatch entry module.

import unittest
import std/[asyncdispatch, strutils, json]
import navi/asyncdispatch
import navi/core/pool      # for pool.idleCount in the streaming lifecycle tests
import navi/core/response as naviresp  # navi's TimeoutError (std/net also defines one)
import std/[monotimes, times]
import ./support
from navi/proto/h1 import h1CoalesceSize  # the streamed-upload write-buffer cap

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

  test "an idempotent request is replayed on a fresh connection when the pooled one was closed before any response":
    # The classic keep-alive race: a pooled connection the server closed while idle is
    # reused and dropped before any response. An idempotent method (PUT) is safe to
    # replay, so it is re-sent on a fresh connection (matching Go net/http).
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

    let r = waitFor api.put(key & "/submit", body = "data")
    check r.status == 200
    check r.body == "replayed:data"                    # served on the fresh connection
    joinThread(th)
    check accepts == 2

  test "a non-idempotent request with an Idempotency-Key is replayed on a dropped fresh connection":
    # A pre-response drop is ambiguous, so a non-idempotent POST is replayed only when
    # the caller vouches safety with an Idempotency-Key (Go net/http's escape hatch).
    var port = 0
    var accepts = 0
    var closed1 = false
    var th: Thread[StaleCtx]
    startFreshDrop(th, port, addr closed1, addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    var h = initHeaders()
    h["idempotency-key"] = "req-1"
    let r = waitFor api.request(POST, key & "/submit", headers = h, body = "data")
    check r.status == 200
    check r.body == "replayed:data"                    # served on the second connection
    joinThread(th)
    check accepts == 2                                  # first (fresh) dropped, retry served

  test "a non-idempotent request WITHOUT an Idempotency-Key is not replayed (at-most-once)":
    # The default: a POST whose connection drops before any response is NOT auto-retried
    # (it may already have been processed). The server serves exactly one connection and
    # stops listening, so a wrongful replay would fail to connect rather than hang.
    var port = 0
    var accepts = 0
    var th: Thread[StaleCtx]
    startDropOnce(th, port, addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    expect naviresp.KeepAliveRaceError:
      discard waitFor api.request(POST, key & "/submit", body = "data")
    joinThread(th)
    check accepts == 1                                  # sent once, never replayed

  test "a streamed non-idempotent request is not replayed on a stale pooled connection (at-most-once)":
    # The streaming path (openStreamConn) must honor the same at-most-once rule as the
    # buffered path: a POST whose reused pooled connection dropped before any response is
    # NOT replayed. Previously the streaming pooled path replayed any rewindable body.
    var port = 0
    var accepts = 0
    var closed1 = false
    var th: Thread[StaleCtx]
    startStaleNoRetry(th, port, addr closed1, addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    check (waitFor api.get(key & "/")).status == 200    # conn 1, then pooled
    waitFlag(addr closed1)                              # server closed the pooled conn
    expect naviresp.KeepAliveRaceError:
      discard waitFor api.stream(POST, key & "/submit")
    joinThread(th)
    check accepts == 1                                  # sent once, never replayed

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

  test "an async producer's tiny chunks should coalesce into few wire chunks (#299)":
    const port = 9223
    var th: Thread[ServerCtx]
    var chunks = 0
    startUploadEcho(th, port, addr chunks)
    proc run(): Future[Response] {.async.} =
      let left = new int
      left[] = 1000
      proc getChunks(): Future[string] {.async.} =
        if left[] <= 0: return ""
        dec left[]
        return "0123456789"
      return await newNavi().put("http://127.0.0.1:" & $port & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 200
    check res.body == "0123456789".repeat(1000)
    joinThread(th)
    check chunks == 1     # 10 KB of tiny chunks -> one buffered write

  test "the coalescing buffer should never be framed above h1CoalesceSize (#299)":
    const port = 9224
    var th: Thread[ServerCtx]
    var chunks = 0
    var biggest = 0
    startUploadEcho(th, port, addr chunks, addr biggest)
    let half = "y".repeat(h1CoalesceSize - 1)
    proc run(): Future[Response] {.async.} =
      let left = new int
      left[] = 2
      proc getChunks(): Future[string] {.async.} =
        if left[] <= 0: return ""
        dec left[]
        return half
      return await newNavi().put("http://127.0.0.1:" & $port & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 200
    check res.body == half & half
    joinThread(th)
    # The second chunk flushes the first BEFORE appending, so neither frame exceeds
    # the cap (one frame of 2 * (16 KiB - 1) would).
    check chunks == 2
    check biggest == h1CoalesceSize - 1

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

  test "a 303 that drops a streamed upload should send the next hop bodiless (#395)":
    # The rewrite makes the hop a plain GET, so the async producer must be dropped
    # with the body: the hop goes out with no Transfer-Encoding, no Content-Length
    # and no bytes, not as a chunked GET with an already-empty producer.
    const port = 9225
    var th: Thread[ServerCtx]
    startUploadRedirect(th, port, 303)
    proc run(): Future[Response] {.async.} =
      let parts = @["alpha ", "beta ", "gamma"]
      var i = 0
      proc getChunks(): Future[string] {.async.} =
        if i >= parts.len: return ""
        let p = parts[i]
        inc i
        return p
      return await newNavi().put("http://127.0.0.1:" & $port & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 200
    check res.body == "arrived"
    check res.headers.get("x-echo-hop1-body") == "alpha beta gamma"
    check res.headers.get("x-echo-method") == "GET"
    check res.headers.get("x-echo-transfer-encoding") == ""
    check res.headers.get("x-echo-content-length") == ""
    check res.headers.get("x-echo-hop2-bytes") == "0"
    joinThread(th)

  test "a 307 must not replay a streamed upload: the 3xx is surfaced (#295)":
    const port = 9226
    var th: Thread[ServerCtx]
    startUploadRedirect(th, port, 307)
    proc run(): Future[Response] {.async.} =
      let parts = @["alpha ", "beta ", "gamma"]
      var i = 0
      proc getChunks(): Future[string] {.async.} =
        if i >= parts.len: return ""
        let p = parts[i]
        inc i
        return p
      var cfg = initNaviConfig()
      cfg.throwHttpErrors = false
      return await newNavi(cfg).put("http://127.0.0.1:" & $port & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 307             # preserved body + spent producer: not followed
    check res.headers.get("location") == "/final"
    joinThread(th)

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

suite "asyncdispatch Expect: 100-continue gate (#392)":
  test "an expect-gated upload should wait for the 100 and then send the body":
    var port = 0
    var sawExpect = false
    var bodyLen = 0
    var th: Thread[ExpectCtx]
    startExpect(th, port, emSend100, addr sawExpect, addr bodyLen)
    let key = "http://127.0.0.1:" & $port

    proc run(): Future[(Response, int)] {.async.} =
      let api = newNavi()
      api.config.expectContinueMs = 2000
      let parts = @["hello ", "expect ", "world"]
      let idx = new int
      proc getChunks(): Future[string] {.async.} =
        if idx[] >= parts.len: return ""
        let p = parts[idx[]]
        inc idx[]
        return p
      let r = await api.request(POST, key & "/", body = getChunks)
      return (r, api.pool.idleCount(key))
    let (res, idle) = waitFor run()
    check res.status == 200
    check res.body == "hello expect world"
    joinThread(th)
    check sawExpect
    check bodyLen == "hello expect world".len
    check idle == 1

  test "a final status before the body should withhold it and never pull the producer":
    var port = 0
    var sawExpect = false
    var bodyLen = -1
    var th: Thread[ExpectCtx]
    startExpect(th, port, emReject, addr sawExpect, addr bodyLen)
    let key = "http://127.0.0.1:" & $port

    proc run(): Future[(Response, int, int)] {.async.} =
      let api = newNavi()
      api.config.expectContinueMs = 2000
      api.config.throwHttpErrors = false
      let pulls = new int
      proc getChunks(): Future[string] {.async.} =
        inc pulls[]
        return "never sent"
      let r = await api.request(POST, key & "/", body = getChunks)
      return (r, pulls[], api.pool.idleCount(key))
    let (res, pulls, idle) = waitFor run()
    check res.status == 413
    check res.body == "too large"
    joinThread(th)
    check sawExpect
    check pulls == 0
    check bodyLen == 0
    check idle == 0              # the peer may still expect the body: never pooled

  test "a silent server should get the body once expectContinueMs lapses":
    # Also the asyncdispatch parked-read test: the gate's abandoned read cannot be
    # cancelled, so it is parked and must hand the 200 to the read that follows the
    # body send instead of swallowing it.
    var port = 0
    var sawExpect = false
    var bodyLen = 0
    var th: Thread[ExpectCtx]
    startExpect(th, port, emIgnore, addr sawExpect, addr bodyLen)
    let key = "http://127.0.0.1:" & $port

    proc run(): Future[Response] {.async.} =
      let api = newNavi()
      api.config.expectContinueMs = 150
      let sent = new bool
      proc getChunks(): Future[string] {.async.} =
        if sent[]: return ""
        sent[] = true
        return "sent anyway"
      return await api.request(POST, key & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 200
    check res.body == "sent anyway"
    joinThread(th)
    check sawExpect
    check bodyLen == "sent anyway".len

  test "a 100 arriving after the gate expired should be discarded, not read as final":
    var port = 0
    var bodyLen = 0
    var th: Thread[ExpectCtx]
    startExpect(th, port, emLate100, nil, addr bodyLen)
    let key = "http://127.0.0.1:" & $port

    proc run(): Future[Response] {.async.} =
      let api = newNavi()
      api.config.expectContinueMs = 100
      let sent = new bool
      proc getChunks(): Future[string] {.async.} =
        if sent[]: return ""
        sent[] = true
        return "late continue"
      return await api.request(POST, key & "/", body = getChunks)
    let res = waitFor run()
    check res.status == 200
    check res.body == "late continue"
    joinThread(th)
    check bodyLen == "late continue".len

  test "a bodyless request should never carry the Expect header":
    var port = 0
    var sawExpect = true
    var th: Thread[ExpectCtx]
    startExpect(th, port, emIgnore, addr sawExpect)

    let api = newNavi()
    api.config.expectContinueMs = 2000
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    joinThread(th)
    check not sawExpect
