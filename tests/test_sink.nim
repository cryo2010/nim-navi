## Response-body sink on the buffered `request()` (sync backend). The gated sink
## streams ONLY the final surfaced response's body; redirect/retry/digest/thrown
## bodies never reach it. Async/chronos mirrors live in test_sink_async.nim and
## test_sink_chronos.nim via the shared sink_spec.nim.

import unittest
import std/[strutils, net]
import navi
import navi/core/response as naviresp   # navi's HttpError/ResponseTooLargeError
import ./support

suite "sync response sink (request)":
  test "a void sink receives the full body and leaves res.body empty":
    const port = 9701
    var th: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    let api = newNavi()
    var got = ""
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      got.add data; true
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 200
    check res.body == ""
    check got == "Hello, chunked world!"
    check res.trailers.get("x-checksum") == "done"    # trailers on a full drain
    joinThread(th)

  test "delivery rule: a redirect hop body is not delivered, only the final":
    const port = 9702
    var th: Thread[ServerCtx]
    startRedirect(th, port)          # 302 -> /final (body "") then 200 "arrived"
    let api = newNavi()
    var got = ""
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      got.add data; true
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 200
    check got == "arrived"           # only the final response body
    joinThread(th)

  test "delivery rule: a retried 503 body is not delivered, only the 200":
    const port = 9703
    var th: Thread[ServerCtx]
    startRetry(th, port, failures = 1)   # 503 (body "") then 200 "recovered"
    let api = newNavi()
    var got = ""
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      got.add data; true
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 200
    check got == "recovered"
    joinThread(th)

  test "delivery rule: a digest 401 challenge body is not delivered, the protected body is":
    const port = 9704
    var th: Thread[ServerCtx]
    startDigestThenBody(th, port, "secret-data")
    var cfg = initNaviConfig()
    cfg.auth = digestAuth("user", "pass")
    let api = newNavi(cfg)
    var got = ""
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      got.add data; true
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 200
    check got == "secret-data"        # never the challenge body
    joinThread(th)

  test "early stop: a false return truncates and the client stays usable":
    const port = 9705
    var th: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    let api = newNavi()
    var chunks = 0
    var got = ""
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      inc chunks; got.add data; false      # stop after the first chunk
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 200
    check res.bodyTruncated
    check res.body == ""
    check chunks == 1
    check res.trailers.len == 0            # trailers absent on an early stop
    joinThread(th)
    # a subsequent request on the same client succeeds (connection state sane)
    const port2 = 9706
    var th2: Thread[ServerCtx]
    startServer(th2, port2)
    let res2 = api.get("http://127.0.0.1:" & $port2 & "/")
    check res2.status == 200
    check res2.data["ok"].getBool()
    joinThread(th2)

  test "throwHttpErrors: a non-2xx never calls the sink and HttpError carries the body":
    const port = 9707
    var th: Thread[ServerCtx]
    startStatusBody(th, port, 404, "not found here")
    let api = newNavi()
    var called = false
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      called = true; true
    var raised = false
    try:
      discard api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    except HttpError as e:
      raised = true
      check e.response.body == "not found here"
    check raised
    check not called
    joinThread(th)

  test "maxResponseBytes breach raises through the sinked path":
    const port = 9708
    var th: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    var cfg = initNaviConfig()
    cfg.maxResponseBytes = 5             # "Hello, chunked world!" exceeds this
    let api = newNavi(cfg)
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} = true
    var raised = false
    try:
      discard api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    except ResponseTooLargeError:
      raised = true
    check raised
    joinThread(th)

  test "a sink raising a real exception propagates and the client stays usable":
    const port = 9709
    var th: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    let api = newNavi()
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      raise newException(ValueError, "boom")
    var raised = false
    try:
      discard api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    except ValueError:
      raised = true
    check raised
    joinThread(th)
    const port2 = 9710
    var th2: Thread[ServerCtx]
    startServer(th2, port2)
    check api.get("http://127.0.0.1:" & $port2 & "/").status == 200
    joinThread(th2)

  test "a transport truncation mid-delivery raises rather than re-delivering":
    const port = 9711
    var th: Thread[ServerCtx]
    startTruncated(th, port, bodyBytes = 10)   # Content-Length 100, only 10 sent
    let api = newNavi()
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} = true
    var raised = false
    try:
      discard api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    except IOError:
      raised = true
    check raised
    joinThread(th)

  test "a gzip body arrives DECODED through the sink":
    const port = 9712
    var th: Thread[ServerCtx]
    startGzipBody(th, port)
    let api = newNavi()
    var got = ""
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      got.add data; true
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 200
    check got == """{"ok":true}"""     # decoded, not the gzip bytes
    joinThread(th)

  test "an early stop on a gzip body does not raise the truncated-compressed error":
    const port = 9713
    var th: Thread[ServerCtx]
    startGzipBody(th, port)
    let api = newNavi()
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} = false
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.bodyTruncated
    joinThread(th)

  test "HEAD never calls the sink":
    const port = 9714
    var th: Thread[ServerCtx]
    startHeadNoBody(th, port)
    let api = newNavi()
    var called = false
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      called = true; true
    let res = api.head("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 200
    check not called
    check not res.bodyTruncated
    joinThread(th)

  test "204 never calls the sink":
    const port = 9715
    var th: Thread[ServerCtx]
    start204(th, port)
    var cfg = initNaviConfig()
    cfg.throwHttpErrors = false          # 204 is 2xx anyway, but keep it simple
    let api = newNavi(cfg)
    var called = false
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      called = true; true
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 204
    check not called
    check not res.bodyTruncated
    joinThread(th)

  test "middleware observes the final response with an empty body":
    const port = 9716
    var th: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    var seenBodyLen = -1
    var seenStatus = 0
    proc observe(): NaviMiddleware =
      result = proc(ctx: NaviContext) =
        ctx.next()
        seenBodyLen = ctx.res.body.len
        seenStatus = ctx.res.status
    var cfg = initNaviConfig()
    cfg.middleware = @[observe()]
    let api = newNavi(cfg)
    var got = ""
    let sink = proc(data: string): bool {.closure, raises: [CatchableError].} =
      got.add data; true
    let res = api.get("http://127.0.0.1:" & $port & "/", sink = sink)
    check res.status == 200
    check got == "Hello, chunked world!"
    check seenStatus == 200
    check seenBodyLen == 0               # middleware saw an empty (sinked) body
    joinThread(th)
