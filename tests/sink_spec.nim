## Shared response-sink spec for the async backends, instantiated by
## test_sink_async.nim and test_sink_chronos.nim: each imports its backend, defines
## `sinkBackendName` and a base port `sinkBasePort`, then `include`s this. A test
## added here runs under BOTH backends (in particular chronos's gcsafe/strict-raises
## checks). Not named `t*`/`test_*`, so the runner does not compile it standalone.
##
## Each test's logic lives in a local `run(): Future[...] {.async.}` proc so its
## accumulators are proc locals (captured into the async env, gcsafe) rather than
## module globals (which chronos's strict gcsafe rejects), mirroring test_chronos.nim.
## The sink is a `proc(data: string): Future[bool]` (the async GatedBodySink); the
## void form is `proc(data: string): Future[void]`.

suite "response sink (" & sinkBackendName & ", request)":
  test "a void sink receives the full body and leaves res.body empty":
    let port = sinkBasePort + 0
    var th: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    proc run(): Future[(int, string, string, string)] {.async.} =
      let api = newNavi()
      var got = ""
      let sink = proc(data: string): Future[void] {.async.} = got.add data
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      return (res.status, res.body, got, res.trailers.get("x-checksum"))
    let (status, body, got, trailer) = waitFor run()
    check status == 200
    check body == ""
    check got == "Hello, chunked world!"
    check trailer == "done"
    joinThread(th)

  test "delivery rule: a redirect hop body is not delivered, only the final":
    let port = sinkBasePort + 1
    var th: Thread[ServerCtx]
    startRedirect(th, port)
    proc run(): Future[(int, string)] {.async.} =
      let api = newNavi()
      var got = ""
      let sink = proc(data: string): Future[bool] {.async.} = got.add data; return true
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      return (res.status, got)
    let (status, got) = waitFor run()
    check status == 200
    check got == "arrived"
    joinThread(th)

  test "delivery rule: a retried 503 body is not delivered, only the 200":
    let port = sinkBasePort + 2
    var th: Thread[ServerCtx]
    startRetry(th, port, failures = 1)
    proc run(): Future[(int, string)] {.async.} =
      let api = newNavi()
      var got = ""
      let sink = proc(data: string): Future[bool] {.async.} = got.add data; return true
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      return (res.status, got)
    let (status, got) = waitFor run()
    check status == 200
    check got == "recovered"
    joinThread(th)

  test "delivery rule: a digest 401 challenge body is not delivered, the protected body is":
    let port = sinkBasePort + 3
    var th: Thread[ServerCtx]
    startDigestThenBody(th, port, "secret-data")
    proc run(): Future[(int, string)] {.async.} =
      var cfg = initNaviConfig()
      cfg.auth = digestAuth("user", "pass")
      let api = newNavi(cfg)
      var got = ""
      let sink = proc(data: string): Future[bool] {.async.} = got.add data; return true
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      return (res.status, got)
    let (status, got) = waitFor run()
    check status == 200
    check got == "secret-data"
    joinThread(th)

  test "early stop: a false return truncates and the client stays usable":
    let port = sinkBasePort + 4
    let port2 = sinkBasePort + 5
    var th, th2: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    startServer(th2, port2)
    proc run(): Future[(bool, string, int, int, int, bool)] {.async.} =
      let api = newNavi()
      var chunks = 0
      let sink = proc(data: string): Future[bool] {.async.} =
        inc chunks; return false
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      let res2 = await api.get("http://127.0.0.1:" & $port2 & "/")
      return (res.bodyTruncated, res.body, chunks, res.trailers.len,
              res2.status, res2.data["ok"].getBool())
    let (truncated, body, chunks, trailerLen, status2, ok2) = waitFor run()
    check truncated
    check body == ""
    check chunks == 1
    check trailerLen == 0
    check status2 == 200
    check ok2
    joinThread(th)
    joinThread(th2)

  test "throwHttpErrors: a non-2xx never calls the sink and HttpError carries the body":
    let port = sinkBasePort + 6
    var th: Thread[ServerCtx]
    startStatusBody(th, port, 404, "not found here")
    proc run(): Future[(bool, bool, string)] {.async.} =
      let api = newNavi()
      var called = false
      let sink = proc(data: string): Future[bool] {.async.} = called = true; return true
      var raised = false
      var errBody = ""
      try:
        discard await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      except HttpError as e:
        raised = true
        errBody = e.response.body
      return (raised, called, errBody)
    let (raised, called, errBody) = waitFor run()
    check raised
    check not called
    check errBody == "not found here"
    joinThread(th)

  test "maxResponseBytes breach raises through the sinked path":
    let port = sinkBasePort + 7
    var th: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    proc run(): Future[bool] {.async.} =
      var cfg = initNaviConfig()
      cfg.maxResponseBytes = 5
      let api = newNavi(cfg)
      let sink = proc(data: string): Future[bool] {.async.} = return true
      try:
        discard await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      except ResponseTooLargeError:
        return true
      return false
    check waitFor run()
    joinThread(th)

  test "a sink raising a real exception propagates and the client stays usable":
    let port = sinkBasePort + 8
    let port2 = sinkBasePort + 9
    var th, th2: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    startServer(th2, port2)
    proc run(): Future[(bool, int)] {.async.} =
      let api = newNavi()
      let sink = proc(data: string): Future[bool] {.async.} =
        raise newException(ValueError, "boom")
      var raised = false
      try:
        discard await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      except ValueError:
        raised = true
      let res2 = await api.get("http://127.0.0.1:" & $port2 & "/")
      return (raised, res2.status)
    let (raised, status2) = waitFor run()
    check raised
    check status2 == 200
    joinThread(th)
    joinThread(th2)

  test "a transport truncation mid-delivery raises rather than re-delivering":
    let port = sinkBasePort + 10
    var th: Thread[ServerCtx]
    startTruncated(th, port, bodyBytes = 10)
    proc run(): Future[bool] {.async.} =
      let api = newNavi()
      let sink = proc(data: string): Future[bool] {.async.} = return true
      try:
        discard await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      except IOError:
        return true
      return false
    check waitFor run()
    joinThread(th)

  test "a gzip body arrives DECODED through the sink":
    let port = sinkBasePort + 11
    var th: Thread[ServerCtx]
    startGzipBody(th, port)
    proc run(): Future[(int, string)] {.async.} =
      let api = newNavi()
      var got = ""
      let sink = proc(data: string): Future[bool] {.async.} = got.add data; return true
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      return (res.status, got)
    let (status, got) = waitFor run()
    check status == 200
    check got == """{"ok":true}"""
    joinThread(th)

  test "an early stop on a gzip body does not raise the truncated-compressed error":
    let port = sinkBasePort + 12
    var th: Thread[ServerCtx]
    startGzipBody(th, port)
    proc run(): Future[bool] {.async.} =
      let api = newNavi()
      let sink = proc(data: string): Future[bool] {.async.} = return false
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      return res.bodyTruncated
    check waitFor run()
    joinThread(th)

  test "HEAD never calls the sink":
    let port = sinkBasePort + 13
    var th: Thread[ServerCtx]
    startHeadNoBody(th, port)
    proc run(): Future[(int, bool, bool)] {.async.} =
      let api = newNavi()
      var called = false
      let sink = proc(data: string): Future[bool] {.async.} = called = true; return true
      let res = await api.head("http://127.0.0.1:" & $port & "/", sink = sink)
      return (res.status, called, res.bodyTruncated)
    let (status, called, truncated) = waitFor run()
    check status == 200
    check not called
    check not truncated
    joinThread(th)

  test "204 never calls the sink":
    let port = sinkBasePort + 14
    var th: Thread[ServerCtx]
    start204(th, port)
    proc run(): Future[(int, bool, bool)] {.async.} =
      let api = newNavi()
      var called = false
      let sink = proc(data: string): Future[bool] {.async.} = called = true; return true
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      return (res.status, called, res.bodyTruncated)
    let (status, called, truncated) = waitFor run()
    check status == 204
    check not called
    check not truncated
    joinThread(th)

  test "middleware observes the final response with an empty body":
    let port = sinkBasePort + 15
    var th: Thread[ServerCtx]
    startChunkedTrailer(th, port)
    proc run(): Future[(int, string, int)] {.async.} =
      var seenBodyLen = -1
      proc observe(): NaviMiddleware =
        result = proc(ctx: NaviContext) {.async.} =
          await ctx.next()
          seenBodyLen = ctx.res.body.len
      var cfg = initNaviConfig()
      cfg.middleware = @[observe()]
      let api = newNavi(cfg)
      var got = ""
      let sink = proc(data: string): Future[bool] {.async.} = got.add data; return true
      let res = await api.get("http://127.0.0.1:" & $port & "/", sink = sink)
      return (res.status, got, seenBodyLen)
    let (status, got, seenBodyLen) = waitFor run()
    check status == 200
    check got == "Hello, chunked world!"
    check seenBodyLen == 0
    joinThread(th)
