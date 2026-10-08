## Shared SSE connection-sharing spec for the async backends (#466), instantiated by
## test_sse_share_async.nim and test_sse_share_chronos.nim: each imports its backend,
## defines `sseBackendName`, then `include`s this. A test added here runs under BOTH
## backends (in particular chronos's gcsafe / strict-raises checks). Not named
## `t*`/`test_*`, so the runner does not compile it standalone.
##
## Each test's logic lives in a local `run(): Future[...] {.async.}` proc so its
## accumulators are proc locals (captured into the async env, gcsafe) rather than
## module globals, mirroring sse_retry_spec.nim.

const
  sseBody = ": keep-alive\n\ndata: one\nid: 1\n\ndata: two\nid: 2\n\n"
  sseHalf1 = ": keep-alive\n\ndata: one\nid: 1\n\n"
  sseHalf2 = "data: two\nid: 2\n\n"

suite "SSE shares the caller's client (" & sseBackendName & ", #466)":
  test "two consecutive streams should reuse the client's one connection":
    var th: Thread[SseKeepAliveSrv]
    var port, accepts, requests: int
    var cfg = SseKeepAliveSrv(accepts: addr accepts, requests: addr requests,
                              maxConns: 3, events: sseBody,
                              altSvc: "h3=\":443\"; ma=3600")
    startSseKeepAlive(th, port, cfg)
    proc run(): Future[(seq[string], seq[string], bool, bool)] {.async.} =
      let api = newNavi()
      let url = "http://127.0.0.1:" & $port & "/events"
      var got1, got2: seq[string]
      let s1 = await api.sse(url, reconnect = false)
      # The stream's view IS the client's state: on an -d:naviHttp3 build that
      # identity covers the Alt-Svc cache, so the h3 advertisement carried on this
      # very response is recorded in the CALLER's cache.
      let shares = s1.sharesConnections(api)
      # ...but NOT the h2 mux table its requests use: one h2 connection carries a
      # single read bound for all of its streams, and an SSE stream runs with none
      # (#466). http/1.1, which this test is on, is pooled and shared either way.
      let sharesH2 = s1.sharesH2Connections(api)
      while true:
        let ev = await s1.next()
        if ev.isNone: break
        got1.add ev.get.data
      await s1.close()
      let s2 = await api.sse(url, reconnect = false)
      while true:
        let ev = await s2.next()
        if ev.isNone: break
        got2.add ev.get.data
      await s2.close()
      await api.close()
      return (got1, got2, shares, sharesH2)
    let (got1, got2, shares, sharesH2) = waitFor run()
    check shares
    check not sharesH2
    check got1 == @["one", "two"]
    check got2 == @["one", "two"]
    check requests == 2                  # both streams were served
    check accepts == 1                   # ...on ONE connection, no second handshake
    drainSseKeepAlive(port, 3, addr accepts)
    joinThread(th)

  test "a closed stream should leave the client usable on the same connection":
    var th: Thread[SseKeepAliveSrv]
    var port, accepts, requests: int
    var cfg = SseKeepAliveSrv(accepts: addr accepts, requests: addr requests,
                              maxConns: 3, events: sseBody)
    startSseKeepAlive(th, port, cfg)
    proc run(): Future[(seq[string], int, string, int)] {.async.} =
      let api = newNavi()
      let base = "http://127.0.0.1:" & $port
      var got: seq[string]
      let s = await api.sse(base & "/events", reconnect = false)
      while true:
        let ev = await s.next()
        if ev.isNone: break
        got.add ev.get.data
      await s.close()                    # the stream is done; the connection is not
      let res = await api.get(base & "/plain")
      let jar = api.cookies().len
      await api.close()
      return (got, res.status, res.body, jar)
    let (got, status, body, jar) = waitFor run()
    check got == @["one", "two"]
    check status == 200
    check accepts == 1                   # the client's pooled connection survived
    check requests == 2
    # The SSE response's Set-Cookie went into the CALLER's jar, so the caller's own
    # request carried it back: proof the jar is shared, not copied.
    check body == "cookie=sid=sse"
    check jar == 1
    drainSseKeepAlive(port, 3, addr accepts)
    joinThread(th)

  test "closing a stream mid-body should close only that stream's connection":
    # Discriminating by construction: the stream must RIDE the connection the caller
    # already pooled (so `afterOpen` is still 1 -- a stream on a private client of its
    # own would have handshaked a second one here), and the mid-body close must then
    # cost the caller exactly that connection and nothing else. Asserting only the
    # final `accepts` would pass either way: a private stream opens a connection and
    # closes it, leaving the caller's pooled one to serve the last request, which is
    # the same count by a completely different route.
    var th: Thread[SseKeepAliveSrv]
    var port, accepts, requests: int
    var cfg = SseKeepAliveSrv(accepts: addr accepts, requests: addr requests,
                              maxConns: 4, events: sseHalf1, events2: sseHalf2,
                              splitMs: 300)
    startSseKeepAlive(th, port, cfg)
    proc run(): Future[(string, int, int, int)] {.async.} =
      let api = newNavi()
      let base = "http://127.0.0.1:" & $port
      let warm = await api.get(base & "/plain")   # the caller pools a connection
      let s = await api.sse(base & "/events", reconnect = false)
      let afterOpen = accepts            # the stream ran on THAT connection
      let first = await s.next()         # arrives in the first write; the rest is
      await s.close()                    # still coming: a mid-body close
      # A half-read response cannot be pooled, so the caller needs a fresh connection
      # -- but the client itself is untouched and works.
      let res = await api.get(base & "/plain")
      await api.close()
      return ((if first.isSome: first.get.data else: ""), res.status, afterOpen,
              warm.status)
    let (first, status, afterOpen, warmStatus) = waitFor run()
    check warmStatus == 200
    check afterOpen == 1                 # no second handshake to open the stream
    check first == "one"
    check status == 200
    check accepts == 2                   # only the stream's own connection was lost
    check requests == 3
    drainSseKeepAlive(port, 4, addr accepts)
    joinThread(th)

  test "a stream should reuse a connection the client pooled, and close() reap it":
    var th: Thread[SseKeepAliveSrv]
    var port, accepts, requests: int
    var cfg = SseKeepAliveSrv(accepts: addr accepts, requests: addr requests,
                              maxConns: 4, events: sseBody)
    startSseKeepAlive(th, port, cfg)
    proc run(): Future[(int, int, int)] {.async.} =
      let api = newNavi()
      let base = "http://127.0.0.1:" & $port
      let first = await api.get(base & "/plain")  # the client pools a connection here
      let s = await api.sse(base & "/events", reconnect = false)
      while true:
        let ev = await s.next()
        if ev.isNone: break
      await s.close()
      let afterStream = accepts          # the stream ran on the pooled connection
      await api.close()                  # reaps the connection the stream pooled
      # #441: the client still works after close, it just opens fresh connections.
      let res = await api.get(base & "/plain")
      await api.close()
      return (first.status, afterStream, res.status)
    let (firstStatus, afterStream, status) = waitFor run()
    check firstStatus == 200
    check afterStream == 1
    check requests == 3
    check status == 200
    check accepts == 2
    drainSseKeepAlive(port, 4, addr accepts)
    joinThread(th)
