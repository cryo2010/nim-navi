## Sync SSE connection sharing (#466): `sse()` runs on the CALLER's client, so two
## consecutive streams reuse one connection, the cookie jar is genuinely shared, and
## `SseStream.close()` disposes the stream without touching anything of the client's.
## Plain HTTP over a loopback socket (no TLS), so it runs on every host.
import unittest
import std/options
import navi
import ./support_sse

const
  sseBody = ": keep-alive\n\ndata: one\nid: 1\n\ndata: two\nid: 2\n\n"
  sseHalf1 = ": keep-alive\n\ndata: one\nid: 1\n\n"
  sseHalf2 = "data: two\nid: 2\n\n"

proc collect(s: SseStream): seq[string] =
  ## Every event the stream has, until it ends (reconnect is off in these tests, so
  ## the end of the server's body is the end of the stream).
  while true:
    let ev = s.next()
    if ev.isNone: break
    result.add ev.get.data

suite "sync SSE shares the caller's client (#466)":
  test "two consecutive streams should reuse the client's one connection":
    var th: Thread[SseKeepAliveSrv]
    var port, accepts, requests: int
    var cfg = SseKeepAliveSrv(accepts: addr accepts, requests: addr requests,
                              maxConns: 3, events: sseBody,
                              altSvc: "h3=\":443\"; ma=3600")
    startSseKeepAlive(th, port, cfg)
    let api = newNavi()
    let url = "http://127.0.0.1:" & $port & "/events"
    let s1 = api.sse(url, reconnect = false)
    check s1.sharesConnections(api)      # the stream's view IS the client's state:
                                         # on an -d:naviHttp3 build that identity
                                         # covers the Alt-Svc cache, so the h3
                                         # advertisement on this very response is
                                         # recorded in the CALLER's cache
    check s1.sharesH2Connections(api)    # including its h2 connections, which on this
                                         # backend are checked out of the pool one
                                         # request at a time (#466)
    let got1 = s1.collect()
    s1.close()
    let s2 = api.sse(url, reconnect = false)
    let got2 = s2.collect()
    s2.close()
    check got1 == @["one", "two"]
    check got2 == @["one", "two"]
    check requests == 2                  # both streams were served
    check accepts == 1                   # ...on ONE connection, no second handshake
    api.close()
    drainSseKeepAlive(port, 3, addr accepts)
    joinThread(th)

  test "a closed stream should leave the client usable on the same connection":
    var th: Thread[SseKeepAliveSrv]
    var port, accepts, requests: int
    var cfg = SseKeepAliveSrv(accepts: addr accepts, requests: addr requests,
                              maxConns: 3, events: sseBody)
    startSseKeepAlive(th, port, cfg)
    let api = newNavi()
    let base = "http://127.0.0.1:" & $port
    let s = api.sse(base & "/events", reconnect = false)
    let got = s.collect()
    s.close()                            # the stream is done; the connection is not
    let res = api.get(base & "/plain")
    check got == @["one", "two"]
    check res.status == 200
    check accepts == 1                   # the client's pooled connection survived
    check requests == 2
    # The SSE response's Set-Cookie went into the CALLER's jar, so the caller's own
    # request carried it back: proof the jar is shared, not copied.
    check res.body == "cookie=sid=sse"
    check api.cookies().len == 1
    api.close()
    drainSseKeepAlive(port, 3, addr accepts)
    joinThread(th)

  test "closing a stream mid-body should close only that stream's connection":
    # Discriminating by construction: the stream must RIDE the connection the caller
    # already pooled (so `accepts` is still 1 right after it opens -- a stream on a
    # private client of its own would have handshaked a second one), and the mid-body
    # close must then cost the caller exactly that connection and nothing else.
    # Asserting only the final count would pass either way (see the async twin).
    var th: Thread[SseKeepAliveSrv]
    var port, accepts, requests: int
    var cfg = SseKeepAliveSrv(accepts: addr accepts, requests: addr requests,
                              maxConns: 4, events: sseHalf1, events2: sseHalf2,
                              splitMs: 300)
    startSseKeepAlive(th, port, cfg)
    let api = newNavi()
    let base = "http://127.0.0.1:" & $port
    check api.get(base & "/plain").status == 200   # the caller pools a connection
    let s = api.sse(base & "/events", reconnect = false)
    check accepts == 1                   # the stream ran on THAT connection
    let first = s.next()                 # arrives in the first write; the rest is
    s.close()                            # still coming, so this is a mid-body close
    # A half-read response cannot be pooled, so the caller needs a fresh connection
    # -- but the client itself is untouched and works.
    let res = api.get(base & "/plain")
    check first.isSome and first.get.data == "one"
    check res.status == 200
    check accepts == 2                   # only the stream's own connection was lost
    check requests == 3
    api.close()
    drainSseKeepAlive(port, 4, addr accepts)
    joinThread(th)

  test "a stream should reuse a connection the client pooled, and close() reap it":
    var th: Thread[SseKeepAliveSrv]
    var port, accepts, requests: int
    var cfg = SseKeepAliveSrv(accepts: addr accepts, requests: addr requests,
                              maxConns: 4, events: sseBody)
    startSseKeepAlive(th, port, cfg)
    let api = newNavi()
    let base = "http://127.0.0.1:" & $port
    let first = api.get(base & "/plain")  # the client pools a connection here
    let s = api.sse(base & "/events", reconnect = false)
    discard s.collect()
    s.close()
    check first.status == 200
    check accepts == 1                   # the stream ran on the pooled connection
    check requests == 2
    api.close()                          # reaps the connection the stream pooled
    check accepts == 1
    # #441: the client still works after close, it just opens fresh connections.
    let res = api.get(base & "/plain")
    check res.status == 200
    check accepts == 2
    api.close()
    drainSseKeepAlive(port, 4, addr accepts)
    joinThread(th)
