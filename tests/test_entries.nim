## End-to-end test of the sync entry module against an in-process TCP server.

import unittest
import std/[net, os, strutils, tables]
import navi
import navi/core/pool
import navi/core/response  # for the `response.TimeoutError` qualifier
import ./support

var serverReady: bool

# Middleware are closures, so config is captured by a factory (no module globals).
proc authMw(headerValue: string, observed: ref int): NaviMiddleware =
  result = proc(ctx: NaviContext) =
    ctx.req.headers["authorization"] = headerValue  # before  (captures headerValue)
    ctx.next()
    observed[] = ctx.res.status                      # after   (captures observed)
proc cannedMw(status: int, body: string): NaviMiddleware =
  result = proc(ctx: NaviContext) =                  # short-circuit: never calls next
    ctx.res = initResponse(status, "Short", "", initHeaders(), body)

proc serve(port: int) {.thread.} =
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(port), "127.0.0.1")
  server.listen()
  serverReady = true
  var client: Socket
  server.accept(client)
  var req = ""
  while "\r\n\r\n" notin req:
    req.add client.recv(1)
  let body = """{"ok":true}"""
  client.send("HTTP/1.1 200 OK\r\n" &
              "Content-Type: application/json\r\n" &
              "Content-Length: " & $body.len & "\r\n\r\n" & body)
  client.close()
  server.close()

suite "sync entry end to end":
  test "get should return a parsed response":
    const port = 8971
    var th: Thread[int]
    createThread(th, serve, port)
    while not serverReady: sleep(5)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.ok
    check res.headers.get("content-type") == "application/json"
    check res.data["ok"].getBool()
    joinThread(th)

  test "get should reuse a pooled connection for the same origin":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 2, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    let first = api.get(key & "/")
    check first.status == 200
    check first.body == "n=0"
    check api.pool.idleCount(key) == 1  # connection returned to the pool

    let second = api.get(key & "/")
    check second.status == 200
    check second.body == "n=1"
    joinThread(th)
    check accepts == 1  # both requests used the one connection

  test "close should drain the connection pool":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 2, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    discard api.get(key & "/")
    check api.pool.idleCount(key) == 1     # pooled after the request
    api.close()
    check api.pool.idleCount(key) == 0     # drained (and the socket closed)
    joinThread(th)

  test "a client dropped without close should still close its pooled connections":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 1, accepts = addr accepts)
    let key = "http://127.0.0.1:" & $port

    # Keep a handle to the pool, then let the client go out of scope WITHOUT
    # close(): its =destroy leak-guard must drain (and close) the pooled connection.
    let pool = block:
      let api = newNavi()
      check api.get(key & "/").status == 200
      check api.pool.idleCount(key) == 1   # pooled, not yet closed
      api.pool                             # `api` is unreferenced past here -> destroyed
    check pool.idleCount(key) == 0         # the destructor drained and closed it
    joinThread(th)

  test "stream should expose headers before the body and deliver it via each":
    const port = 8975
    var th: Thread[ServerCtx]
    startServer(th, port)  # responds with {"ok":true}, content-length 11

    let api = newNavi()
    var collected = ""
    let res = api.stream(GET, "http://127.0.0.1:" & $port & "/")
    check res.status == 200         # headers available before the body is drained
    res.each(chunk): collected.add chunk
    check collected == """{"ok":true}"""
    joinThread(th)

  test "stream should return the connection to the pool after a full drain":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 2, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    var got = ""
    block:
      let res = api.stream(GET, key & "/")
      check res.status == 200
      check api.pool.idleCount(key) == 0   # checked out while the handle is live
      res.each(chunk): got.add chunk
    check got == "n=0"
    check api.pool.idleCount(key) == 1     # returned to the pool after a full drain
    check api.get(key & "/").body == "n=1" # ...and reused
    joinThread(th)
    check accepts == 1                      # both requests used the one connection

  test "stream should close (not pool) the connection when the drain fails":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 1, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    var raised = false
    let res = api.stream(GET, key & "/")
    check res.status == 200
    try:
      res.each(chunk): raise newException(ValueError, "consumer failed")
    except ValueError: raised = true
    check raised                            # the block's error propagates out of each
    check api.pool.idleCount(key) == 0      # a failed drain closes, never pools
    joinThread(th)

  test "readChunk should deliver the body in order and end by pooling the connection":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 2, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    let res = api.stream(GET, key & "/")
    check res.status == 200
    var body = ""
    while (let c = res.readChunk(); c.len > 0):   # break-friendly pull loop
      body.add c
    check body == "n=0"
    check api.pool.idleCount(key) == 1      # full read to EOF returns it to the pool
    check api.get(key & "/").body == "n=1"  # ...and it is reused
    joinThread(th)
    check accepts == 1

  test "a readChunk stream dropped before EOF should be closed by the guard":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 1, accepts = addr accepts)

    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    let pool = block:
      let res = api.stream(GET, key & "/")
      discard res.readChunk()               # read a chunk but do not reach EOF
      api.pool                              # res dropped here without finishing
    check pool.idleCount(key) == 0          # not pooled: the guard closed it
    joinThread(th)

  test "sse should parse events from an event-stream and end when the server closes":
    const port = 9003
    let payload = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n" &
                  "Connection: close\r\n\r\n" &
                  "retry: 1000\n" &
                  "event: greeting\ndata: hello\n\n" &
                  ": keep-alive\n\n" &
                  "data: line1\ndata: line2\nid: 7\n\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi()
    let s = api.sse("http://127.0.0.1:" & $port & "/", reconnect = false)
    var events: seq[SseEvent]
    s.each(ev): events.add ev             # ends when the server closes (reconnect off)
    joinThread(th)
    check events.len == 2
    check events[0].event == "greeting" and events[0].data == "hello"
    check events[0].retry == 1000
    check events[1].data == "line1\nline2" and events[1].id == "7"
    check s.lastEventId() == "7"

  test "sse should reject a non-event-stream response":
    const port = 9004
    let payload = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n" &
                  "Content-Length: 2\r\nConnection: close\r\n\r\nhi"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi()
    expect IOError:
      discard api.sse("http://127.0.0.1:" & $port & "/", reconnect = false)
    joinThread(th)

  test "sse each should support break":
    const port = 9005
    let payload = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n" &
                  "Connection: close\r\n\r\ndata: 1\n\ndata: 2\n\ndata: 3\n\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi()
    let s = api.sse("http://127.0.0.1:" & $port & "/", reconnect = false)
    var got: seq[string]
    s.each(ev):
      got.add ev.data
      if ev.data == "2": break            # a real loop, so break works
    s.close()
    joinThread(th)
    check got == @["1", "2"]

  test "streaming upload should send a chunked body the server reassembles":
    const port = 8976
    var th: Thread[ServerCtx]
    startUploadEcho(th, port)

    let api = newNavi()
    let parts = @["hello ", "streaming ", "world"]
    var i = 0
    let res = api.request(POST, "http://127.0.0.1:" & $port & "/",
      bodyStream = proc(): string =
        if i < parts.len:
          result = parts[i]
          inc i)
    check res.status == 200
    check res.body == "hello streaming world"
    joinThread(th)

  test "the client should connect over IPv6 loopback":
    const port = 8977
    var th: Thread[ServerCtx]
    startServer(th, port, ipv6 = true)

    let api = newNavi()
    let res = api.get("http://[::1]:" & $port & "/")
    check res.status == 200
    check res.data["ok"].getBool()
    joinThread(th)

  test "the client should transparently decompress a gzip response body":
    const port = 8978
    # gzip -n of {"ok":true}
    let gz = hexToBytes("1f8b0800000000000003ab56cacf56b22a292a4dad0500905fd4a70b000000")
    let payload = "HTTP/1.1 200 OK\r\n" &
                  "Content-Encoding: gzip\r\n" &
                  "Content-Length: " & $gz.len & "\r\n" &
                  "Connection: close\r\n\r\n" & gz
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.body == """{"ok":true}"""       # decoded
    check res.data["ok"].getBool()
    check not res.headers.contains("content-encoding")  # header dropped
    joinThread(th)

  test "the client should transparently decompress a brotli response body":
    const port = 8968
    let br = hexToBytes("0f05807b226f6b223a747275657d03")   # brotli of {"ok":true}
    let payload = "HTTP/1.1 200 OK\r\nContent-Encoding: br\r\n" &
                  "Content-Length: " & $br.len & "\r\nConnection: close\r\n\r\n" & br
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.body == """{"ok":true}"""
    joinThread(th)

  test "the client should transparently decompress a zstd response body":
    const port = 8969
    let zst = hexToBytes("28b52ffd04585900007b226f6b223a747275657d6abe13c7")  # zstd
    let payload = "HTTP/1.1 200 OK\r\nContent-Encoding: zstd\r\n" &
                  "Content-Length: " & $zst.len & "\r\nConnection: close\r\n\r\n" & zst
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.body == """{"ok":true}"""
    joinThread(th)

  test "the client should raise HttpError when the response is non-2xx":
    const port = 8979
    let payload = "HTTP/1.1 404 Not Found\r\nContent-Length: 3\r\nConnection: close\r\n\r\nno!"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi()
    var raised = false
    try:
      discard api.get("http://127.0.0.1:" & $port & "/")
    except HttpError as e:
      raised = true
      check e.response.status == 404
      check e.response.body == "no!"
    check raised
    joinThread(th)

  test "the client should return the non-2xx response when throwHttpErrors is false":
    const port = 8980
    let payload = "HTTP/1.1 404 Not Found\r\nContent-Length: 3\r\nConnection: close\r\n\r\nno!"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = initNaviConfig()
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 404
    check res.body == "no!"
    joinThread(th)

  test "the client should follow a 302 redirect to the final response":
    const port = 8981
    var th: Thread[ServerCtx]
    startRedirect(th, port)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/start")
    check res.status == 200
    check res.body == "arrived"
    joinThread(th)

  test "the client should not follow and should surface the 3xx when maxRedirects is 0":
    const port = 8982
    let payload = "HTTP/1.1 302 Found\r\nLocation: /final\r\n" &
                  "Content-Length: 0\r\nConnection: close\r\n\r\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = initNaviConfig()
    cfg.maxRedirects = 0
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/start")
    check res.status == 302
    check res.headers.get("location") == "/final"
    joinThread(th)

  test "post should encode the body and set content-type when json is given":
    const port = 8983
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    let api = newNavi()
    let res = api.post("http://127.0.0.1:" & $port & "/", json = %*{"a": 1})
    check res.body == """{"a":1}"""
    check res.headers.get("x-echo-content-type") == "application/json"
    joinThread(th)

  test "post should url-encode the body and set content-type when form is given":
    const port = 8984
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    let api = newNavi()
    let res = api.post("http://127.0.0.1:" & $port & "/",
                       form = @[("a", "1"), ("b", "two words")])
    check res.body == "a=1&b=two+words"
    check res.headers.get("x-echo-content-type") == "application/x-www-form-urlencoded"
    joinThread(th)

  test "post should build a multipart/form-data body when multipart is given":
    const port = 8994
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    let api = newNavi()
    let res = api.post("http://127.0.0.1:" & $port & "/", multipart = @[
      field("title", "hello"),
      filePart("file", "a.txt", "file body", "text/plain")])
    let ct = res.headers.get("x-echo-content-type")
    check ct.startsWith("multipart/form-data; boundary=----naviFormBoundary")
    let boundary = ct.split("boundary=")[1]
    check res.body == "--" & boundary & "\r\n" &
      "Content-Disposition: form-data; name=\"title\"\r\n\r\n" &
      "hello\r\n" &
      "--" & boundary & "\r\n" &
      "Content-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n" &
      "Content-Type: text/plain\r\n\r\n" &
      "file body\r\n" &
      "--" & boundary & "--\r\n"
    joinThread(th)

  test "bearerAuth should set the Authorization header":
    const port = 8985
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    var cfg = initNaviConfig()
    cfg.auth = bearerAuth("secret-token")
    let api = newNavi(cfg)
    let res = api.post("http://127.0.0.1:" & $port & "/", body = "x")
    check res.headers.get("x-echo-authorization") == "Bearer secret-token"
    joinThread(th)

  test "basicAuth should base64-encode credentials":
    const port = 8986
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    var cfg = initNaviConfig()
    cfg.auth = basicAuth("user", "pass")
    let api = newNavi(cfg)
    let res = api.post("http://127.0.0.1:" & $port & "/", body = "x")
    check res.headers.get("x-echo-authorization") == "Basic dXNlcjpwYXNz"
    joinThread(th)

  test "the client should retry a 503 and then succeed":
    const port = 8987
    var th: Thread[ServerCtx]
    startRetry(th, port, failures = 1)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.body == "recovered"
    joinThread(th)

  test "the client should return the failing response without retrying when retry limit is 0":
    const port = 8988
    let payload = "HTTP/1.1 503 Service Unavailable\r\n" &
                  "Content-Length: 0\r\nConnection: close\r\n\r\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = initNaviConfig()
    cfg.retry.limit = 0
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 503
    joinThread(th)

  test "the client should time out when the response is stalled":
    var port = 0
    var th: Thread[ServerCtx]
    startHang(th, port)  # accepts, reads the request, never replies

    var cfg = initNaviConfig()
    cfg.timeouts.total = 200
    cfg.retry.limit = 0
    let api = newNavi(cfg)
    var raised = false
    try:
      discard api.get("http://127.0.0.1:" & $port & "/")
    except response.TimeoutError:   # qualified: std/net also defines TimeoutError
      raised = true
    check raised
    joinThread(th)

  test "parallel should time out when the response is stalled":
    var port = 0
    var th: Thread[ServerCtx]
    startHang(th, port)

    var cfg = initNaviConfig()
    cfg.timeouts.total = 200
    let api = newNavi(cfg)
    var raised = false
    try:
      discard api.parallel(@["http://127.0.0.1:" & $port & "/"])
    except response.TimeoutError:
      raised = true
    check raised
    joinThread(th)

  test "middleware should modify the request and observe the response":
    const port = 8989
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    let observed = new(int)
    var cfg = initNaviConfig()
    cfg.middleware = @[authMw("Wrapped", observed)]   # captured header + observer cell
    let api = newNavi(cfg)
    let res = api.post("http://127.0.0.1:" & $port & "/", body = "x")
    check res.headers.get("x-echo-authorization") == "Wrapped"
    check observed[] == 200
    joinThread(th)

  test "middleware should short-circuit without sending a request":
    # No server here: if the request were dialed it would fail to connect, so a
    # 299 proves `ctx.next()` was never called.
    var cfg = initNaviConfig()
    cfg.middleware = @[cannedMw(299, "from middleware")]
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:1/")
    check res.status == 299
    check res.body == "from middleware"

  test "the client should store a Set-Cookie and replay it on the next request":
    const port = 8990
    var th: Thread[ServerCtx]
    startCookies(th, port)

    let api = newNavi()
    discard api.get("http://127.0.0.1:" & $port & "/")       # receives Set-Cookie
    let res = api.get("http://127.0.0.1:" & $port & "/page")  # should send Cookie
    check res.body == "sid=abc123"
    joinThread(th)

  test "the client should route an http request through a proxy with an absolute-URI":
    const port = 8991
    var th: Thread[ServerCtx]
    startProxy(th, port)

    var cfg = initNaviConfig()
    cfg.proxy = "http://127.0.0.1:" & $port
    let api = newNavi(cfg)
    let res = api.get("http://example.test/path?q=1")
    check res.status == 200
    check res.body == "http://example.test/path?q=1"  # proxy saw the absolute URI
    joinThread(th)

  test "parallel should fetch multiple same-origin URLs over one connection":
    var port = 0
    var accepts = 0
    var th: Thread[KeepAliveCtx]
    startKeepAlive(th, port, requests = 3, accepts = addr accepts)

    let api = newNavi()
    let base = "http://127.0.0.1:" & $port
    let res = api.parallel(@[base & "/a", base & "/b", base & "/c"])
    check res.len == 3
    check res[0].status == 200
    check res[0].body == "n=0"
    check res[1].body == "n=1"
    check res[2].body == "n=2"
    joinThread(th)
    check accepts == 1  # all three reused the one connection

  test "options should send an OPTIONS request":
    const port = 8994
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    let api = newNavi()
    let res = api.options("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.headers.get("x-echo-method") == "OPTIONS"
    joinThread(th)

  test "extend should layer headers and prefixUrl":
    var bcfg = initNaviConfig()
    bcfg.headers = initHeaders({"x-base": "1"})
    let base = newNavi(bcfg)
    var ovr = initNaviConfig()
    ovr.prefixUrl = "http://api.test"
    let child = base.extend(ovr)
    check child.config.prefixUrl == "http://api.test"
    check child.config.headers.get("x-base") == "1"

  test "params should append an encoded query string to the target when given a map-like @{}":
    const port = 8951
    var th: Thread[ServerCtx]
    startEchoLine(th, port)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/search",
                      params = @{"q": "test", "n": "2"})
    check res.status == 200
    check res.body == "GET /search?q=test&n=2 HTTP/1.1"
    joinThread(th)

  test "params should accept an OrderedTable and preserve order":
    const port = 8950
    var th: Thread[ServerCtx]
    startEchoLine(th, port)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/search",
                      params = {"q": "test", "n": "2"}.toOrderedTable)
    check res.status == 200
    check res.body == "GET /search?q=test&n=2 HTTP/1.1"
    joinThread(th)

  test "maxResponseBytes should reject an oversized buffered body":
    const port = 8952
    let payload = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n" &
                  "Connection: close\r\n\r\n" & repeat('x', 50)
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = initNaviConfig()
    cfg.maxResponseBytes = 10
    let api = newNavi(cfg)
    var msg = ""
    try:
      discard api.get("http://127.0.0.1:" & $port & "/")
    except ResponseTooLargeError as e: msg = e.msg
    check "maxResponseBytes" in msg
    joinThread(th)

  test "maxResponseBytes should allow a body within the limit":
    const port = 8953
    let payload = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n" &
                  "Connection: close\r\n\r\n" & repeat('x', 50)
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = initNaviConfig()
    cfg.maxResponseBytes = 100
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.body.len == 50
    joinThread(th)

  test "maxResponseBytes should cap a streamed body incrementally":
    const port = 8954
    let payload = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n" &
                  "Connection: close\r\n\r\n" & repeat('y', 50)
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = initNaviConfig()
    cfg.maxResponseBytes = 10
    let api = newNavi(cfg)
    var msg = ""
    try:
      let res = api.stream(GET, "http://127.0.0.1:" & $port & "/")
      res.each(chunk): discard          # cap is enforced during the drain
    except ResponseTooLargeError as e: msg = e.msg
    check "maxResponseBytes" in msg
    joinThread(th)

  test "retry statuses should be configurable so an ineligible status is not retried":
    const port = 8955
    let payload = "HTTP/1.1 503 Service Unavailable\r\n" &
                  "Content-Length: 0\r\nConnection: close\r\n\r\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = initNaviConfig()
    cfg.retry.statuses = @[500]     # 503 no longer eligible, so no retry
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 503
    joinThread(th)

  test "the client should abort before dispatch when the cancel token is cancelled":
    let api = newNavi()
    let tok = newCancelToken()
    tok.cancel()
    var msg = ""
    try:
      discard api.get("http://127.0.0.1:1/", cancel = tok)
    except RequestCancelledError as e: msg = e.msg
    check "cancelled" in msg

  test "the client should leave the request unaffected when the cancel token is un-cancelled":
    const port = 8956
    var th: Thread[ServerCtx]
    startServer(th, port)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/", cancel = newCancelToken())
    check res.status == 200
    joinThread(th)

suite "TLS session resumption config":
  test "resumeSessions should be on by default and allocate a per-client session cache":
    check initNaviConfig().tls.resumeSessions
    let api = newNavi()
    when defined(ssl):
      check not api.config.tls.sessionCache.isNil
    else:
      check api.config.tls.sessionCache.isNil   # no OpenSSL, no cache

  test "the client should leave no session cache when resumeSessions is false":
    var cfg = initNaviConfig()
    cfg.tls.resumeSessions = false
    let api = newNavi(cfg)
    check api.config.tls.sessionCache.isNil

  test "extend should give the derived client its own cache":
    let parent = newNavi()
    let child = parent.extend(initNaviConfig())
    when defined(ssl):
      check not child.config.tls.sessionCache.isNil
      check not (child.config.tls.sessionCache == parent.config.tls.sessionCache)

suite "per-phase timeouts":
  test "timeout resolvers should read the per-phase struct":
    var cfg = initNaviConfig()
    check (cfg.connectMs, cfg.readMs, cfg.totalMs) == (0, 0, 0)
    cfg.timeouts = Timeouts(connect: 100, read: 200, total: 300)
    check (cfg.connectMs, cfg.readMs, cfg.totalMs) == (100, 200, 300)

  test "the read timeout should fire when the response is stalled":
    var port = 0
    var th: Thread[ServerCtx]
    startHang(th, port)
    var cfg = initNaviConfig()
    cfg.timeouts.read = 200
    cfg.retry.limit = 0
    let api = newNavi(cfg)
    var raised = false
    try: discard api.get("http://127.0.0.1:" & $port & "/")
    except response.TimeoutError: raised = true
    check raised
    joinThread(th)

  test "the total timeout should fire when the response is stalled":
    var port = 0
    var th: Thread[ServerCtx]
    startHang(th, port)
    var cfg = initNaviConfig()
    cfg.timeouts.total = 200
    cfg.retry.limit = 0
    let api = newNavi(cfg)
    var raised = false
    try: discard api.get("http://127.0.0.1:" & $port & "/")
    except response.TimeoutError: raised = true
    check raised
    joinThread(th)

suite "TLS version pinning config":
  test "tls minVersion and maxVersion should default to tlsDefault and be settable":
    var cfg = initNaviConfig()
    check cfg.tls.minVersion == tlsDefault
    check cfg.tls.maxVersion == tlsDefault
    cfg.tls.minVersion = tls12
    cfg.tls.maxVersion = tls13
    check cfg.tls.minVersion == tls12
    check cfg.tls.maxVersion == tls13

suite "TLS cipher selection config":
  test "ciphers and cipherSuites should default empty and be settable":
    var cfg = initNaviConfig()
    check cfg.tls.ciphers == "" and cfg.tls.cipherSuites == ""
    cfg.tls.ciphers = "ECDHE-RSA-AES128-GCM-SHA256"
    cfg.tls.cipherSuites = "TLS_AES_128_GCM_SHA256"
    check cfg.tls.ciphers == "ECDHE-RSA-AES128-GCM-SHA256"
    check cfg.tls.cipherSuites == "TLS_AES_128_GCM_SHA256"

  # A value with no valid cipher must fail loudly at context build (before any
  # socket work), never be silently ignored. newTlsContext runs before the dial,
  # so an unreachable URL never gets connected to.
  when defined(ssl):
    test "ciphers should raise rather than be ignored when the value is unusable":
      var cfg = initNaviConfig()
      cfg.tls.verify = false
      cfg.retry.limit = 0
      cfg.tls.ciphers = "NOT-A-REAL-CIPHER"
      let api = newNavi(cfg)
      var msg = ""
      try:
        discard api.get("https://127.0.0.1:1/")
      except ValueError as e: msg = e.msg
      check "no usable cipher" in msg
      api.close()

    test "cipherSuites should raise rather than be ignored when the value is unusable":
      var cfg = initNaviConfig()
      cfg.tls.verify = false
      cfg.retry.limit = 0
      cfg.tls.cipherSuites = "NOT-A-REAL-SUITE"
      let api = newNavi(cfg)
      var msg = ""
      try:
        discard api.get("https://127.0.0.1:1/")
      except ValueError as e: msg = e.msg
      check "ciphersuite" in msg   # OpenSSL: "no usable ciphersuite"; LibreSSL: "does not support ... ciphersuites"
      api.close()

suite "request header injection (CRLF)":
  test "a header value containing CRLF is rejected before any dispatch":
    let api = newNavi()
    var raised = false
    try:
      # If the guard works this raises ValueError before opening a socket; if it
      # did not, the request would attempt to connect and fail differently.
      discard api.get("http://127.0.0.1:9/",
                      headers = initHeaders({"x-evil": "a\r\nInjected: 1"}))
    except ValueError:
      raised = true
    except CatchableError:
      discard
    check raised
    api.close()

  test "a header name containing a newline is rejected":
    let api = newNavi()
    var raised = false
    try:
      discard api.get("http://127.0.0.1:9/",
                      headers = initHeaders({"x\r\nSmuggled": "1"}))
    except ValueError:
      raised = true
    except CatchableError:
      discard
    check raised
    api.close()

suite "streamed request body is not retried":
  test "a bodyStream PUT is not retried on 503 (non-rewindable body)":
    # startRetry answers one 503 then 200. A buffered idempotent request would be
    # retried and see 200; a streamed body can't be rewound, so it must not retry
    # and the 503 is returned as-is.
    const port = 8996
    var th: Thread[ServerCtx]
    startRetry(th, port, failures = 1)

    var cfg = initNaviConfig()
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    var sent = false
    proc producer(): string =
      if sent: "" else: (sent = true; "streamed-body")
    let res = api.request(PUT, "http://127.0.0.1:" & $port & "/",
                          bodyStream = producer)
    check res.status == 503        # not retried
    joinThread(th)
