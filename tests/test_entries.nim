## End-to-end test of the sync entry module against an in-process TCP server.

import unittest
import std/[net, os, strutils]
import navi
import navi/core/pool
import navi/core/response  # for the `response.TimeoutError` qualifier
import ./support

var serverReady: bool

# Middleware are nimcall procs (no capture), so shared state is module-level.
var mwObservedStatus: int
proc addAuthMw(ctx: NaviContext) {.nimcall.} =
  ctx.req.headers["authorization"] = "Wrapped"   # before
  ctx.next()
  mwObservedStatus = ctx.res.status              # after
proc cannedMw(ctx: NaviContext) {.nimcall.} =
  ctx.res = initResponse(299, "Short", "", initHeaders(), "from middleware")

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
  test "GET localhost returns parsed response":
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

  test "reuses a pooled connection for the same origin":
    const port = 8974
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

  test "close drains the connection pool":
    const port = 8995
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

  test "stream delivers the response body to a sink and leaves body empty":
    const port = 8975
    var th: Thread[ServerCtx]
    startServer(th, port)  # responds with {"ok":true}, content-length 11

    let api = newNavi()
    var collected = ""
    let res = api.stream(GET, "http://127.0.0.1:" & $port & "/",
      sink = proc(data: openArray[byte]) =
        for b in data: collected.add char(b))
    check res.status == 200
    check res.body == ""            # not buffered
    check collected == """{"ok":true}"""
    joinThread(th)

  test "streaming upload sends a chunked body the server reassembles":
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

  test "connects over IPv6 loopback":
    const port = 8977
    var th: Thread[ServerCtx]
    startServer(th, port, ipv6 = true)

    let api = newNavi()
    let res = api.get("http://[::1]:" & $port & "/")
    check res.status == 200
    check res.data["ok"].getBool()
    joinThread(th)

  test "transparently decompresses a gzip response body":
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

  test "transparently decompresses a brotli response body":
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

  test "transparently decompresses a zstd response body":
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

  test "raises HttpError on a non-2xx response":
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

  test "throwHttpErrors = false returns the non-2xx response":
    const port = 8980
    let payload = "HTTP/1.1 404 Not Found\r\nContent-Length: 3\r\nConnection: close\r\n\r\nno!"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = newNaviConfig()
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 404
    check res.body == "no!"
    joinThread(th)

  test "follows a 302 redirect to the final response":
    const port = 8981
    var th: Thread[ServerCtx]
    startRedirect(th, port)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/start")
    check res.status == 200
    check res.body == "arrived"
    joinThread(th)

  test "maxRedirects = 0 does not follow and surfaces the 3xx":
    const port = 8982
    let payload = "HTTP/1.1 302 Found\r\nLocation: /final\r\n" &
                  "Content-Length: 0\r\nConnection: close\r\n\r\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = newNaviConfig()
    cfg.maxRedirects = 0
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/start")
    check res.status == 302
    check res.headers.get("location") == "/final"
    joinThread(th)

  test "post json= encodes the body and sets content-type":
    const port = 8983
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    let api = newNavi()
    let res = api.post("http://127.0.0.1:" & $port & "/", json = %*{"a": 1})
    check res.body == """{"a":1}"""
    check res.headers.get("x-echo-content-type") == "application/json"
    joinThread(th)

  test "post form= url-encodes the body and sets content-type":
    const port = 8984
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    let api = newNavi()
    let res = api.post("http://127.0.0.1:" & $port & "/",
                       form = @[("a", "1"), ("b", "two words")])
    check res.body == "a=1&b=two+words"
    check res.headers.get("x-echo-content-type") == "application/x-www-form-urlencoded"
    joinThread(th)

  test "post multipart= builds a multipart/form-data body":
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

  test "bearer auth sets the Authorization header":
    const port = 8985
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    var cfg = newNaviConfig()
    cfg.auth = bearerAuth("secret-token")
    let api = newNavi(cfg)
    let res = api.post("http://127.0.0.1:" & $port & "/", body = "x")
    check res.headers.get("x-echo-authorization") == "Bearer secret-token"
    joinThread(th)

  test "basic auth base64-encodes credentials":
    const port = 8986
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    var cfg = newNaviConfig()
    cfg.auth = basicAuth("user", "pass")
    let api = newNavi(cfg)
    let res = api.post("http://127.0.0.1:" & $port & "/", body = "x")
    check res.headers.get("x-echo-authorization") == "Basic dXNlcjpwYXNz"
    joinThread(th)

  test "retries a 503 then succeeds":
    const port = 8987
    var th: Thread[ServerCtx]
    startRetry(th, port, failures = 1)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.body == "recovered"
    joinThread(th)

  test "maxRetries = 0 returns the failing response without retrying":
    const port = 8988
    let payload = "HTTP/1.1 503 Service Unavailable\r\n" &
                  "Content-Length: 0\r\nConnection: close\r\n\r\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = newNaviConfig()
    cfg.retry.limit = 0
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 503
    joinThread(th)

  test "times out a stalled response":
    const port = 8996
    var th: Thread[ServerCtx]
    startHang(th, port)  # accepts, reads the request, never replies

    var cfg = newNaviConfig()
    cfg.timeout = 200
    cfg.retry.limit = 0
    let api = newNavi(cfg)
    var raised = false
    try:
      discard api.get("http://127.0.0.1:" & $port & "/")
    except response.TimeoutError:   # qualified: std/net also defines TimeoutError
      raised = true
    check raised
    joinThread(th)

  test "parallel times out a stalled response":
    const port = 8997
    var th: Thread[ServerCtx]
    startHang(th, port)

    var cfg = newNaviConfig()
    cfg.timeout = 200
    let api = newNavi(cfg)
    var raised = false
    try:
      discard api.parallel(@["http://127.0.0.1:" & $port & "/"])
    except response.TimeoutError:
      raised = true
    check raised
    joinThread(th)

  test "middleware modifies the request and observes the response":
    const port = 8989
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    mwObservedStatus = 0
    var cfg = newNaviConfig()
    cfg.middleware = @[Middleware(addAuthMw)]
    let api = newNavi(cfg)
    let res = api.post("http://127.0.0.1:" & $port & "/", body = "x")
    check res.headers.get("x-echo-authorization") == "Wrapped"
    check mwObservedStatus == 200
    joinThread(th)

  test "middleware can short-circuit without sending a request":
    # No server here: if the request were dialed it would fail to connect, so a
    # 299 proves `ctx.next()` was never called.
    var cfg = newNaviConfig()
    cfg.middleware = @[Middleware(cannedMw)]
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:1/")
    check res.status == 299
    check res.body == "from middleware"

  test "stores a Set-Cookie and replays it on the next request":
    const port = 8990
    var th: Thread[ServerCtx]
    startCookies(th, port)

    let api = newNavi()
    discard api.get("http://127.0.0.1:" & $port & "/")       # receives Set-Cookie
    let res = api.get("http://127.0.0.1:" & $port & "/page")  # should send Cookie
    check res.body == "sid=abc123"
    joinThread(th)

  test "routes an http request through a proxy with an absolute-URI":
    const port = 8991
    var th: Thread[ServerCtx]
    startProxy(th, port)

    var cfg = newNaviConfig()
    cfg.proxy = "http://127.0.0.1:" & $port
    let api = newNavi(cfg)
    let res = api.get("http://example.test/path?q=1")
    check res.status == 200
    check res.body == "http://example.test/path?q=1"  # proxy saw the absolute URI
    joinThread(th)

  test "parallel fetches multiple same-origin URLs over one connection":
    const port = 8993
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

  test "options sends an OPTIONS request":
    const port = 8994
    var th: Thread[ServerCtx]
    startBodyEcho(th, port)

    let api = newNavi()
    let res = api.options("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.headers.get("x-echo-method") == "OPTIONS"
    joinThread(th)

  test "extend layers headers and prefixUrl":
    var bcfg = newNaviConfig()
    bcfg.headers = initHeaders({"x-base": "1"})
    let base = newNavi(bcfg)
    var ovr = newNaviConfig()
    ovr.prefixUrl = "http://api.test"
    let child = base.extend(ovr)
    check child.config.prefixUrl == "http://api.test"
    check child.config.headers.get("x-base") == "1"

  test "params appends an encoded query string to the target":
    const port = 8951
    var th: Thread[ServerCtx]
    startEchoLine(th, port)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/search",
                      params = @[("q", "test"), ("n", "2")])
    check res.status == 200
    check res.body == "GET /search?q=test&n=2 HTTP/1.1"
    joinThread(th)

  test "maxResponseBytes rejects an oversized buffered body":
    const port = 8952
    let payload = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n" &
                  "Connection: close\r\n\r\n" & repeat('x', 50)
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = newNaviConfig()
    cfg.maxResponseBytes = 10
    let api = newNavi(cfg)
    expect ResponseTooLargeError:
      discard api.get("http://127.0.0.1:" & $port & "/")
    joinThread(th)

  test "maxResponseBytes allows a body within the limit":
    const port = 8953
    let payload = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n" &
                  "Connection: close\r\n\r\n" & repeat('x', 50)
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = newNaviConfig()
    cfg.maxResponseBytes = 100
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.body.len == 50
    joinThread(th)

  test "maxResponseBytes caps a streamed body incrementally":
    const port = 8954
    let payload = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n" &
                  "Connection: close\r\n\r\n" & repeat('y', 50)
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = newNaviConfig()
    cfg.maxResponseBytes = 10
    let api = newNavi(cfg)
    expect ResponseTooLargeError:
      discard api.stream(GET, "http://127.0.0.1:" & $port & "/",
        sink = proc(data: openArray[byte]) = discard)
    joinThread(th)

  test "retry statuses are configurable":
    const port = 8955
    let payload = "HTTP/1.1 503 Service Unavailable\r\n" &
                  "Content-Length: 0\r\nConnection: close\r\n\r\n"
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = newNaviConfig()
    cfg.retry.statuses = @[500]     # 503 no longer eligible, so no retry
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 503
    joinThread(th)

  test "a cancelled token aborts before dispatch":
    let api = newNavi()
    let tok = newCancelToken()
    tok.cancel()
    expect RequestCancelledError:
      discard api.get("http://127.0.0.1:1/", cancel = tok)

  test "an un-cancelled token leaves the request unaffected":
    const port = 8956
    var th: Thread[ServerCtx]
    startServer(th, port)

    let api = newNavi()
    let res = api.get("http://127.0.0.1:" & $port & "/", cancel = newCancelToken())
    check res.status == 200
    joinThread(th)
