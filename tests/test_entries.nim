## End-to-end test of the sync entry module against an in-process TCP server.

import unittest
import std/[net, os, strutils]
import navi
import navi/core/pool
import ./support

var serverReady: bool

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
    check res.json()["ok"].getBool()
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

  test "extend layers headers and prefixUrl":
    let base = newNavi(NaviOptions(headers: initHeaders({"x-base": "1"})))
    let child = base.extend(NaviOptions(prefixUrl: "http://api.test"))
    check child.options.prefixUrl == "http://api.test"
    check child.options.headers.get("x-base") == "1"
