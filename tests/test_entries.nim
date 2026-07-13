## End-to-end test of the sync entry module against an in-process TCP server.

import unittest
import std/[net, os, strutils]
import navi

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

  test "extend layers headers and prefixUrl":
    let base = newNavi(NaviOptions(headers: initHeaders({"x-base": "1"})))
    let child = base.extend(NaviOptions(prefixUrl: "http://api.test"))
    check child.options.prefixUrl == "http://api.test"
    check child.options.headers.get("x-base") == "1"
