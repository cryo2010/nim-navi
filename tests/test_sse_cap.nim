## DIAGNOSTIC COPY (ci/diag-sse-cap-windows): the #292 sync SSE cap test with
## timestamped progress written to $SSE_CAP_DIAG from both threads, so a Windows
## hang can be located. Not for merge.
import unittest
import std/[net, options, strutils, os, times, locks]
import navi
import navi/proto/sse   # maxSseEventBytes

var diagLock: Lock
initLock(diagLock)
let diagPath = getEnv("SSE_CAP_DIAG", "")
let t0 = epochTime()

proc diag(msg: string) =
  if diagPath.len == 0: return
  withLock diagLock:
    let f = open(diagPath, fmAppend)
    f.writeLine(formatFloat(epochTime() - t0, ffDecimal, 3), " ", msg)
    f.close()

var portChan: Channel[int]

const
  LinePayload = 64 * 1024
  LineCount = maxSseEventBytes div LinePayload + 8

proc runFloodSse() {.thread.} =
  {.cast(gcsafe).}:
    diag("srv: start")
    var srv = newSocket()
    srv.setSockOpt(OptReuseAddr, true)
    srv.bindAddr(Port(0), "127.0.0.1")
    srv.listen()
    portChan.send(srv.getLocalAddr()[1].int)
    diag("srv: listening")
    var client: Socket
    srv.accept(client)
    diag("srv: accepted")
    var line = ""
    while true:
      line = ""
      client.readLine(line, timeout = 2000)
      if line.len == 0 or line == "\c\l": break
    diag("srv: request headers drained")
    try:
      client.send("HTTP/1.1 200 OK\r\n" &
                  "Content-Type: text/event-stream\r\n" &
                  "Connection: close\r\n\r\n")
      let dataLine = "data: " & repeat('x', LinePayload) & "\n"
      for i in 0 ..< LineCount:
        client.send(dataLine)
        if i mod 16 == 0 or i == LineCount - 1: diag("srv: sent line " & $i)
      diag("srv: all lines sent")
    except CatchableError as e:
      diag("srv: send raised " & $e.name & ": " & e.msg)
    try: client.close() except CatchableError: discard
    diag("srv: client closed")
    try: srv.close() except CatchableError: discard
    diag("srv: done")

suite "sync SSE size cap (#292) [diag]":
  test "a feed() cap breach should close the underlying handle, not just raise":
    diag("cli: start")
    portChan.open()
    var th: Thread[void]
    createThread(th, runFloodSse)
    let port = portChan.recv()
    diag("cli: port " & $port)
    let api = newNavi()
    let s = api.sse("http://127.0.0.1:" & $port & "/events",
                    reconnect = false, idleTimeoutMs = 10_000)
    diag("cli: sse opened, httpVersion=" & s.httpVersion())
    var msg = ""
    try:
      discard s.next()
    except ValueError as e:
      msg = e.msg
    diag("cli: next() -> " & msg)
    check "limit" in msg
    diag("cli: httpVersion=" & s.httpVersion())
    check s.httpVersion() == ""
    let n2 = s.next()
    diag("cli: second next() isNone=" & $n2.isNone)
    check n2.isNone
    s.close()
    diag("cli: closed; joining")
    joinThread(th)
    diag("cli: joined")
    portChan.close()
