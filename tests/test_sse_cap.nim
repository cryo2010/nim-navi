## DIAGNOSTIC COPY (ci/diag-sse-cap-windows), round 2. Not for merge.
## A: navi client against a server whose sends carry a 3 s timeout.
## B: raw std/net client (reads ~16.8 MB then closes) against a server with no
##    send timeout: hangs => Winsock does not wake a blocked send on peer reset.
import unittest
import std/[net, options, strutils, os, times, locks, nativesockets]
import navi
import navi/proto/sse   # maxSseEventBytes

when defined(windows):
  from std/winlean import SOL_SOCKET
  var SO_SNDTIMEO {.importc, header: "winsock2.h".}: cint
  proc setSendTimeout(s: Socket, ms: int) =
    setSockOptInt(s.getFd, SOL_SOCKET.int, SO_SNDTIMEO.int, ms)   # win: DWORD ms
else:
  import std/posix
  proc setSendTimeout(s: Socket, ms: int) =
    var tv = Timeval(tv_sec: posix.Time(ms div 1000),
                     tv_usec: Suseconds((ms mod 1000) * 1000))
    discard setsockopt(s.getFd, SOL_SOCKET, SO_SNDTIMEO, addr tv, SockLen(sizeof tv))

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

proc runFloodSse(arg: tuple[sendTimeoutMs: int, tag: string]) {.thread.} =
  {.cast(gcsafe).}:
    let tag = arg.tag & " srv: "
    diag(tag & "start")
    var srv = newSocket()
    srv.setSockOpt(OptReuseAddr, true)
    srv.bindAddr(Port(0), "127.0.0.1")
    srv.listen()
    portChan.send(srv.getLocalAddr()[1].int)
    var client: Socket
    srv.accept(client)
    if arg.sendTimeoutMs > 0: client.setSendTimeout(arg.sendTimeoutMs)
    diag(tag & "accepted")
    var line = ""
    while true:
      line = ""
      client.readLine(line, timeout = 2000)
      if line.len == 0 or line == "\c\l": break
    diag(tag & "request headers drained")
    try:
      client.send("HTTP/1.1 200 OK\r\n" &
                  "Content-Type: text/event-stream\r\n" &
                  "Connection: close\r\n\r\n")
      let dataLine = "data: " & repeat('x', LinePayload) & "\n"
      for i in 0 ..< LineCount:
        client.send(dataLine, flags = {})
        if i mod 32 == 0 or i == LineCount - 1: diag(tag & "sent line " & $i)
      diag(tag & "all lines sent")
    except CatchableError as e:
      diag(tag & "send raised " & $e.name & ": " & e.msg)
    try: client.close() except CatchableError: discard
    try: srv.close() except CatchableError: discard
    diag(tag & "done")

suite "sync SSE size cap (#292) [diag round 2]":
  test "A: navi client, flood sent with flags = {}":
    diag("A cli: start")
    portChan.open()
    var th: Thread[(int, string)]
    createThread(th, runFloodSse, (0, "A"))
    let port = portChan.recv()
    let api = newNavi()
    let s = api.sse("http://127.0.0.1:" & $port & "/events",
                    reconnect = false, idleTimeoutMs = 10_000)
    var msg = ""
    try:
      discard s.next()
    except ValueError as e:
      msg = e.msg
    diag("A cli: next() -> " & msg)
    check "limit" in msg
    check s.httpVersion() == ""
    check s.next().isNone
    s.close()
    diag("A cli: closed; joining")
    joinThread(th)
    diag("A cli: joined")
    portChan.close()

  test "B: raw client reads 16.8 MB then closes, server has no send timeout":
    diag("B cli: start")
    portChan.open()
    var th: Thread[(int, string)]
    createThread(th, runFloodSse, (0, "B"))
    let port = portChan.recv()
    var c = newSocket()
    c.connect("127.0.0.1", Port(port))
    c.send("GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
    var got = 0
    while got < 16_800_000:
      let chunk = c.recv(65536, timeout = 10_000)
      if chunk.len == 0: break
      got += chunk.len
    diag("B cli: read " & $got & " bytes; closing")
    c.close()
    diag("B cli: closed; joining")
    joinThread(th)
    diag("B cli: joined")
    check got >= 16_800_000
    portChan.close()
