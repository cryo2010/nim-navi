## Sync SSE size-cap teardown (#292): a `maxSseEventBytes` breach raises out of
## `next`, and the underlying stream must not be left open behind it. Plain HTTP
## over a loopback socket (no TLS), so it runs on this host; the TLS feature path
## is exercised in the Docker/CI interop suite.
import unittest
import std/[net, options, strutils]
import navi
import navi/proto/sse   # maxSseEventBytes

# A one-shot loopback server that speaks a valid SSE response and then streams an
# unbounded single event: `data:` lines with no terminating blank line, so the
# parser accumulates them until the cap trips. It never stops on its own; the
# client is expected to tear the connection down, which surfaces here as a failing
# send (caught, so the worker thread exits cleanly either way).
#
# The flood is sent with `flags = {}`: std/net's default `SafeDisconn` swallows a
# peer reset inside `send` WITHOUT advancing its write offset, so the send loop
# spins forever once the client has closed. macOS and Linux never hit that here
# (their loopback buffers absorb the whole flood before the cap trips), but the
# Windows loopback holds far less, so the reset lands mid-flood and the thread
# never returns for joinThread. With no flags the reset raises and ends the loop.
var portChan: Channel[int]

const
  LinePayload = 64 * 1024                    # bytes of `x` per data line
  # enough lines to carry the accumulated event past the cap, with margin
  LineCount = maxSseEventBytes div LinePayload + 8

proc runFloodSse() {.thread.} =
  var srv = newSocket()
  srv.setSockOpt(OptReuseAddr, true)
  srv.bindAddr(Port(0), "127.0.0.1")
  srv.listen()
  portChan.send(srv.getLocalAddr()[1].int)
  var client: Socket
  srv.accept(client)
  var line = ""
  while true:                       # drain the request headers to the blank line
    line = ""
    client.readLine(line, timeout = 2000)
    # std/net readLine yields "" on a closed peer and the sentinel "\c\l" for an
    # empty (blank) line: the blank line is the end of the request headers.
    if line.len == 0 or line == "\c\l": break
  try:
    client.send("HTTP/1.1 200 OK\r\n" &
                "Content-Type: text/event-stream\r\n" &
                "Connection: close\r\n\r\n")
    let dataLine = "data: " & repeat('x', LinePayload) & "\n"
    for _ in 0 ..< LineCount:
      client.send(dataLine, flags = {})   # no blank line: one ever-growing event
  except CatchableError:
    discard                         # the client tore the connection down: expected
  try: client.close() except CatchableError: discard
  try: srv.close() except CatchableError: discard

suite "sync SSE size cap (#292)":
  test "a feed() cap breach should close the underlying handle, not just raise":
    portChan.open()
    var th: Thread[void]
    createThread(th, runFloodSse)
    let port = portChan.recv()             # blocks until the worker has bound + listened
    let api = newNavi()
    let s = api.sse("http://127.0.0.1:" & $port & "/events",
                    reconnect = false, idleTimeoutMs = 10_000)
    var msg = ""
    try:
      discard s.next()                     # reads until the accumulated event trips the cap
    except ValueError as e:
      msg = e.msg
    check "limit" in msg                   # the cap, not some other failure
    # The regression: the breach used to escape `next` with the stream handle still
    # open, leaking the socket (and, on h2, a mux slot) for the life of the stream.
    # `httpVersion` is "" exactly when the handle has been released.
    check s.httpVersion() == ""
    check s.next().isNone                  # no handle, reconnect off: cleanly ended
    s.close()                              # idempotent; unblocks a still-sending server
    joinThread(th)
    portChan.close()
