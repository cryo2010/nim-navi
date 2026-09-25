## Async WebSocket client (navi/asyncdispatch) against an in-process echo server
## built from the same sans-io core (server frames unmasked).

import unittest
import std/os
import navi/asyncdispatch
import navi/proto/ws        # WebSocket message types (wmText, closeNormal, ...)
import navi/core/response   # navi's TimeoutError (qualified; std/net has one too)
import ./support_ws         # shared WebSocket test servers (WsSrv / startWs*)

suite "async websocket client end to end":
  test "the WebSocket client should handshake, echo text and binary, reassemble fragments, and close":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    proc run() {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")

      await ws.send("hello")
      let m1 = await ws.receive()
      check m1.kind == wmText
      check m1.data == "hello"

      await ws.send("\x00\x01\x02 bytes", binary = true)
      let m2 = await ws.receive()
      check m2.kind == wmBinary
      check m2.data == "\x00\x01\x02 bytes"

      await ws.send("please fragment")
      let m3 = await ws.receive()
      check m3.kind == wmText
      check m3.data == "frag-ment"

      await ws.send("bye")
      let m4 = await ws.receive()
      check m4.kind == wmClose
      check m4.closeCode == closeNormal
      await ws.close()

    waitFor run()

  test "keepalive should raise TimeoutError when the peer never responds":
    var th: Thread[WsSrv]
    var port: int
    startWsSilent(th, port)

    proc run2() {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat", keepAlive = 40)
      expect response.TimeoutError:
        discard await ws.receive()             # ping at 40ms, dead at 80ms (no pong)

    waitFor run2()
    joinThread(th)

  test "stream()/stream(writer) should read and write a message as fragments":
    var th: Thread[WsSrv]
    var port: int
    startWsStreamEcho(th, port)

    proc run3() {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")

      await ws.send("fragment")                  # server replies with 3 fragments
      let reader = await ws.stream()
      check reader.kind == wmText
      var chunks: seq[string]
      reader.each(chunk):
        chunks.add chunk
      check chunks == @["one", "-two", "-three"]

      ws.stream(writer):                         # streamed upload; server echoes whole
        for part in @["aa", "bb", "cc"]:
          await writer.write(part)
      let m = await ws.receive()
      check m.kind == wmText
      check m.data == "aabbcc"
      await ws.close()

    waitFor run3()
    joinThread(th)

  test "the WebSocket client should time out on open when the server never completes the handshake":
    # The whole open is bounded by timeouts.total (parity with chronos): a server
    # that accepts the TCP connection but never sends the 101 must not hang forever.
    var th: Thread[WsSrv]
    var stallPort: int
    startWsStall(th, stallPort)

    proc run(): Future[string] {.async.} =
      var cfg = initNaviConfig()
      cfg.timeouts.total = 600
      let api = newNavi(cfg)
      try:
        discard await api.websocket("ws://127.0.0.1:" & $stallPort & "/")
        return "opened"
      except CatchableError as e:
        # navi raises its own TimeoutError; match by name to avoid the
        # std/net vs navi ambiguity on the bare type.
        return (if $e.name == "TimeoutError": "timeout" else: "other:" & $e.name)

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "timeout"

suite "async websocket protocol-error teardown (#281)":
  # A protocol error from the peer must fail the connection (RFC 6455 7.1.7), not
  # just raise: the transport has to be torn down, else it leaks and the decoder
  # stays desynced. The server reports whether the client actually dropped it.
  test "receive should fail the connection when the server sends a masked frame":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    # The outcome travels back as a string, matching the chronos file: strict
    # exception tracking there rejects unittest's check/expect inside an async proc.
    proc run(): Future[string] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("masked")
      try:
        discard await ws.receive()
        result = "no error"
      except ValueError as e:
        result = "raised:" & $e.name
      # deliberately no close(): the driver must have torn the transport down
      # itself, which is exactly what `sawEof` below proves.

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "raised:ValueError"
    check sawEof                               # torn down, not leaked

  test "receive should fail the connection on invalid UTF-8 in a text message":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    proc run(): Future[string] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("badutf8")
      try:
        discard await ws.receive()
        result = "no error"
      except ValueError as e:
        result = "raised:" & $e.name

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "raised:ValueError"
    check sawEof

suite "async websocket streaming text validation (#282)":
  # The streaming read path never buffers the whole message, so it validates each
  # chunk as it arrives instead of relying on the assembler's whole-message check.
  test "a streamed text message should be rejected when a code point is invalid across frames":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    proc run(): Future[string] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("splitbad")
      let r = await ws.stream()
      try:
        while (await r.readChunk()).len > 0: discard
        result = "no error"
      except ValueError as e:
        result = "raised:" & $e.name

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "raised:ValueError"
    check sawEof

  test "a streamed text message should accept a code point split across frames":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false                         # not asserted here: nothing failed
    startWsMisbehave(th, port, sawEof)

    proc run(): Future[string] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("splitok")
      let r = await ws.stream()
      while true:
        let chunk = await r.readChunk()
        if chunk.len == 0: break
        result.add chunk
      await ws.close()

    let msg = waitFor run()
    joinThread(th)
    check msg == "\xf0\x9f\x92\xa9"            # U+1F4A9, whole again

suite "async websocket streaming desync teardown (#284)":
  # The streaming read path desyncs on the same protocol errors, and only `drain`
  # used to tear down: a direct readChunk/stream() left the transport alive.
  test "stream() should fail the connection when a message starts with a continuation":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    proc run(): Future[string] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("orphan")
      try:
        discard await ws.stream()
        result = "no error"
      except IOError as e:
        result = "raised:" & $e.name

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "raised:IOError"
    check sawEof

  test "readChunk should fail the connection on a data frame where a continuation is due":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    proc run(): Future[string] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("datamid")
      let r = await ws.stream()
      result = await r.readChunk()             # the opening fragment
      try:
        discard await r.readChunk()            # a new text frame, not a continuation
        result.add "|no error"
      except IOError as e:
        result.add "|raised:" & $e.name

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "aa|raised:IOError"
    check sawEof

suite "WebSocket transport selection (asyncdispatch)":
  test "websocket over {H2} on a non-TLS URL should raise ProtocolError":
    # {H2} excludes h1; h2 needs TLS, so a ws:// (plaintext) target has no usable
    # transport -> ProtocolError before any connection is attempted.
    proc run() {.async.} =
      var cfg = initNaviConfig()
      cfg.http = {H2}
      let api = newNavi(cfg)
      var raised = false
      try: discard await api.websocket("ws://127.0.0.1:1/never")
      except ProtocolError: raised = true
      check raised
      await api.close()
    waitFor run()
