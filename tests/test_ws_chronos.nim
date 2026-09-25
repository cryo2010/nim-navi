## Chronos WebSocket client (navi/chronos) against an in-process echo server
## built from the same sans-io core (server frames unmasked).

import unittest
import std/os
import pkg/chronos
import navi/chronos
import navi/proto/ws        # WebSocket message types (WsMessage, wmText, closeNormal, ...)
import ./support_ws         # shared WebSocket test servers (WsSrv / startWs*)


suite "chronos websocket client end to end":
  test "the WebSocket client should handshake, echo text and binary, reassemble fragments, and close":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    # Checks live in the sync body: chronos's strict exception tracking rejects
    # unittest's `check` (which can raise) inside an {.async.} proc.
    proc run(): Future[seq[WsMessage]] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("hello")
      result.add await ws.receive()
      await ws.send("\x00\x01\x02 bytes", binary = true)
      result.add await ws.receive()
      await ws.send("please fragment")
      result.add await ws.receive()
      await ws.send("bye")
      result.add await ws.receive()
      await ws.close()

    let m = waitFor run()
    joinThread(th)
    check m.len == 4
    check m[0].kind == wmText and m[0].data == "hello"
    check m[1].kind == wmBinary and m[1].data == "\x00\x01\x02 bytes"
    check m[2].kind == wmText and m[2].data == "frag-ment"
    check m[3].kind == wmClose and m[3].closeCode == closeNormal

  test "the WebSocket client should time out on open when the server never completes the handshake":
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

  test "keepalive should time out when the peer never responds":
    var th: Thread[WsSrv]
    var silentPort: int
    startWsSilent(th, silentPort)

    proc run(): Future[string] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $silentPort & "/",
                                   keepAlive = 40)
      try:
        discard await ws.receive()             # ping at 40ms, dead at 80ms (no pong)
        return "received"
      except CatchableError as e:
        return (if $e.name == "TimeoutError": "timeout" else: "other:" & $e.name)

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "timeout"

  test "stream()/stream(writer) should read and write a message as fragments":
    var th: Thread[WsSrv]
    var port: int
    startWsStreamEcho(th, port)

    proc run(): Future[tuple[chunks: seq[string], echoed: string]] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("fragment")
      let reader = await ws.stream()
      doAssert reader.kind == wmText
      var got: seq[string]                       # a local, not `result` (captured by each)
      reader.each(chunk):
        got.add chunk
      ws.stream(writer):
        for part in @["aa", "bb", "cc"]:
          await writer.write(part)
      let echoed = (await ws.receive()).data
      await ws.close()
      return (got, echoed)

    let (chunks, echoed) = waitFor run()
    joinThread(th)
    check chunks == @["one", "-two", "-three"]
    check echoed == "aabbcc"

suite "chronos websocket lifecycle guards (#289)":
  test "send and ping should raise on a closed WebSocket":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    proc run(): Future[string] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.close()
      try:
        await ws.send("too late")
        result = "sent"
      except IOError: result = "send raised"
      try:
        await ws.ping()
        result.add "|pinged"
      except IOError: result.add "|ping raised"

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "send raised|ping raised"

  test "close should reject the close codes reserved for local use":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    proc run(): Future[int] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      for code in [closeNoStatus, closeAbnormal, 1015'u16]:
        try: await ws.close(code)
        except ValueError: inc result
      await ws.close()                         # a valid code still works

    let rejected = waitFor run()
    joinThread(th)
    check rejected == 3

  test "a transport EOF should surface as 1006 on both read paths":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    proc run(): Future[uint16] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("eofnow")                  # dropped with no close frame
      let m = await ws.receive()
      result = m.closeCode

    let code = waitFor run()
    joinThread(th)
    check code == closeAbnormal

    var th2: Thread[WsSrv]
    var port2: int
    var sawEof2 = false
    startWsMisbehave(th2, port2, sawEof2)

    proc run2(): Future[uint16] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port2 & "/chat")
      await ws.send("eofnow")
      let r = await ws.stream()                # the streaming path reports the same
      result = r.closeCode

    let code2 = waitFor run2()
    joinThread(th2)
    check code2 == closeAbnormal

suite "chronos websocket protocol-error teardown (#281)":
  # A protocol error from the peer must fail the connection (RFC 6455 7.1.7), not
  # just raise: the transport has to be torn down, else it leaks and the decoder
  # stays desynced. The server reports whether the client actually dropped it.
  test "receive should fail the connection when the server sends a masked frame":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

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

suite "chronos websocket streaming text validation (#282)":
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

suite "chronos websocket streaming desync teardown (#284)":
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

import navi/core/response as resp   # ProtocolError

suite "WebSocket transport selection (chronos)":
  test "websocket over {H2} on a non-TLS URL should raise ProtocolError":
    # {H2} excludes h1; h2 needs TLS, so a ws:// (plaintext) target has no usable
    # transport. `check` runs outside the async proc (chronos strict-raises).
    proc run(): Future[bool] {.async.} =
      var cfg = initNaviConfig()
      cfg.http = {H2}
      let api = newNavi(cfg)
      result = false
      try: discard await api.websocket("ws://127.0.0.1:1/never")
      except resp.ProtocolError: result = true
      await api.close()
    check waitFor run()
