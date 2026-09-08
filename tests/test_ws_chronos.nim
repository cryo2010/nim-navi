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
