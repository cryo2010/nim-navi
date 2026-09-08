## Async WebSocket client (navi/asyncdispatch) against an in-process echo server
## built from the same sans-io core (server frames unmasked).

import unittest
import std/os
import navi/asyncdispatch
import navi/proto/ws        # WebSocket message types (wmText, closeNormal, ...)
import navi/core/response   # navi's TimeoutError (qualified; std/net has one too)
import ./support            # shared WebSocket test servers (WsSrv / startWs*)

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
