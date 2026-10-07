# The verified WebSocket echo loop, shared by clients/ws.nim and the ws slice of
# clients/mixed.nim. `include`d (not imported) AFTER the including file's
# `import navi[/backend]` + `include ../common/httpset`, like common/httpset and
# common/chaos, because WebSocket/WsMessage/Future come from that backend. Not a
# standalone module. Needs std/times, `from std/os import getEnv` and
# ../common/[config, reporter] from the including file.
#
# One round-trip is a text echo plus a binary echo, both byte-verified; a peer
# close ends the socket normally, a mismatch or a transport error tallies a
# failure and stops that socket (the cell's zero-work check is what fails a
# wholly dead slice).

let logErrors = getEnv("NAVI_LOG_ERRORS").len > 0

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

proc wsWorker(ws: WebSocket, counter: StatusCounter, deadline: float) {.async.} =
  try:
    while epochTime() < deadline:
      await ws.send("ping")
      let t = await ws.receive()
      if t.kind == wmClose: break          # peer closed (e.g. the close handshake as
                                           # the soak winds down): a normal end, not a fail
      if t.kind != wmText or t.data != "ping":
        counter.fail()
        if logErrors: stderr.writeLine("ws text mismatch: kind=" & $t.kind & " len=" & $t.data.len)
        break
      await ws.send("bytes", binary = true)
      let b = await ws.receive()
      if b.kind == wmClose: break
      if b.kind != wmBinary or b.data != "bytes":
        counter.fail()
        if logErrors: stderr.writeLine("ws bin mismatch: kind=" & $b.kind & " len=" & $b.data.len)
        break
      counter.tally(200)
  except CatchableError as e:
    counter.fail()
    if logErrors: stderr.writeLine("ws err: " & $e.name & ": " & e.msg)
  try: await ws.close()                    # closing an already-closing socket is not a failure
  except CatchableError: discard
