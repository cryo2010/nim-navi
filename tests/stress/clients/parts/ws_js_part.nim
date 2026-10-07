# The verified WebSocket echo loop for the navi/js backend, shared by
# clients/ws_js.nim and the ws slice of clients/mixed_js.nim. `include`d (not
# imported) AFTER the including file's `import navi/js`, like the native parts.
# Not a standalone module. Needs ../common/harness_js from the including file.
#
# One round-trip is a text echo plus a binary echo, both byte-verified; a
# mismatch or a transport error notes a failure and stops that socket (the
# cell's zero-work check is what fails a wholly dead slice).

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

proc wsWorker(ws: WebSocket, counter: JsCounter, deadline: float) {.async.} =
  try:
    while nowMs() < deadline:
      await ws.send("ping")
      let t = await ws.receive()
      if t.kind != wmText or t.data != "ping": counter.note(); break
      await ws.send("bytes", binary = true)
      let b = await ws.receive()
      if b.kind != wmBinary or b.data != "bytes": counter.note(); break
      counter.tally(200)
    await ws.close()
  except CatchableError:
    counter.note()
