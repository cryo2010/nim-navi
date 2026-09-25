## navi/js WebSocket client for the interop test. Connects to the native server,
## exercises text/binary/close and the streaming reader's `closeCode` (an explicit
## code, a codeless close, and an abrupt drop), and asserts the results. A failed
## assert rejects the promise, which exits Node non-zero.
import navi/js

const
  base = "ws://127.0.0.1:9500"
  explicitCode = 4001'u16     ## matches jsws_server.nim

proc echoRound(api: Navi) {.async.} =
  let ws = await api.websocket(base & "/chat")

  await ws.send("hello from navi/js")
  let m1 = await ws.receive()
  doAssert m1.kind == wmText, "expected text"
  doAssert m1.data == "hello from navi/js", "text echo mismatch: " & m1.data

  await ws.send("\x01\x02\x03\xff", binary = true)
  let m2 = await ws.receive()
  doAssert m2.kind == wmBinary, "expected binary"
  doAssert m2.data.len == 4, "binary length mismatch: " & $m2.data.len

  # The reserved local-use codes may never be sent, as on the native clients.
  var raised = false
  try: await ws.close(closeNoStatus)
  except ValueError: raised = true
  doAssert raised, "close(closeNoStatus) must raise while the socket is open"

  await ws.close()

proc closeCodeRound(api: Navi, path: string, want: uint16) {.async.} =
  ## Open `path`, poke the server so it runs its close scenario, and assert the
  ## code the streaming reader reports.
  let ws = await api.websocket(base & path)
  await ws.send("go")                      # the server closes once it sees this
  let r = await ws.stream()
  doAssert r.kind == wmClose,
    path & ": expected the stream to end in a close, got " & $r.kind
  doAssert r.closeCode == want,
    path & ": closeCode mismatch: got " & $r.closeCode & ", want " & $want
  doAssert (await r.readChunk()) == "", path & ": a close reader has no payload"
  # Mirroring the reported code back is a no-op once the socket is closed, even
  # for a reserved one (parity with the native clients' documented behaviour).
  await ws.close(r.closeCode)

proc main() {.async.} =
  let api = newNavi()
  await api.echoRound()
  await api.closeCodeRound("/close-code", explicitCode)
  await api.closeCodeRound("/close-nostatus", closeNoStatus)
  await api.closeCodeRound("/abrupt", closeAbnormal)
  echo "navi/js websocket ok"

discard main()
