## navi/js WebSocket client for the interop test. Connects to the native server,
## exercises text/binary/close and the streaming reader's `closeCode` (an explicit
## code, a codeless close, and an abrupt drop), and asserts the results. A failed
## assert rejects the promise, which exits Node non-zero.
##
## `binaryRound` is the byte-exactness guard for the bulk Uint8Array <-> Nim string
## conversions in backend/jsws (#412): every byte value 0..255, and a message far
## larger than one frame, must round-trip unchanged in both directions. A mangled
## conversion still compiles, so it has to be run under Node.
import navi/js

const
  base = "ws://127.0.0.1:9500"
  explicitCode = 4001'u16     ## matches jsws_server.nim

proc allBytes(): string =
  ## Every byte value 0..255, once.
  result = newString(256)
  for i in 0 .. 255: result[i] = char(i)

proc binaryRound(api: Navi) {.async.} =
  ## Byte-exact binary echo: the Nim string -> Uint8Array send and the
  ## ArrayBuffer -> Nim string receive must both be lossless for every byte value.
  let ws = await api.websocket(base & "/chat")

  let small = allBytes()
  await ws.send(small, binary = true)
  let m1 = await ws.receive()
  doAssert m1.kind == wmBinary, "expected binary, got " & $m1.kind
  doAssert m1.data.len == 256, "256-byte echo length: " & $m1.data.len
  for i in 0 .. 255:
    doAssert ord(m1.data[i]) == i,
      "byte " & $i & " came back as " & $ord(m1.data[i])

  # A message well past a single small frame, with the pattern crossing every
  # 256-byte boundary, so a chunked or truncated conversion cannot hide.
  const bigLen = 256 * 256          # 64 KiB: extended frame length on the wire
  var big = newString(bigLen)
  for i in 0 ..< bigLen: big[i] = char(i mod 256)
  await ws.send(big, binary = true)
  let m2 = await ws.receive()
  doAssert m2.kind == wmBinary, "expected binary, got " & $m2.kind
  doAssert m2.data.len == bigLen, "64 KiB echo length: " & $m2.data.len
  for i in 0 ..< bigLen:
    doAssert ord(m2.data[i]) == i mod 256,
      "big byte " & $i & " came back as " & $ord(m2.data[i])

  # Several messages in flight at once: they queue, and `receive` must pop them
  # in arrival order (the queue is a deque, popped from the head).
  for n in 0 .. 4:
    await ws.send("q" & $n & ":" & small, binary = true)
  for n in 0 .. 4:
    let m = await ws.receive()
    doAssert m.kind == wmBinary, "queued message " & $n & ": " & $m.kind
    let want = "q" & $n & ":" & small
    doAssert m.data == want, "queued message " & $n & " out of order or corrupted"

  await ws.close()

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
  await api.binaryRound()
  await api.closeCodeRound("/close-code", explicitCode)
  await api.closeCodeRound("/close-nostatus", closeNoStatus)
  await api.closeCodeRound("/abrupt", closeAbnormal)
  echo "navi/js websocket ok"

discard main()
