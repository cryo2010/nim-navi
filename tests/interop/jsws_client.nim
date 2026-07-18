## navi/js WebSocket client for the interop test. Connects to the native echo
## server, exercises text/binary/close, and asserts the echoes. A failed assert
## rejects the promise, which exits Node non-zero.
import navi/js

proc main() {.async.} =
  let api = newNavi()
  let ws = await api.websocket("ws://127.0.0.1:9500/chat")

  await ws.send("hello from navi/js")
  let m1 = await ws.receive()
  doAssert m1.kind == wmText, "expected text"
  doAssert m1.data == "hello from navi/js", "text echo mismatch: " & m1.data

  await ws.send("\x01\x02\x03\xff", binary = true)
  let m2 = await ws.receive()
  doAssert m2.kind == wmBinary, "expected binary"
  doAssert m2.data.len == 4, "binary length mismatch: " & $m2.data.len

  await ws.close()
  echo "navi/js websocket ok"

discard main()
