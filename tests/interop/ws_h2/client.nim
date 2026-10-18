import navi/asyncdispatch
import std/[os, strutils]

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false          # self-signed test cert
  cfg.http = {H2}                  # force WebSocket over h2 Extended CONNECT
  cfg.retry.limit = 0
  # A response-body cap far below what the tunnel carries below (#470): it bounds
  # a response body, never a WebSocket, so the tunnel must outlive it.
  cfg.maxResponseBytes = 1024
  let api = newNavi(cfg)
  let port = getEnv("WS_PORT", "8443")
  let ws = await api.websocket("wss://127.0.0.1:" & port & "/chat")
  await ws.send("hello over h2")
  let msg = await ws.receive()
  doAssert msg.kind == wmText, "expected text, got " & $msg.kind
  doAssert msg.data == "hello over h2", "echo mismatch: [" & msg.data & "]"

  # 1. Well over maxResponseBytes of frames, in messages each under the cap: the
  #    tunnel used to be RST once its frames passed the cap in total.
  var total = 0
  for i in 1 .. 16:
    let p = align($i, 4, '0') & "-".repeat(508)    # 512 bytes
    await ws.send(p)
    let r = await ws.receive()
    doAssert r.kind == wmText and r.data == p, "capped round-trip " & $i & " mismatch"
    total += r.data.len
  # 2. One message larger than the cap on its own.
  let big = "navi-h2-cap-".repeat(1000)               # 12 KB
  await ws.send(big, binary = true)
  let echoed = await ws.receive()
  doAssert echoed.kind == wmBinary and echoed.data == big, "big echo mismatch"
  total += echoed.data.len
  await ws.close()

  # 3. maxMessageBytes is still the WebSocket's size bound.
  let small = await api.websocket("wss://127.0.0.1:" & port & "/chat",
                                  maxMessageBytes = 256)
  await small.send("x".repeat(512))
  var tooBig = false
  try: discard await small.receive()
  except WsMessageTooLarge: tooBig = true
  doAssert tooBig, "maxMessageBytes did not reject a 512-byte message"
  await small.close()

  # 4. A plain h2 response on the same client still honours maxResponseBytes.
  var capped = false
  try: discard await api.get("https://127.0.0.1:" & port & "/big")
  except ResponseTooLargeError: capped = true
  doAssert capped, "a 4 KiB h2 response passed maxResponseBytes = 1024"

  echo "H2_WS_OK payload=", msg.data, " tunnelled=", total, " cap=", cfg.maxResponseBytes
  await api.close()

waitFor main()
