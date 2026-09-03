import navi/chronos
import std/os

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false          # self-signed test cert
  cfg.http = {H3}                  # force WebSocket over h3 Extended CONNECT (RFC 9220)
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  let port = getEnv("WS_PORT", "4433")
  let ws = await api.websocket("wss://127.0.0.1:" & port & "/chat")
  await ws.send("hello over h3")
  let msg = await ws.receive()
  doAssert msg.kind == wmText, "expected text, got " & $msg.kind
  doAssert msg.data == "hello over h3", "echo mismatch: [" & msg.data & "]"
  echo "H3_WS_OK payload=", msg.data
  await ws.close()
  await api.close()

waitFor main()
