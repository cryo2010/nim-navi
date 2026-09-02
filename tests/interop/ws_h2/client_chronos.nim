import navi/chronos
import std/os

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false          # self-signed test cert
  cfg.http = {H2}                  # force WebSocket over h2 Extended CONNECT
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  let port = getEnv("WS_PORT", "8443")
  let ws = await api.websocket("wss://127.0.0.1:" & port & "/chat")
  await ws.send("hello over h2")
  let msg = await ws.receive()
  doAssert msg.kind == wmText, "expected text, got " & $msg.kind
  doAssert msg.data == "hello over h2", "echo mismatch: [" & msg.data & "]"
  echo "H2_WS_OK payload=", msg.data
  await ws.close()
  await api.close()

waitFor main()
