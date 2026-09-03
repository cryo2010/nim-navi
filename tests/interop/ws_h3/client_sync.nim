import navi
import std/os

var cfg = initNaviConfig()
cfg.tls.verify = false          # self-signed test cert
cfg.http = {H3}                  # WebSocket over h3 Extended CONNECT (RFC 9220)
cfg.retry.limit = 0
let api = newNavi(cfg)
let port = getEnv("WS_PORT", "4433")
let ws = api.websocket("wss://127.0.0.1:" & port & "/chat")
ws.send("hello over h3 sync")
let msg = ws.receive()
doAssert msg.kind == wmText, "expected text, got " & $msg.kind
doAssert msg.data == "hello over h3 sync", "echo mismatch: [" & msg.data & "]"
echo "H3_WS_SYNC_OK payload=", msg.data
ws.close()
api.close()
