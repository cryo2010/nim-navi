# Proves the pump thread keeps the connection alive across an app-side idle gap:
# send/receive, then sit idle (no ws activity) for longer than the server's QUIC
# idle timeout, then send/receive again. Survives only because the pump thread is
# emitting keepalive PINGs the whole time. Run with a short server idle timeout and
# NAVI_H3_KEEPALIVE_MS well below it.
import navi
import std/[os, strutils]

var cfg = initNaviConfig()
cfg.tls.verify = false
cfg.http = {H3}
cfg.retry.limit = 0
let api = newNavi(cfg)
let port = getEnv("WS_PORT", "4433")
let gapMs = parseInt(getEnv("WS_IDLE_MS", "6000"))
let ws = api.websocket("wss://127.0.0.1:" & port & "/chat")
ws.send("first")
doAssert ws.receive().data == "first"
sleep(gapMs)                    # app idle: no send/receive; pump must keep QUIC alive
ws.send("after-idle")
let m = ws.receive()
doAssert m.kind == wmText and m.data == "after-idle",
  "idle-gap failed: kind=" & $m.kind & " data=" & m.data
echo "H3_WS_IDLE_OK gap=", gapMs, "ms"
ws.close()
api.close()
