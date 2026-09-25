# The chronos twin of client_noconnect.nim: the RFC 9220 Extended CONNECT gate
# (issue #393) on the chronos h3 driver. See that file for what is being proved.
import navi/chronos
import navi/core/response   # ProtocolError
import std/[os, strutils, times]

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false          # self-signed test cert
  cfg.http = {H3}                  # WebSocket over h3 Extended CONNECT (RFC 9220)
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  let port = getEnv("WS_PORT", "4434")
  let t0 = epochTime()
  try:
    let ws = await api.websocket("wss://127.0.0.1:" & port & "/chat")
    await ws.close()
    doAssert false, "expected a ProtocolError, but the h3 WebSocket opened"
  except ProtocolError as e:
    doAssert "SETTINGS_ENABLE_CONNECT_PROTOCOL" in e.msg,
      "wrong diagnostic: " & e.msg
  except CatchableError as e:
    doAssert false, "expected ProtocolError, got " & $e.name & ": " & e.msg
  let ms = int((epochTime() - t0) * 1000)
  doAssert ms < 10_000, "the gate was not fast: " & $ms & "ms"
  echo "H3_WS_NOCONNECT_OK ms=", ms
  await api.close()

waitFor main()
