# RFC 9220 / RFC 8441 3 gate (issue #393), asyncdispatch client: against an h3 origin
# whose SETTINGS does NOT enable the Extended CONNECT protocol, navi must refuse to
# open the tunnel -- fast, and with the same ProtocolError the h2 path raises -- rather
# than submit a CONNECT and fail late on a stream reset. The server answers a CONNECT
# with 200 if one arrives, so an ungated client is caught here, not hidden.
import navi/asyncdispatch
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
  # Fast means "as soon as the peer's SETTINGS lands", not "after a timeout".
  doAssert ms < 10_000, "the gate was not fast: " & $ms & "ms"
  echo "H3_WS_NOCONNECT_OK ms=", ms
  await api.close()

waitFor main()
