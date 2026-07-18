## WebSocket-over-TLS (wss) echo over the chronos backend. Compile with -d:ssl.
##
##   nim c -r -d:ssl examples/websocket/wss_echo_server.nim   # in one terminal
##   nim c -r -d:ssl examples/websocket/wss_chronos.nim        # in another
##
## chronos uses BearSSL, which can't add a custom CA, so a self-signed cert
## always needs verify off (TlsConfig(caFile: ...) is not honored here).

import pkg/chronos
import navi/chronos

const message = "hello from the chronos backend (wss)"

proc main() {.async.} =
  let api = newNavi(NaviOptions(tls: TlsConfig(verify: some(false))))
  let ws = await api.websocket("wss://127.0.0.1:9701/")
  await ws.send(message)
  echo "sent: ", message
  let reply = await ws.receive()
  echo "recv: ", reply.data
  doAssert reply.kind == wmText and reply.data == message
  await ws.close()
  echo "ok"

waitFor main()
