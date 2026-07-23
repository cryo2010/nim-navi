## WebSocket-over-TLS (wss) echo over the chronos backend. Compile with -d:ssl.
##
##   nim c -r -d:ssl examples/websocket/wss_echo_server.nim   # in one terminal
##   nim c -r -d:ssl examples/websocket/wss_chronos.nim        # in another
##
## chronos (BearSSL) can verify against a custom CA via TlsConfig(caFile: ...),
## but the demo server's cert is self-signed with no separate CA, so this uses
## verify off. Client certificates (mTLS) are still not supported on this backend.

import pkg/chronos
import navi/chronos

const message = "hello from the chronos backend (wss)"

proc main() {.async.} =
  var cfg = newNaviConfig()
  cfg.tls.verify = false   # the demo server uses a self-signed cert
  let api = newNavi(cfg)
  let ws = await api.websocket("wss://127.0.0.1:9701/")
  await ws.send(message)
  echo "sent: ", message
  let reply = await ws.receive()
  echo "recv: ", reply.data
  doAssert reply.kind == wmText and reply.data == message
  await ws.close()
  echo "ok"

waitFor main()
