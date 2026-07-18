## WebSocket-over-TLS (wss) echo over the std/asyncdispatch backend. Compile with -d:ssl.
##
##   nim c -r -d:ssl examples/websocket/wss_echo_server.nim      # in one terminal
##   nim c -r -d:ssl examples/websocket/wss_asyncdispatch.nim     # in another

import navi/asyncdispatch

const message = "hello from the asyncdispatch backend (wss)"

proc main() {.async.} =
  # verify is off for the demo's self-signed cert (use caFile in production).
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
