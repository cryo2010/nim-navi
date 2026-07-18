## WebSocket-over-TLS (wss) echo over the synchronous backend. Same as sync.nim,
## just a wss:// URL and a TlsConfig. Compile with -d:ssl.
##
##   nim c -r -d:ssl examples/websocket/wss_echo_server.nim   # in one terminal
##   nim c -r -d:ssl examples/websocket/wss_sync.nim           # in another

import navi

const message = "hello from the sync backend (wss)"

# verify is off because the demo server uses a self-signed cert. A real
# deployment would verify against a trusted CA: TlsConfig(caFile: "ca.pem").
let api = newNavi(NaviOptions(tls: TlsConfig(verify: some(false))))
let ws = api.websocket("wss://127.0.0.1:9701/")
ws.send(message)
echo "sent: ", message
let reply = ws.receive()
echo "recv: ", reply.data
doAssert reply.kind == wmText and reply.data == message
ws.close()
echo "ok"
