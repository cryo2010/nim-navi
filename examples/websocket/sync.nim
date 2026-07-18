## WebSocket echo over the synchronous backend.
##
## Start the echo server first, then run this:
##   nim c -r examples/websocket/echo_server.nim   # in one terminal
##   nim c -r examples/websocket/sync.nim           # in another

import navi

const message = "hello from the sync backend"

let api = newNavi()
let ws = api.websocket("ws://127.0.0.1:9700/")
ws.send(message)
echo "sent: ", message
let reply = ws.receive()
echo "recv: ", reply.data
doAssert reply.kind == wmText and reply.data == message
ws.close()
echo "ok"
