## WebSocket echo over the std/asyncdispatch backend.
##
##   nim c -r examples/websocket/echo_server.nim      # in one terminal
##   nim c -r examples/websocket/asyncdispatch.nim     # in another

import navi/asyncdispatch

const message = "hello from the asyncdispatch backend"

proc main() {.async.} =
  let api = newNavi()
  let ws = await api.websocket("ws://127.0.0.1:9700/")
  await ws.send(message)
  echo "sent: ", message
  let reply = await ws.receive()
  echo "recv: ", reply.data
  doAssert reply.kind == wmText and reply.data == message
  await ws.close()
  echo "ok"

waitFor main()
