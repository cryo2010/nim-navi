## WebSocket echo over the JavaScript backend, rendered into the page.
##
## Compile to JS, then open index.html in a browser with the echo server running:
##   nim c -r examples/websocket/echo_server.nim                      # terminal 1
##   nim js -o:examples/websocket/navi_ws.js examples/websocket/js.nim
##   (serve this folder, e.g. `python3 -m http.server`, and open index.html)
##
## See examples/websocket/README.md for the full walkthrough.

import navi/js
import std/dom

const message = "hello from the navi/js backend"

proc log(line: string) =
  let el = document.getElementById("out")
  el.innerHTML = cstring($el.innerHTML & line & "\n")

proc main() {.async.} =
  try:
    let api = newNavi()
    log("connecting to ws://127.0.0.1:9700/ ...")
    let ws = await api.websocket("ws://127.0.0.1:9700/")
    log("connected")
    await ws.send(message)
    log("sent: " & message)
    let reply = await ws.receive()
    log("recv: " & reply.data)
    if reply.kind == wmText and reply.data == message:
      log("ok - the server echoed the message back")
    else:
      log("unexpected reply")
    await ws.close()
  except CatchableError as e:
    log("error: " & e.msg & "  (is echo_server.nim running?)")

discard main()
