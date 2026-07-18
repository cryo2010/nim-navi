## Interactive WebSocket client over the JavaScript backend, driving the page in
## examples/websocket/index.html. Connects to the echo server, shows the live
## connection status, lets you send messages, and reports when the server goes
## away (kill echo_server.nim to see the "disconnected" state).
##
## Compile to JS, then open index.html with the echo server running:
##   nim c -r examples/websocket/echo_server.nim
##   nim js -o:examples/websocket/navi_ws.js examples/websocket/js.nim
##   (serve this folder, e.g. `python3 -m http.server`, and open index.html)

import navi/js
import std/dom

# The page may set `window.NAVI_WS_URL` (see wss_index.html for the wss variant);
# defaults to the plain-ws echo server.
proc configuredUrl(): cstring {.importjs: "(window.NAVI_WS_URL || 'ws://127.0.0.1:9700/')".}
let url = $configuredUrl()

var
  sock: WebSocket
  isOpen = false
  logBuf = ""

proc log(line: string) =
  logBuf.add(line & "\n")
  document.getElementById("log").innerHTML = cstring(logBuf)

proc setStatus(text, cls: string) =
  let el = document.getElementById("status")
  el.innerHTML = cstring(text)
  el.className = cstring(cls)

# std/dom's Element has no typed `.value`; reach it directly.
proc inputValue(el: Element): cstring {.importjs: "#.value".}
proc clearInput(el: Element) {.importjs: "#.value = ''".}

proc listen(ws: WebSocket) {.async.} =
  ## Surface every message, and the close when the peer (or server) goes away.
  while true:
    let msg = await ws.receive()
    case msg.kind
    of wmText: log("recv: " & msg.data)
    of wmBinary: log("recv: <" & $msg.data.len & " binary bytes>")
    of wmClose:
      isOpen = false
      setStatus("disconnected", "off")
      log("-- disconnected (close code " & $msg.closeCode & ") --")
      break

proc connect() {.async.} =
  setStatus("connecting...", "wait")
  log("connecting to " & url & " ...")
  try:
    sock = await newNavi().websocket(url)
    isOpen = true
    setStatus("connected", "on")
    log("-- connected to " & url & " --")
    await listen(sock)
  except CatchableError as e:
    isOpen = false
    setStatus("disconnected", "off")
    log("connect failed: " & e.msg & "  (is echo_server.nim running?)")

proc doSend() =
  let box = document.getElementById("msg")
  let text = $inputValue(box)
  if text.len == 0: return
  if not isOpen:
    log("not connected -- press Reconnect")
    return
  log("send: " & text)
  discard sock.send(text)
  clearInput(box)

# --- wire up the page ---
document.getElementById("send").addEventListener("click", proc(ev: Event) = doSend())
document.getElementById("reconnect").addEventListener("click",
  proc(ev: Event) = (if not isOpen: discard connect()))
document.getElementById("msg").addEventListener("keydown", proc(ev: Event) =
  if $cast[KeyboardEvent](ev).key == "Enter": doSend())

discard connect()
