## JavaScript WebSocket: a thin wrapper over the runtime's native `WebSocket`.
##
## The browser (or Node) does the RFC 6455 handshake and framing, so unlike the
## native backends this does not use `proto/ws`. The one real job is bridging the
## browser's event-callback model (`onmessage`/`onclose`) to navi's async
## `receive()`: incoming messages are queued, and a pending `receive` is resolved
## as they arrive. JavaScript-only, compiled solely through `import navi/js`.

when not defined(js):
  {.error: "navi/backend/jsws is JavaScript-only; compile with `nim js` via `import navi/js`.".}

import std/[asyncjs, jsffi]

type
  WsMessageKind* = enum wmText, wmBinary, wmClose
  WsMessage* = object
    ## A received message. `data` is the payload (text, or bytes as a byte-string
    ## for binary) and the reason for a close; `closeCode` is set for `wmClose`.
    kind*: WsMessageKind
    data*: string
    closeCode*: uint16

  WebSocket* = ref object
    raw: JsObject
    queue: seq[WsMessage]           ## messages received before a `receive` awaited them
    resolve: proc(m: WsMessage)     ## resolver of a pending `receive`, or nil
    open: bool

const
  closeNormal* = 1000'u16
  closeGoingAway* = 1001'u16

# --- native WebSocket bindings ---
proc jsNewSocket(url: cstring): JsObject {.importjs: "new WebSocket(#)".}
proc jsSetBinary(s: JsObject) {.importjs: "#.binaryType = 'arraybuffer'".}
proc jsAddOpen(s: JsObject, cb: proc()) {.importjs: "#.addEventListener('open', #)".}
proc jsAddError(s: JsObject, cb: proc()) {.importjs: "#.addEventListener('error', #)".}
proc jsAddMessage(s: JsObject, cb: proc(ev: JsObject)) {.importjs: "#.addEventListener('message', #)".}
proc jsAddClose(s: JsObject, cb: proc(ev: JsObject)) {.importjs: "#.addEventListener('close', #)".}
proc jsSendText(s: JsObject, data: cstring) {.importjs: "#.send(#)".}
proc jsSendBin(s: JsObject, data: JsObject) {.importjs: "#.send(#)".}
proc jsClose(s: JsObject, code: int, reason: cstring) {.importjs: "#.close(#, #)".}
proc jsReadyState(s: JsObject): int {.importjs: "#.readyState".}

# --- event-payload helpers ---
# Binary crosses the JS boundary as raw bytes (a Uint8Array indexed with ord()),
# not via cstring, because a Nim js string <-> JS string conversion transcodes
# UTF-8/UTF-16 and mangles bytes > 127.
proc dataIsString(ev: JsObject): bool {.importjs: "(typeof #.data === 'string')".}
proc dataAsString(ev: JsObject): cstring {.importjs: "#.data".}
proc evCode(ev: JsObject): int {.importjs: "#.code".}
proc u8View(ev: JsObject): JsObject {.importjs: "new Uint8Array(#.data)".}
proc u8Len(v: JsObject): int {.importjs: "#.length".}
proc u8At(v: JsObject, i: int): int {.importjs: "#[#]".}
proc u8New(n: int): JsObject {.importjs: "new Uint8Array(#)".}
proc u8Set(v: JsObject, i, b: int) {.importjs: "#[#] = #".}

proc bytesOf(ev: JsObject): string =
  ## Copy the message's ArrayBuffer into a Nim string, one byte per char.
  let v = u8View(ev)
  let n = u8Len(v)
  result = newString(n)
  for i in 0 ..< n: result[i] = char(u8At(v, i))

proc toU8(s: string): JsObject =
  ## A Uint8Array of `s`'s bytes (ord of each char), byte-exact.
  result = u8New(s.len)
  for i in 0 ..< s.len: result.u8Set(i, ord(s[i]))

proc deliver(ws: WebSocket, m: WsMessage) =
  ## Hand a message to a waiting `receive`, or queue it for the next one.
  if ws.resolve != nil:
    let r = ws.resolve
    ws.resolve = nil
    r(m)
  else:
    ws.queue.add(m)

proc openWebSocket*(url: string): Future[WebSocket] {.async.} =
  ## Construct the native WebSocket and resolve once it opens (or raise on error).
  let raw = jsNewSocket(cstring(url))
  jsSetBinary(raw)
  let ws = WebSocket(raw: raw)
  jsAddMessage(raw, proc(ev: JsObject) =
    if dataIsString(ev):
      ws.deliver(WsMessage(kind: wmText, data: $dataAsString(ev)))
    else:
      ws.deliver(WsMessage(kind: wmBinary, data: bytesOf(ev))))
  jsAddClose(raw, proc(ev: JsObject) =
    ws.open = false
    ws.deliver(WsMessage(kind: wmClose, closeCode: uint16(evCode(ev)))))
  await newPromise(proc(resolve: proc()) =
    jsAddOpen(raw, proc() = resolve())
    jsAddError(raw, proc() = resolve()))       # error resolves too; readyState check below
  if jsReadyState(raw) != 1:                    # 1 == OPEN
    raise newException(IOError, "navi: websocket failed to open")
  ws.open = true
  result = ws

proc send*(ws: WebSocket, data: string, binary = false): Future[void] {.async.} =
  ## Send a text (default) or binary message. Returns a (resolved) Future so the
  ## call site matches the native async backends (`await ws.send(...)`).
  if binary: ws.raw.jsSendBin(toU8(data))
  else: ws.raw.jsSendText(cstring(data))

proc receive*(ws: WebSocket): Future[WsMessage] {.async.} =
  ## Await the next message. Ping/pong are handled by the runtime; a close
  ## arrives as `wmClose`.
  if ws.queue.len > 0:
    result = ws.queue[0]
    ws.queue.delete(0)
  else:
    result = await newPromise(proc(resolve: proc(m: WsMessage)) =
      ws.resolve = resolve)

proc close*(ws: WebSocket, code = closeNormal, reason = ""): Future[void] {.async.} =
  ## Close the connection. Idempotent. Returns a Future to match the native
  ## async backends (`await ws.close()`).
  if not ws.open: return
  ws.open = false
  ws.raw.jsClose(int(code), cstring(reason))
