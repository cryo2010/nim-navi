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
    maxMessageBytes: int            ## cap on a received message; 0 = unlimited

  WsMessageTooLarge* = object of CatchableError
    ## Raised by `receive` when a message exceeds `maxMessageBytes`. On js the
    ## runtime has already buffered it (and applies its own limit), so this is an
    ## API-consistency check with the native backends rather than a memory guard.

const
  # Close codes (RFC 6455 7.4.1); the same set the native clients re-export, so
  # cross-client code can name them without a backend switch.
  closeNormal* = 1000'u16
  closeGoingAway* = 1001'u16
  closeProtocolError* = 1002'u16
  closeMessageTooBig* = 1009'u16   ## RFC 6455 7.4.1: a message exceeded a size limit
  closeNoStatus* = 1005'u16
    ## RFC 6455 7.1.5: surfaced (never sent on the wire) when a close frame carries
    ## no status code, so an application can distinguish it from an explicit 1000.
    ## The runtime reports it as the `CloseEvent` code, so navi surfaces it as-is.
  closeAbnormal* = 1006'u16
    ## RFC 6455 7.4.1: reserved, never sent on the wire. Surfaced locally when the
    ## transport ends without a close frame, so an abrupt EOF is distinguishable
    ## from a clean closure. The runtime reports it as the `CloseEvent` code, so
    ## navi surfaces it as-is.

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

proc openWebSocket*(url: string, maxMessageBytes = 0): Future[WebSocket] {.async.} =
  ## Construct the native WebSocket and resolve once it opens (or raise on error).
  let raw = jsNewSocket(cstring(url))
  jsSetBinary(raw)
  let ws = WebSocket(raw: raw, maxMessageBytes: maxMessageBytes)
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
  ## arrives as `wmClose`. A message over `maxMessageBytes` closes with 1009 and
  ## raises `WsMessageTooLarge`.
  var m: WsMessage
  if ws.queue.len > 0:
    m = ws.queue[0]
    ws.queue.delete(0)
  else:
    m = await newPromise(proc(resolve: proc(msg: WsMessage)) =
      ws.resolve = resolve)
  if ws.maxMessageBytes > 0 and m.kind != wmClose and
     m.data.len > ws.maxMessageBytes:
    if ws.open:
      ws.open = false
      ws.raw.jsClose(int(closeMessageTooBig), "")
    raise newException(WsMessageTooLarge,
      "navi: WebSocket message exceeds maxMessageBytes (" & $ws.maxMessageBytes & ")")
  return m

proc close*(ws: WebSocket, code = closeNormal, reason = ""): Future[void] {.async.} =
  ## Close the connection. Idempotent. Returns a Future to match the native
  ## async backends (`await ws.close()`).
  ##
  ## `code` must be one that may appear on the wire: 1005, 1006 and 1015 are
  ## reserved for local use (RFC 6455 7.4.1) and raise `ValueError` -- but only
  ## while a close frame would actually be sent. Once the socket is closed there is
  ## nothing to send, so mirroring a code `receive` reported (`ws.close(m.closeCode)`,
  ## which is 1005 or 1006 after a codeless close or an abrupt EOF) is a safe no-op.
  ## Same rule as the native clients; without it the runtime would throw its own
  ## `InvalidAccessError` instead.
  if not ws.open: return
  if code == closeNoStatus or code == closeAbnormal or code == 1015'u16:
    raise newException(ValueError, "navi: WebSocket close code " & $code &
      " is reserved and must never be sent (RFC 6455 7.4.1)")
  ws.open = false
  ws.raw.jsClose(int(code), cstring(reason))

# --- WebSocket streaming (whole-message on js) ---
# The runtime's WebSocket delivers whole messages and fragments outbound internally,
# so js cannot stream at the frame level. These keep the `stream`/`each`/`stream(writer)`
# API compiling and correct, but a read yields the message as a single chunk and a
# write buffers until the block exits (documented parity limitation).

type
  WsSink = proc(chunk: string): Future[void]
  WsReader* = ref object
    ## A message being received incrementally. `kind` is the message type (or
    ## `wmClose` if a close arrived instead). Consume with `each`/`readChunk`.
    ws: WebSocket
    kind*: WsMessageKind
    closeCode*: uint16       ## set when `kind` is wmClose, or when a close ends the
                             ## stream early: the peer's code, 1005 when it sent none,
                             ## 1006 when the transport just ended (as `receive`)
    msg: string
    consumed: bool
  WsWriter* = ref object
    ws: WebSocket
    binary: bool
    buf: string

proc openStreamReader(ws: WebSocket): Future[WsReader] {.async.} =
  ## The runtime delivers whole messages, so a close can never interrupt one
  ## part-way here: it arrives as its own `wmClose`, and its `closeCode` carries
  ## straight over from the `CloseEvent` (which already reports 1005 for a codeless
  ## close and 1006 for a transport that just ended).
  let m = await ws.receive()
  result = WsReader(ws: ws, kind: m.kind, closeCode: m.closeCode, msg: m.data,
                    consumed: m.kind == wmClose)

proc readChunk*(r: WsReader): Future[string] {.async.} =
  ## The message as a single chunk (js cannot sub-stream), then "".
  if r.consumed: return ""
  r.consumed = true
  return r.msg

proc drain*(r: WsReader, sink: WsSink): Future[void] {.async.} =
  let c = await r.readChunk()
  if c.len > 0: await sink(c)

template each*(r: WsReader; chunk, body: untyped): untyped =
  await r.drain(proc(chunk: string): Future[void] {.async.} = body)

template stream*(ws: WebSocket): untyped =
  ## Receive the next message as a (single-chunk) stream; see the note above.
  openStreamReader(ws)

proc write*(w: WsWriter, data: string): Future[void] {.async.} =
  ## Buffer a fragment; the whole message is sent when the `stream` block exits.
  w.buf.add data

proc finishWrite(w: WsWriter): Future[void] {.async.} =
  await w.ws.send(w.buf, binary = w.binary)

template streamOut(ws: WebSocket; writer: untyped; isBinary: bool; body: untyped) =
  block:
    var writer {.inject.} = WsWriter(ws: ws, binary: isBinary)
    body
    await writer.finishWrite()

template stream*(ws: WebSocket; writer, body: untyped): untyped =
  ## Send the next message; `write` buffers fragments, sent whole on block exit.
  streamOut(ws, writer, false, body)

template streamBinary*(ws: WebSocket; writer, body: untyped): untyped =
  ## Like `stream(writer)`, but the message is binary.
  streamOut(ws, writer, true, body)
