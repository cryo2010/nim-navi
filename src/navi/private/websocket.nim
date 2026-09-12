## WebSocket (RFC 6455) over h1/h2/h3, plus message streaming.
## `include`d by navi.nim (the sync entry); shares its imports, the `Navi`
## type, and the pooled-transport engine. Not a standalone module.

# --- WebSocket (RFC 6455) ---

export ws.WsMessage, ws.WsMessageKind, ws.closeNormal, ws.closeGoingAway,
       ws.closeMessageTooBig, ws.WsMessageTooLarge

type
  WsKind = enum wkH1, wkH2, wkH3
  WsH2 = ref object
    ## Blocking driver for a WebSocket-over-h2 tunnel (RFC 8441 Extended CONNECT).
    ## Owns a dedicated h2 connection whose single CONNECT stream carries the WS
    ## frames as DATA. Full duplex over one blocking socket: a read services inbound
    ## DATA and, while sending, drains the peer's WINDOW_UPDATE / PING so a large
    ## send cannot deadlock. The sans-io `feed` auto-answers PING/SETTINGS and
    ## releases window-blocked DATA, so this driver only shuttles bytes.
    sock: Conn
    h2: H2Conn
    sid: uint32
    inbuf: string   ## inbound tunnel bytes read ahead of a `recv` (e.g. during a send)
    eof: bool       ## the peer half-closed the tunnel (its stream ended or was reset)
  WsTransport = object
    ## The duplex byte channel under a sync WebSocket: an h1 Upgrade connection, an
    ## h2 Extended CONNECT tunnel (RFC 8441) over a dedicated h2 connection, or an h3
    ## Extended CONNECT tunnel (RFC 9220) driven by a background pump thread (the
    ## blocking API is unchanged; the thread just keeps QUIC's timers alive).
    case kind: WsKind
    of wkH1: conn: Conn
    of wkH2: h2c: WsH2
    of wkH3:
      when defined(naviHttp3):
        pump: WsH3Pump
  WebSocketObj = object
    tr: WsTransport
    dec: WsDecoder
    asmb: WsAssembler
    open: bool                ## the WS protocol is open (not yet closed/closing)
    closed: bool              ## the transport has been torn down (closeRaw ran); keeps
                              ## teardown idempotent and lets =destroy skip a done pump
    maxMessageBytes: int      ## cap on a reassembled message; 0 = unlimited
    keepAlive: int            ## ms between keepalive pings while receiving; 0 = off
    pingOutstanding: bool      ## a keepalive ping is awaiting any inbound byte
  WebSocket* = ref WebSocketObj

proc pumpH2(w: WsH2) =
  ## Read one socket chunk and advance the h2 connection: `feed` answers PING /
  ## SETTINGS, replenishes flow-control windows, and releases any window-blocked
  ## outbound DATA (returned as bytes to write). Buffer inbound tunnel bytes and
  ## note a half-close. One blocking read per call, so callers stay in control.
  let chunk = w.sock.recvSome()
  if chunk.len == 0:
    w.eof = true
    return
  let toSend = w.h2.feed(chunk)
  if toSend.len > 0: w.sock.sendAll(toSend)
  let body = w.h2.takeBody(w.sid)         # drain before the eof check: a final DATA
  if body.len > 0: w.inbuf.add body       # frame can carry END_STREAM with its payload
  if w.h2.connError.len > 0 or w.h2.streamEnded(w.sid) or w.h2.streamReset(w.sid):
    w.eof = true

proc h2Send(w: WsH2, data: string) =
  ## Emit `data` as tunnel DATA, pumping the socket for a WINDOW_UPDATE whenever the
  ## send window is exhausted, so a frame larger than the peer's window cannot
  ## deadlock. Inbound bytes read while waiting are buffered for the next `recv`.
  let first = w.h2.queueSend(w.sid, data)
  if first.len > 0: w.sock.sendAll(first)
  while not w.h2.sendDrained(w.sid):
    if w.eof: raise newException(IOError, "navi: websocket h2 tunnel closed by peer")
    w.pumpH2()

proc h2Recv(w: WsH2): string =
  ## The next inbound tunnel chunk, or "" once the peer half-closes with nothing
  ## buffered (the transport-EOF signal the frame reader expects).
  while w.inbuf.len == 0 and not w.eof:
    w.pumpH2()
  result = move(w.inbuf)
  w.inbuf = ""

proc h2DataWaiting(w: WsH2, ms: int): bool =
  ## Whether a `recv` would return promptly: buffered bytes, a known EOF, or the
  ## socket is readable within ~`ms`.
  w.inbuf.len > 0 or w.eof or w.sock.dataWaiting(ms)

proc h2Close(w: WsH2) =
  ## Half-close the tunnel (END_STREAM) best-effort, then close the socket. A
  ## WebSocket owns its dedicated h2 connection, so tearing down the socket ends it.
  try:
    if not w.eof and not w.h2.streamDone(w.sid):
      let fin = w.h2.finishSend(w.sid)
      if fin.len > 0: w.sock.sendAll(fin)
  except CatchableError: discard
  try: w.sock.close()
  except CatchableError: discard

# The h3 arm of each transport dispatcher, with the -d:naviHttp3 guard defined
# once per op instead of inline in every dispatcher below. Constructing a
# WsTransport of kind wkH3 already requires the h3 build, so the else branches
# are reached only on a misconfigured call.
proc h3Send(ws: WebSocket, data: string) =
  when defined(naviHttp3): wsSend(ws.tr.pump, data)
  else: raise newException(ValueError, "navi: h3 WebSocket without -d:naviHttp3")
proc h3Recv(ws: WebSocket): string =
  when defined(naviHttp3): wsRecv(ws.tr.pump)
  else: raise newException(ValueError, "navi: h3 WebSocket without -d:naviHttp3")
proc h3DataWaiting(ws: WebSocket, ms: int): bool =
  when defined(naviHttp3): wsDataWaiting(ws.tr.pump, ms)
  else: false
proc h3Close(ws: WebSocket) =
  when defined(naviHttp3): wsClose(ws.tr.pump)
  else: discard

proc sendRaw(ws: WebSocket, data: string) =
  ## Write raw bytes to the transport (an encoded WS frame).
  case ws.tr.kind
  of wkH1: ws.tr.conn.sendAll(data)
  of wkH2: h2Send(ws.tr.h2c, data)
  of wkH3: h3Send(ws, data)

proc recvRaw(ws: WebSocket): string =
  ## Block for the next inbound chunk ("" on EOF / peer half-close).
  case ws.tr.kind
  of wkH1: ws.tr.conn.recvSome()
  of wkH2: h2Recv(ws.tr.h2c)
  of wkH3: h3Recv(ws)

proc dataWaitingRaw(ws: WebSocket, ms: int): bool =
  ## Whether an inbound chunk is available within ~`ms` (for keepalive).
  case ws.tr.kind
  of wkH1: ws.tr.conn.dataWaiting(ms)
  of wkH2: h2DataWaiting(ws.tr.h2c, ms)
  of wkH3: h3DataWaiting(ws, ms)

proc closeRaw(ws: WebSocket) =
  ## Tear down the transport exactly once (h3: stop + join the pump thread, then free
  ## the connection). Idempotent, so every close path -- explicit close, a peer-close
  ## EOF, and the streaming error handlers -- can call it freely without a double
  ## joinThread / free.
  if ws.closed: return
  ws.closed = true
  case ws.tr.kind
  of wkH1: ws.tr.conn.close()
  of wkH2: h2Close(ws.tr.h2c)
  of wkH3: h3Close(ws)

proc `=destroy`(o: var WebSocketObj) =
  ## Backstop: a WebSocket dropped without close() still releases its transport. For
  ## h3 that means joining the pump thread and freeing the connection + shared state
  ## (a raw pointer the GC will not reclaim); h2 half-closes its stream and shuts the
  ## dedicated socket; h1's Conn frees its own fd via `=destroy` of the field below.
  ## Best-effort and idempotent (skips a transport already torn down by closeRaw).
  if o.tr.kind == wkH2 and not o.closed:
    o.closed = true
    try: h2Close(o.tr.h2c)
    except CatchableError: discard
  when defined(naviHttp3):
    if o.tr.kind == wkH3 and not o.closed:
      o.closed = true
      try: wsClose(o.tr.pump)
      except CatchableError: discard
  `=destroy`(o.tr)          # h1: destructs the Conn value, closing its fd
  `=destroy`(o.dec)
  `=destroy`(o.asmb)

proc toWsUrl(url: string): Url =
  var s = url
  if s.startsWith("ws://"): s = "http://" & s["ws://".len .. ^1]
  elif s.startsWith("wss://"): s = "https://" & s["wss://".len .. ^1]
  parseUrl(s)

proc websocketH1(client: Navi, u: Url, headers: Headers,
                 maxMessageBytes, keepAlive: int): WebSocket =
  ## WebSocket over an HTTP/1.1 Upgrade (RFC 6455): the universal transport.
  let conn = connect(u.host, u.port, u.isTls, client.config.tls,
                     resolveProxy(client.config, u), @[],
                     client.config.connectMs, client.config.readMs, client.config.totalMs)
  # Close the connection unless the handshake completes -- a send/recv error mid
  # handshake would otherwise leak the socket (and its SSL_CTX for wss).
  var handshakeOk = false
  defer:
    if not handshakeOk:
      try: conn.close()
      except CatchableError: discard
  let key = genKey()
  conn.sendAll(upgradeRequest(u, key, headers))
  var buf = ""
  while "\r\n\r\n" notin buf:
    let chunk = conn.recvSome()
    if chunk.len == 0:
      raise newException(IOError, "navi: websocket handshake closed by peer")
    buf.add chunk
  let headEnd = buf.find("\r\n\r\n") + 4
  if not validate101(buf[0 ..< headEnd], key):
    raise newException(IOError, "navi: websocket upgrade rejected: " &
      buf[0 ..< headEnd].splitLines[0])
  result = WebSocket(tr: WsTransport(kind: wkH1, conn: conn), open: true,
                     maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)
  if buf.len > headEnd:                 # server frames already buffered
    result.dec.feed(buf[headEnd .. ^1])
  handshakeOk = true

proc websocketH2(client: Navi, u: Url, headers: Headers,
                 maxMessageBytes, keepAlive: int): WebSocket =
  ## WebSocket over HTTP/2 Extended CONNECT (RFC 8441). Dials a dedicated h2
  ## connection (ALPN "h2"), confirms the peer advertised ENABLE_CONNECT_PROTOCOL,
  ## opens a CONNECT `:protocol=websocket` stream, and tunnels frames as DATA. No
  ## Sec-WebSocket-Key/Accept over h2; a `:status 200` accepts the tunnel. The
  ## connection is dedicated to this WebSocket (not pooled), so the single blocking
  ## socket drives one full-duplex stream without contending with other requests.
  let conn = connect(u.host, u.port, u.isTls, client.config.tls,
                     resolveProxy(client.config, u), @["h2", "http/1.1"],
                     client.config.connectMs, client.config.readMs, client.config.totalMs)
  var handshakeOk = false
  defer:
    if not handshakeOk:
      try: conn.close()
      except CatchableError: discard
  if conn.protocol != "h2":
    let got = if conn.protocol.len > 0: conn.protocol else: "http/1.1"
    raise newException(ProtocolError,
      "navi: WebSocket over h2 requested but the server negotiated " & got)
  let h2 = initH2Conn(client.config.maxResponseBytes)
  conn.sendAll(h2.preamble())
  # RFC 8441 forbids sending an Extended CONNECT before the peer's SETTINGS
  # (which carries ENABLE_CONNECT_PROTOCOL) has been seen. Read until that initial
  # SETTINGS lands -- a deterministic signal, not an idle guess -- then require the
  # capability, so an origin that does not support it fails fast and clearly.
  while not h2.sawPeerSettings:
    let chunk = conn.recvSome()
    if chunk.len == 0:
      raise newException(IOError, "navi: websocket h2 handshake closed by peer")
    let toSend = h2.feed(chunk)
    if toSend.len > 0: conn.sendAll(toSend)
    if h2.connError.len > 0 or h2.goneAway:   # fatal preface / early GOAWAY: fail fast,
      raise newException(ProtocolError,        # never spin on a dead-but-open connection
        "navi: websocket h2 handshake failed before the server's SETTINGS")
  if not h2.peerAllowsConnect:
    raise newException(ProtocolError,
      "navi: server does not support WebSocket over HTTP/2 " &
      "(no SETTINGS_ENABLE_CONNECT_PROTOCOL); use an h1 WebSocket")
  var extra = headers
  extra["sec-websocket-version"] = wsVersion
  let sid = h2.openStream()
  conn.sendAll(h2.encodeRequestHead(sid, h2ConnectHeaderList(u, "websocket", extra)))
  while not h2.headersReady(sid):        # await the CONNECT response headers
    let chunk = conn.recvSome()
    if chunk.len == 0:
      raise newException(IOError, "navi: websocket h2 handshake closed by peer")
    let toSend = h2.feed(chunk)
    if toSend.len > 0: conn.sendAll(toSend)
    if h2.streamReset(sid):
      raise newException(IOError, "navi: websocket h2 tunnel reset before it opened")
    if h2.streamDone(sid):               # fatal conn error or GOAWAY before the 200
      raise newException(IOError, "navi: websocket h2 tunnel closed before it opened")
  let status = h2.respSnapshot(sid).status
  if status != 200:                      # RFC 8441: a 200 accepts the tunnel
    raise newException(IOError,
      "navi: WebSocket over h2 rejected with :status " & $status)
  let w = WsH2(sock: conn, h2: h2, sid: sid)
  w.inbuf.add h2.takeBody(sid)           # any DATA already buffered behind the 200
  result = WebSocket(tr: WsTransport(kind: wkH2, h2c: w), open: true,
                     maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)
  handshakeOk = true

when defined(naviHttp3):
  proc websocketH3(client: Navi, u: Url, headers: Headers,
                   maxMessageBytes, keepAlive: int): WebSocket =
    ## WebSocket over HTTP/3 Extended CONNECT (RFC 9220). The connection is driven
    ## by a background pump thread so QUIC's timers stay alive between the blocking
    ## send/receive calls; the sync API is unchanged. Needs `--threads:on` (the
    ## default on navi's supported Nim), else it raises so the caller isn't surprised.
    when not compileOption("threads"):
      raise newException(ProtocolError,
        "navi: sync WebSocket over h3 needs a --threads:on build (its connection is " &
        "kept alive by a background pump thread); or use navi/asyncdispatch")
    else:
      let (pump, status) = openWsH3(u.host, u.port, u.host, client.config.tls.caFile,
                                    client.config.tls.verify, u.requestTarget,
                                    wsExtraFields(headers), client.config.connectMs)
      if status != 200:            # RFC 9220 / 8441: a 200 accepts the tunnel
        wsClose(pump)
        raise newException(IOError,
          "navi: WebSocket over h3 rejected with :status " & $status)
      result = WebSocket(tr: WsTransport(kind: wkH3, pump: pump), open: true,
                         maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)

proc websocket*(client: Navi, url: string, headers = initHeaders(),
                maxMessageBytes = 0, keepAlive = 0): WebSocket =
  ## Open a WebSocket connection. Accepts `ws://` / `wss://` (or http/https);
  ## `wss` uses TLS. The transport follows `config.http`: an HTTP/1.1 Upgrade
  ## (RFC 6455) whenever H1 is allowed (the universal path); to tunnel over Extended
  ## CONNECT instead, exclude H1 -- `config.http = {H2}` for h2 (RFC 8441) or `{H3}`
  ## for h3 (RFC 9220, needs `-d:naviHttp3`); both require `wss`. Use `send`,
  ## `receive`, and `close`.
  ##
  ## `maxMessageBytes` (0 = unlimited) caps a reassembled message: past it `receive`
  ## closes with 1009 and raises `WsMessageTooLarge`. Set it for untrusted servers,
  ## since a peer can otherwise grow one message without bound via continuation frames.
  ##
  ## `keepAlive` (ms, 0 = off) sends a ping after that long with no data *while a
  ## `receive` is in progress*, and raises `TimeoutError` (closing the connection) if
  ## another interval passes with still nothing back -- so a dead peer is detected
  ## instead of blocking forever.
  let httpset = client.config.http
  let u = toWsUrl(url)
  if httpset.card == 0 or H1 in httpset:             # h1 is the universal ws transport
    return client.websocketH1(u, headers, maxMessageBytes, keepAlive)
  elif H2 in httpset and u.isTls:                    # opt-in h2 (RFC 8441): config.http = {H2}
    return client.websocketH2(u, headers, maxMessageBytes, keepAlive)
  elif H3 in httpset and u.isTls:                    # opt-in h3 (RFC 9220): config.http = {H3}
    when defined(naviHttp3):
      return client.websocketH3(u, headers, maxMessageBytes, keepAlive)
    else:
      raise newException(ProtocolError,
        "navi: sync WebSocket over h3 requires a -d:naviHttp3 build")
  else:
    raise newException(ProtocolError,
      "navi: config.http " & $httpset & " permits no usable WebSocket transport on " &
      "the sync client (h2/h3 need TLS; h3 also needs {H3} + -d:naviHttp3)")

proc send*(ws: WebSocket, data: string, binary = false) =
  ## Send a text (default) or binary message. Client frames are masked.
  ws.sendRaw(encodeFrame(if binary: opBinary else: opText, data))

proc ping*(ws: WebSocket, data = "") =
  ws.sendRaw(encodeFrame(opPing, data))

proc kaRecv(ws: WebSocket): string =
  ## One read chunk. With keepalive off, a plain (blocking) read. With it on, poll
  ## for data within the interval; on an idle interval send a ping, and on a second
  ## idle interval with a ping already outstanding, declare the peer dead.
  if ws.keepAlive <= 0: return ws.recvRaw()
  while true:
    if ws.dataWaitingRaw(ws.keepAlive):
      ws.pingOutstanding = false          # any inbound byte proves liveness
      return ws.recvRaw()
    if ws.pingOutstanding:                 # pinged last interval, still nothing back
      ws.open = false
      try: ws.closeRaw() except CatchableError: discard
      raise newException(TimeoutError, "navi: websocket keepalive timed out")
    ws.sendRaw(encodeFrame(opPing, ""))
    ws.pingOutstanding = true

proc receive*(ws: WebSocket): WsMessage =
  ## Block until a full message arrives, answering pings and reassembling
  ## fragments. A close returns `wmClose` (and the connection is then closed).
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = ws.kaRecv()
      if chunk.len == 0:                       # peer closed: tear the transport down
        ws.open = false                        # now (h3: join the pump) so it is not
        ws.closeRaw()                          # leaked when the caller's close() no-ops
        return WsMessage(kind: wmClose, closeCode: closeGoingAway)
      ws.dec.feed(chunk)
    var o: WsOutcome
    try:
      o = ws.asmb.offer(f, ws.maxMessageBytes, rejectMasked = true)
    except WsMessageTooLarge:
      if ws.open:      # tell the peer why (1009), then drop the connection
        try: ws.sendRaw(encodeFrame(opClose, closePayload(closeMessageTooBig)))
        except CatchableError: discard
        ws.open = false
        ws.closeRaw()
      raise
    case o.reply
    of wrPong:
      ws.sendRaw(encodeFrame(opPong, o.replyPayload))
    of wrCloseEcho:
      if ws.open:
        try: ws.sendRaw(encodeFrame(opClose, o.replyPayload))
        except CatchableError: discard
        ws.open = false
        ws.closeRaw()
    of wrNone: discard
    if o.ready: return o.message

proc close*(ws: WebSocket, code = closeNormal, reason = "") =
  ## Send a close frame (if still open) and tear the transport down. Idempotent: safe
  ## to call after a peer-close, which already set open=false and tore down -- the
  ## transport teardown still runs (once) so an already-closed h3 pump is not leaked.
  if ws.open:
    ws.open = false
    try: ws.sendRaw(encodeFrame(opClose, closePayload(code, reason)))
    except CatchableError: discard
  ws.closeRaw()

# --- WebSocket streaming (a large message, one frame at a time) ---

type
  WsReader* = ref object
    ## A message being received incrementally. `kind` is the message type (or
    ## `wmClose` if a close arrived instead). Consume with `each`/`readChunk`.
    ws: WebSocket
    kind*: WsMessageKind
    first: string            ## the first data frame's payload, buffered by `stream`
    hasFirst: bool
    done: bool               ## the fin frame has been consumed
  WsWriter* = ref object
    ## A message being sent incrementally. `write` each fragment; the final frame is
    ## sent by the `stream` block on exit.
    ws: WebSocket
    binary: bool
    started: bool

proc readDataFrame(ws: WebSocket): Frame =
  ## The next non-control frame (data / continuation / close), answering pings along
  ## the way; keepalive applies (via `kaRecv`). A transport EOF yields a close frame.
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = ws.kaRecv()
      if chunk.len == 0:
        ws.open = false
        ws.closeRaw()                          # tear down on EOF (see receive)
        return Frame(fin: true, opcode: opClose, payload: "")
      ws.dec.feed(chunk)
    case f.opcode
    of opPing: ws.sendRaw(encodeFrame(opPong, f.payload)); continue
    of opPong: continue
    else: return f

proc closeOnFrame(ws: WebSocket, f: Frame) =
  ## Echo a peer close and drop the transport (used when a close interrupts a stream).
  if ws.open:
    try: ws.sendRaw(encodeFrame(opClose, f.payload))
    except CatchableError: discard
    ws.open = false
    ws.closeRaw()

proc openStreamReader(ws: WebSocket): WsReader =
  ## Impl of the no-arg `ws.stream()`; kept a proc so the public `stream` is a
  ## template (a proc + template overload would force early symbol resolution of the
  ## `stream(writer)` block's injected identifier).
  result = WsReader(ws: ws)
  let f = ws.readDataFrame()
  case f.opcode
  of opText, opBinary:
    result.kind = if f.opcode == opText: wmText else: wmBinary
    result.first = f.payload
    result.hasFirst = true
    result.done = f.fin
  of opClose:
    result.kind = wmClose
    result.done = true
    ws.closeOnFrame(f)
  else:
    raise newException(IOError, "navi: WebSocket message started with a continuation frame")

proc readChunk*(r: WsReader): string =
  ## The next chunk of the streamed message (one frame's payload), or "" at its end.
  if r.hasFirst:
    r.hasFirst = false
    return r.first
  if r.done: return ""
  let f = r.ws.readDataFrame()
  case f.opcode
  of opContinuation:
    r.done = f.fin
    return f.payload
  of opClose:                # a close interrupted the message: truncate and drop
    r.done = true
    r.ws.closeOnFrame(f)
    return ""
  else:
    raise newException(IOError, "navi: expected a continuation frame mid-message")

proc drain*(r: WsReader, sink: BodySink) =
  ## Deliver the message's chunks to `sink`, leaving the WebSocket ready for the next
  ## message. On a sink error the connection is closed (a half-read message cannot be
  ## resumed) and the error re-raised. Prefer the `each` template.
  try:
    while true:
      let chunk = r.readChunk()
      if chunk.len == 0: break
      sink(chunk)
  except CatchableError:
    r.ws.open = false
    try: r.ws.closeRaw()
    except CatchableError: discard
    raise

template each*(r: WsReader; chunk, body: untyped): untyped =
  ## Run `body` for each chunk of the streamed message, with `chunk` bound to it.
  ## Like `StreamResponse.each`, `body` runs as a proc, so `break`/`continue`/`return`
  ## cannot escape it; raise to stop early (which closes the connection).
  r.drain(proc(chunk: string) {.raises: [CatchableError].} = body)

proc write*(w: WsWriter, data: string) =
  ## Append a fragment to the message being streamed out.
  if not w.started:
    w.ws.sendRaw(encodeFrame(if w.binary: opBinary else: opText, data, fin = false))
    w.started = true
  else:
    w.ws.sendRaw(encodeFrame(opContinuation, data, fin = false))

proc finishWrite(w: WsWriter) =
  ## Send the terminating fin frame (an empty continuation, or an empty text/binary
  ## frame for a message with no `write`s).
  if not w.started:
    w.ws.sendRaw(encodeFrame(if w.binary: opBinary else: opText, "", fin = true))
  else:
    w.ws.sendRaw(encodeFrame(opContinuation, "", fin = true))

template stream*(ws: WebSocket): WsReader =
  ## Begin receiving the next message as a stream of chunks (one per frame) instead
  ## of buffering the whole thing. `kind` is set from the first frame. Consume the
  ## returned reader with `each` or `readChunk`. `maxMessageBytes` does not apply --
  ## you bound your own sink.
  openStreamReader(ws)

template streamOut(ws: WebSocket; writer: untyped; isBinary: bool; body: untyped) =
  ## Shared body of `stream`/`streamBinary` (called internally, so the block-syntax
  ## last-arg rule doesn't apply and the `isBinary` middle param is fine here).
  block:
    var writer {.inject.} = WsWriter(ws: ws, binary: isBinary)
    try:
      body
      writer.finishWrite()
    except CatchableError:      # a partial message can't be completed: drop the conn
      ws.open = false
      try: ws.closeRaw()
      except CatchableError: discard
      raise

template stream*(ws: WebSocket; writer, body: untyped): untyped =
  ## Send the next message as a stream of text fragments: `write` each chunk inside
  ## the block, and the final (fin) frame is sent automatically on block exit. On an
  ## exception the partial message cannot be completed, so the connection is closed.
  ##   ws.stream(writer):
  ##     for chunk in chunks: writer.write(chunk)
  streamOut(ws, writer, false, body)

template streamBinary*(ws: WebSocket; writer, body: untyped): untyped =
  ## Like `stream(writer)`, but the message is binary.
  streamOut(ws, writer, true, body)
