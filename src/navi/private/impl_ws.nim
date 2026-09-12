## WebSocket (RFC 6455) over h1/h2/h3 for the async backends.
## `include`d (transitively, via impl_common) by the asyncdispatch and chronos
## backends; shares their imports, the `Navi`/`Conn`/`H2Mux` types, and `await`.
## Not a standalone module.

# --- WebSocket (RFC 6455) ---

export ws.WsMessage, ws.WsMessageKind, ws.closeNormal, ws.closeGoingAway,
       ws.closeMessageTooBig, ws.WsMessageTooLarge

type
  WsKind = enum wkH1, wkH2, wkH3
  WsTransport = object
    ## The duplex byte channel under a WebSocket: an h1 Upgrade connection, an h2
    ## Extended CONNECT tunnel stream (RFC 8441), or an h3 Extended CONNECT tunnel
    ## stream (RFC 9220). The frame codec is identical on top of any of them; only
    ## sendRaw/recvRaw/closeRaw differ.
    case kind: WsKind
    of wkH1: conn: Conn
    of wkH2:
      mux: H2Mux
      sid: uint32
    of wkH3:
      when defined(naviHttp3):     # the h3 (QUIC) transport type is opt-in
        qc: QuicConn
        h3sid: int64
  WebSocket* = ref object
    tr: WsTransport
    dec: WsDecoder
    asmb: WsAssembler
    open: bool                ## the WS protocol is open (not yet closed/closing)
    closed: bool              ## the transport has been torn down (closeRaw ran); keeps
                              ## teardown idempotent (h2/h3 own a dedicated connection)
    maxMessageBytes: int      ## cap on a reassembled message; 0 = unlimited
    keepAlive: int            ## ms between keepalive pings while receiving; 0 = off
    pingOutstanding: bool      ## a keepalive ping is awaiting any inbound byte
    pendingRecv: Future[string]  ## the one in-flight read, kept across keepalive
                                 ## timeouts so a timed-out read is not orphaned

# kaRecv is defined per backend after the shared body (the fireSend pattern):
# withTimeout on asyncdispatch; race + a cancellable timer on chronos.
proc kaRecv(ws: WebSocket): Future[string] {.async.}

proc sendRaw(ws: WebSocket, data: string): Future[void] =
  ## Write raw bytes to the underlying transport (an encoded WS frame).
  case ws.tr.kind
  of wkH1: ws.tr.conn.sendAll(data)
  of wkH2: ws.tr.mux.tunnelSend(ws.tr.sid, data)
  of wkH3:
    when defined(naviHttp3): ws.tr.qc.tunnelSend(ws.tr.h3sid, data)
    else: raise newException(ValueError, "navi: h3 WebSocket without -d:naviHttp3")

proc recvRaw(ws: WebSocket): Future[string] =
  ## Read the next inbound chunk ("" on EOF / peer half-close).
  case ws.tr.kind
  of wkH1: ws.tr.conn.recvSome()
  of wkH2: ws.tr.mux.tunnelRecv(ws.tr.sid)
  of wkH3:
    when defined(naviHttp3): ws.tr.qc.tunnelRecv(ws.tr.h3sid)
    else: raise newException(ValueError, "navi: h3 WebSocket without -d:naviHttp3")

proc closeRaw(ws: WebSocket): Future[void] {.async.} =
  ## Tear down the transport exactly once (h2/h3: half-close the stream, then the
  ## dedicated connection). Idempotent, so a peer-close EOF, an explicit close, and
  ## the streaming error handlers can all call it without a double close of the
  ## dedicated mux/QUIC connection (which would leak or double-free).
  if ws.closed: return
  ws.closed = true
  case ws.tr.kind
  of wkH1: await ws.tr.conn.close()
  of wkH2:
    await ws.tr.mux.tunnelClose(ws.tr.sid)
    await ws.tr.mux.close()
  of wkH3:
    when defined(naviHttp3):
      await ws.tr.qc.tunnelClose(ws.tr.h3sid)
      await ws.tr.qc.closeConn()

proc toWsUrl(url: string): Url =
  var s = url
  if s.startsWith("ws://"): s = "http://" & s["ws://".len .. ^1]
  elif s.startsWith("wss://"): s = "https://" & s["wss://".len .. ^1]
  parseUrl(s)

proc doWebsocketH1(client: Navi, u: Url, headers: Headers,
                   maxMessageBytes, keepAlive: int): Future[WebSocket] {.async.} =
  ## WebSocket over an HTTP/1.1 Upgrade (RFC 6455): the universal transport.
  let conn = await connect(u.host, u.port, u.isTls, client.config.tls,
                           resolveProxy(client.config, u), @[],
                           client.config.connectMs, client.config.readMs)
  let key = genKey()
  # Close the connection on any handshake failure (its close is async, so this
  # uses try/except rather than defer).
  try:
    await conn.sendAll(upgradeRequest(u, key, headers))
    var buf = ""
    while "\r\n\r\n" notin buf:
      let chunk = await conn.recvSome()
      if chunk.len == 0:
        raise newException(IOError, "navi: websocket handshake closed by peer")
      buf.add chunk
    let headEnd = buf.find("\r\n\r\n") + 4
    if not validate101(buf[0 ..< headEnd], key):
      raise newException(IOError, "navi: websocket upgrade rejected: " &
        buf[0 ..< headEnd].splitLines[0])
    result = WebSocket(tr: WsTransport(kind: wkH1, conn: conn), open: true,
                       maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)
    if buf.len > headEnd:
      result.dec.feed(buf[headEnd .. ^1])
  except CatchableError:
    await conn.close()
    raise

proc doWebsocketH2(client: Navi, u: Url, headers: Headers,
                   maxMessageBytes, keepAlive: int): Future[WebSocket] {.async.} =
  ## WebSocket over HTTP/2 Extended CONNECT (RFC 8441). Dials a dedicated h2
  ## connection (ALPN "h2"), opens a CONNECT stream with `:protocol=websocket`,
  ## and tunnels frames as DATA. Sec-WebSocket-Key/Accept are not used over h2.
  let conn = await connect(u.host, u.port, u.isTls, client.config.tls,
                           resolveProxy(client.config, u), @["h2", "http/1.1"],
                           client.config.connectMs, client.config.readMs)
  if conn.protocol != "h2":
    let got = if conn.protocol.len > 0: conn.protocol else: "http/1.1"
    await conn.close()
    raise newException(ProtocolError,
      "navi: WebSocket over h2 requested but the server negotiated " & got)
  let mux = await newH2Mux(conn, client.config.maxResponseBytes,
                           client.config.wantsDecompress,
                           client.config.h2KeepAliveMs)
  try:
    var reqHeaders = headers
    reqHeaders["sec-websocket-version"] = wsVersion
    let sid = await mux.openConnect(h2ConnectHeaderList(u, "websocket", reqHeaders))
    let status = mux.respSnapshot(sid).status
    if status != 200:            # RFC 8441: a 2xx (200) accepts the tunnel
      raise newException(IOError,
        "navi: WebSocket over h2 rejected with :status " & $status)
    result = WebSocket(tr: WsTransport(kind: wkH2, mux: mux, sid: sid), open: true,
                       maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)
  except CatchableError:
    await mux.close()
    raise

when defined(naviHttp3):
  proc doWebsocketH3(client: Navi, u: Url, headers: Headers,
                     maxMessageBytes, keepAlive: int): Future[WebSocket] {.async.} =
    ## WebSocket over HTTP/3 Extended CONNECT (RFC 9220). Dials a dedicated h3
    ## (QUIC) connection to the origin and opens a CONNECT `:protocol=websocket`
    ## stream, tunnelling frames as DATA. Sec-WebSocket-Key/Accept are not used.
    let qc = await openQuicConn(u.host, u.port, u.host, client.config.tls.caFile,
                                 client.config.tls.verify, 0'u64)  # WS frames stream, no body cap
    try:
      let (sid, status) = await qc.openConnect(u.requestTarget,
                                               wsExtraFields(headers), "websocket")
      if status != 200:            # RFC 9220 / 8441: a 200 accepts the tunnel
        raise newException(IOError,
          "navi: WebSocket over h3 rejected with :status " & $status)
      result = WebSocket(tr: WsTransport(kind: wkH3, qc: qc, h3sid: sid), open: true,
                         maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)
    except CatchableError:
      await qc.closeConn()
      raise

proc doWebsocket(client: Navi, url: string,
                 headers = initHeaders(),
                 maxMessageBytes = 0, keepAlive = 0): Future[WebSocket] {.async.} =
  let u = toWsUrl(url)
  let httpset = client.config.http
  if httpset.card == 0 or H1 in httpset:             # h1 is the universal ws transport
    return await client.doWebsocketH1(u, headers, maxMessageBytes, keepAlive)
  elif H2 in httpset and u.isTls:                    # opt-in h2 (RFC 8441) by excluding H1
    return await client.doWebsocketH2(u, headers, maxMessageBytes, keepAlive)
  elif H3 in httpset and u.isTls:                    # opt-in h3 (RFC 9220): config.http = {H3}
    when defined(naviHttp3):
      return await client.doWebsocketH3(u, headers, maxMessageBytes, keepAlive)
    else:
      raise newException(ProtocolError,
        "navi: WebSocket over h3 requires a -d:naviHttp3 build")
  else:
    raise newException(ProtocolError,
      "navi: config.http " & $httpset & " permits no usable WebSocket transport " &
      "(h2/h3 need TLS)")

proc websocket*(client: Navi, url: string,
                headers = initHeaders(),
                maxMessageBytes = 0, keepAlive = 0): Future[WebSocket] {.async.} =
  ## Open a WebSocket connection. Accepts `ws://` / `wss://` (or http/https);
  ## `wss` uses TLS. The transport follows `config.http`: h1 Upgrade (RFC 6455) is
  ## used whenever H1 is allowed (the universal path); to tunnel over Extended
  ## CONNECT instead, exclude H1 -- `config.http = {H2}` for h2 (RFC 8441) or
  ## `{H3}` for h3 (RFC 9220, needs `-d:naviHttp3`). The whole open is bounded by
  ## `timeout`. Use `send`, `receive`, and `close`.
  ##
  ## `maxMessageBytes` (0 = unlimited) caps a reassembled message: past it `receive`
  ## closes with 1009 and raises `WsMessageTooLarge`. Set it for untrusted servers,
  ## since a peer can otherwise grow one message without bound via continuation frames.
  ##
  ## `keepAlive` (ms, 0 = off) sends a ping after that long with no data *while a
  ## `receive` is in progress*, and raises `TimeoutError` (closing the connection) if
  ## another interval passes with still nothing back -- so a dead peer is detected
  ## instead of awaiting forever.
  result = await guard(client.config.totalMs,
    doWebsocket(client, url, headers, maxMessageBytes, keepAlive), nil)

proc send*(ws: WebSocket, data: string, binary = false): Future[void] {.async.} =
  ## Send a text (default) or binary message. Client frames are masked.
  await ws.sendRaw(encodeFrame(if binary: opBinary else: opText, data))

proc ping*(ws: WebSocket, data = ""): Future[void] {.async.} =
  await ws.sendRaw(encodeFrame(opPing, data))

proc receive*(ws: WebSocket): Future[WsMessage] {.async.} =
  ## Await a full message, answering pings and reassembling fragments. A close
  ## returns `wmClose` (and the connection is then closed).
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = await ws.kaRecv()
      if chunk.len == 0:                        # peer closed: tear the transport down
        ws.open = false                        # now (h2/h3: close the dedicated conn)
        await ws.closeRaw()                    # so it is not leaked when close() no-ops
        return WsMessage(kind: wmClose, closeCode: closeGoingAway)
      ws.dec.feed(chunk)
    var o: WsOutcome
    try:
      o = ws.asmb.offer(f, ws.maxMessageBytes, rejectMasked = true)
    except WsMessageTooLarge:
      if ws.open:      # tell the peer why (1009), then drop the connection
        try: await ws.sendRaw(encodeFrame(opClose, closePayload(closeMessageTooBig)))
        except CatchableError: discard
        ws.open = false
        await ws.closeRaw()
      raise
    case o.reply
    of wrPong:
      await ws.sendRaw(encodeFrame(opPong, o.replyPayload))
    of wrCloseEcho:
      if ws.open:
        try: await ws.sendRaw(encodeFrame(opClose, o.replyPayload))
        except CatchableError: discard
        ws.open = false
        await ws.closeRaw()
    of wrNone: discard
    if o.ready: return o.message

proc close*(ws: WebSocket, code = closeNormal, reason = ""): Future[void] {.async.} =
  ## Send a close frame (if still open) and tear the transport down. Idempotent: safe
  ## after a peer-close, which already set open=false and tore down -- the transport
  ## teardown still runs (once) so an already-closed h2/h3 connection is not leaked.
  if ws.open:
    ws.open = false
    try: await ws.sendRaw(encodeFrame(opClose, closePayload(code, reason)))
    except CatchableError: discard
  await ws.closeRaw()

# --- WebSocket streaming (a large message, one frame at a time) ---

type
  WsReader* = ref object
    ## A message being received incrementally. Consume with `each`/`readChunk`.
    ws: WebSocket
    kind*: WsMessageKind
    first: string
    hasFirst: bool
    done: bool
  WsWriter* = ref object
    ## A message being sent incrementally; `write` each fragment.
    ws: WebSocket
    binary: bool
    started: bool

proc readDataFrame(ws: WebSocket): Future[Frame] {.async.} =
  ## Next non-control frame (data/continuation/close), answering pings; keepalive
  ## applies via `kaRecv`. A transport EOF yields a close frame.
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = await ws.kaRecv()
      if chunk.len == 0:
        ws.open = false
        await ws.closeRaw()                    # tear down on EOF (see receive)
        return Frame(fin: true, opcode: opClose, payload: "")
      ws.dec.feed(chunk)
    case f.opcode
    of opPing: await ws.sendRaw(encodeFrame(opPong, f.payload)); continue
    of opPong: continue
    else: return f

proc closeOnFrame(ws: WebSocket, f: Frame): Future[void] {.async.} =
  if ws.open:
    try: await ws.sendRaw(encodeFrame(opClose, f.payload))
    except CatchableError: discard
    ws.open = false
    await ws.closeRaw()

proc openStreamReader(ws: WebSocket): Future[WsReader] {.async.} =
  result = WsReader(ws: ws)
  let f = await ws.readDataFrame()
  case f.opcode
  of opText, opBinary:
    result.kind = if f.opcode == opText: wmText else: wmBinary
    result.first = f.payload
    result.hasFirst = true
    result.done = f.fin
  of opClose:
    result.kind = wmClose
    result.done = true
    await ws.closeOnFrame(f)
  else:
    raise newException(IOError, "navi: WebSocket message started with a continuation frame")

proc readChunk*(r: WsReader): Future[string] {.async.} =
  ## The next chunk of the streamed message (one frame's payload), or "" at its end.
  if r.hasFirst:
    r.hasFirst = false
    return r.first
  if r.done: return ""
  let f = await r.ws.readDataFrame()
  case f.opcode
  of opContinuation:
    r.done = f.fin
    return f.payload
  of opClose:
    r.done = true
    await r.ws.closeOnFrame(f)
    return ""
  else:
    raise newException(IOError, "navi: expected a continuation frame mid-message")

proc drain*(r: WsReader, sink: BodySink): Future[void] {.async.} =
  ## Deliver the message's chunks to `sink`; on a sink error close the connection (a
  ## half-read message cannot be resumed) and re-raise. Prefer the `each` template.
  try:
    while true:
      let chunk = await r.readChunk()
      if chunk.len == 0: break
      # the sink type carries no raises annotation; discharge chronos's strict
      # effects here (it raises at most CatchableError), as the HTTP drain does.
      {.cast(gcsafe).}:
        {.cast(raises: [CatchableError]).}:
          await sink(chunk)
  except CatchableError:
    r.ws.open = false
    try: await r.ws.closeRaw()
    except CatchableError: discard
    raise

template each*(r: WsReader; chunk, body: untyped): untyped =
  ## Run `body` for each chunk of the streamed message. `body` runs as a proc, so
  ## `break`/`continue`/`return` cannot escape it; raise to stop early. The `await`
  ## is baked in, so there is none on the `each` line.
  await r.drain(proc(chunk: string): Future[void] {.async.} = body)

template stream*(ws: WebSocket): untyped =
  ## Begin receiving the next message as a stream of chunks (one per frame). `kind`
  ## is set from the first frame. Consume with `each`/`readChunk`. `maxMessageBytes`
  ## does not apply -- you bound your own sink. Returns a `Future[WsReader]`, so:
  ##   let reader = await ws.stream()
  openStreamReader(ws)

proc write*(w: WsWriter, data: string): Future[void] {.async.} =
  ## Append a fragment to the message being streamed out.
  if not w.started:
    await w.ws.sendRaw(encodeFrame(if w.binary: opBinary else: opText, data, fin = false))
    w.started = true
  else:
    await w.ws.sendRaw(encodeFrame(opContinuation, data, fin = false))

proc finishWrite(w: WsWriter): Future[void] {.async.} =
  if not w.started:
    await w.ws.sendRaw(encodeFrame(if w.binary: opBinary else: opText, "", fin = true))
  else:
    await w.ws.sendRaw(encodeFrame(opContinuation, "", fin = true))

template streamOut(ws: WebSocket; writer: untyped; isBinary: bool; body: untyped) =
  block:
    var writer {.inject.} = WsWriter(ws: ws, binary: isBinary)
    try:
      body
      await writer.finishWrite()
    except CatchableError:
      ws.open = false
      try: await ws.closeRaw()
      except CatchableError: discard
      raise

template stream*(ws: WebSocket; writer, body: untyped): untyped =
  ## Send the next message as a stream of text fragments: `await writer.write(chunk)`
  ## inside the block; the final (fin) frame is sent on block exit. On an exception
  ## the partial message cannot be completed, so the connection is closed.
  streamOut(ws, writer, false, body)

template streamBinary*(ws: WebSocket; writer, body: untyped): untyped =
  ## Like `stream(writer)`, but the message is binary.
  streamOut(ws, writer, true, body)
