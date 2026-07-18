## navi — chronos entry point.
##
##   import navi/chronos
##   let api = newNavi()
##   let res = await api.get("http://example.com")
##
## Requires the `chronos` package. Only compiled when this module is imported,
## so sync/asyncdispatch users never pull chronos in.

import navi/private/entryguard
import navi/core/public
import navi/core/[engine, pool, session, proxy]
import navi/proto/ws
import navi/backend/chronos
from std/strutils import startsWith, find, splitLines, contains

claimEntry("navi/chronos")
export public, chronos

type
  Hook* = proc(ctx: HookCtx): Future[void] {.closure.}
    ## Lifecycle callback; may be async. Mutate `ctx.request` (beforeRequest),
    ## read/mutate `ctx.response` (afterResponse), or read `ctx.attempt`.
  Hooks* = object
    beforeRequest*: seq[Hook]
    afterResponse*: seq[Hook]
    beforeRetry*: seq[Hook]

  NaviOptions* = object of NaviOptionsBase
    hooks*: Hooks   ## lifecycle callbacks (may be async)

  Navi* = object
    options*: NaviOptions
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar

proc defaultOptions*(): NaviOptions =
  result.http = {H1, H2}
  result.tls = defaultTls()

proc mergeHooks(base, add: Hooks): Hooks =
  Hooks(beforeRequest: base.beforeRequest & add.beforeRequest,
        afterResponse: base.afterResponse & add.afterResponse,
        beforeRetry: base.beforeRetry & add.beforeRetry)

proc runHook(hook: Hook, ctx: HookCtx): Future[void] =
  # See the asyncdispatch entry: the call returns a Future without raising.
  {.cast(gcsafe).}:
    {.cast(raises: []).}:
      result = hook(ctx)

proc newNavi*(options = defaultOptions()): Navi =
  Navi(options: options, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc extend*(client: Navi, options: NaviOptions): Navi =
  var merged = mergeBase(client.options, options)
  merged.hooks = mergeHooks(client.options.hooks, options.hooks)
  Navi(options: merged, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc close*(client: Navi): Future[void] {.async.} =
  ## Close all idle pooled connections. Optional but recommended when done with
  ## the client (a later request opens fresh connections).
  for pc in client.pool.drain():
    await close(pc.transport)

proc transport(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  ## Pool-based transport (http/1.1; chronos has no h2).
  result = poolTransport(client, req, sink)

proc doRequest(client: Navi, req: Request): Future[Response] {.async.} =
  result = performRequest(client, req)

proc doStream(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  result = performStream(client, req, sink)

proc withDeadline[T](client: Navi, fut: Future[T]): Future[T] {.async.} =
  ## Bound the whole operation by `timeout`. On expiry, raise TimeoutError;
  ## chronos cancels the abandoned future (whose own cleanup closes the socket).
  let ms = client.options.timeoutMs
  if ms <= 0:
    return await fut
  if await withTimeout(fut, ms.milliseconds):
    return await fut
  raise newException(TimeoutError, "navi: request timed out after " & $ms & " ms")

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[], multipart: Multipart = @[],
              bodyStream: BodyProducer = nil): Future[Response] {.async.} =
  let req = buildRequest(client.options, verb, target, headers, body, json,
                         form, multipart, bodyStream)
  result = await client.withDeadline(doRequest(client, req))

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Future[Response] {.async.} =
  ## Deliver the response body to `sink` as it arrives; Response.body is empty.
  let req = buildRequest(client.options, verb, target, headers)
  result = await client.withDeadline(doStream(client, req, sink))

include navi/private/verbs

# --- WebSocket (RFC 6455) ---

export ws.WsMessage, ws.WsMessageKind, ws.closeNormal, ws.closeGoingAway

type
  WebSocket* = ref object
    conn: Conn
    dec: WsDecoder
    asmb: WsAssembler
    open: bool

proc toWsUrl(url: string): Url =
  var s = url
  if s.startsWith("ws://"): s = "http://" & s["ws://".len .. ^1]
  elif s.startsWith("wss://"): s = "https://" & s["wss://".len .. ^1]
  parseUrl(s)

proc doWebsocket(client: Navi, url: string,
                 headers = initHeaders()): Future[WebSocket] {.async.} =
  let u = toWsUrl(url)
  let conn = await connect(u.host, u.port, u.isTls, client.options.tls,
                           resolveProxy(client.options, u), @[],
                           client.options.timeoutMs)
  let key = genKey()
  # Close the connection on any handshake failure (its close is async, so this
  # uses try/except rather than defer). A timeout cancels this future, raising
  # CancelledError here (a CatchableError), so the connection is closed too.
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
    result = WebSocket(conn: conn, open: true)
    if buf.len > headEnd:
      result.dec.feed(buf[headEnd .. ^1])
  except CatchableError:
    await conn.close()
    raise

proc websocket*(client: Navi, url: string,
                headers = initHeaders()): Future[WebSocket] {.async.} =
  ## Open a WebSocket connection (RFC 6455). Accepts `ws://` / `wss://` (or
  ## http/https); `wss` uses TLS. Does the HTTP/1.1 Upgrade over the transport and
  ## validates `Sec-WebSocket-Accept`. The whole open (connect, TLS handshake, and
  ## Upgrade) is bounded by `timeout`. Use `send`, `receive`, and `close`.
  result = await client.withDeadline(doWebsocket(client, url, headers))

proc send*(ws: WebSocket, data: string, binary = false): Future[void] {.async.} =
  ## Send a text (default) or binary message. Client frames are masked.
  await ws.conn.sendAll(encodeFrame(if binary: opBinary else: opText, data))

proc ping*(ws: WebSocket, data = ""): Future[void] {.async.} =
  await ws.conn.sendAll(encodeFrame(opPing, data))

proc receive*(ws: WebSocket): Future[WsMessage] {.async.} =
  ## Await a full message, answering pings and reassembling fragments. A close
  ## returns `wmClose` (and the connection is then closed).
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = await ws.conn.recvSome()
      if chunk.len == 0:
        ws.open = false
        return WsMessage(kind: wmClose, closeCode: closeGoingAway)
      ws.dec.feed(chunk)
    let o = ws.asmb.offer(f)
    case o.reply
    of wrPong:
      await ws.conn.sendAll(encodeFrame(opPong, o.replyPayload))
    of wrCloseEcho:
      if ws.open:
        try: await ws.conn.sendAll(encodeFrame(opClose, o.replyPayload))
        except CatchableError: discard
        ws.open = false
        await ws.conn.close()
    of wrNone: discard
    if o.ready: return o.message

proc close*(ws: WebSocket, code = closeNormal, reason = ""): Future[void] {.async.} =
  ## Send a close frame and close the transport (freeing its TLS context).
  ## Idempotent: a no-op once the socket is already closed.
  if not ws.open: return
  try: await ws.conn.sendAll(encodeFrame(opClose, closePayload(code, reason)))
  except CatchableError: discard
  ws.open = false
  await ws.conn.close()
