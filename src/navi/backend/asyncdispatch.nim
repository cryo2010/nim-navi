## Asynchronous transport backend: a raw non-blocking fd we own directly.
##
## This does not use std/asyncnet for TLS. asyncnet runs SSL over memory BIO
## pairs, copying every handshake flight through buffers with a fresh allocation
## and an event-loop round trip per step -- a resumed handshake costs ~1 ms of
## machinery on loopback. Instead we own the fd (asyncdispatch's async connect +
## AsyncFD), attach the SSL directly with SSL_set_fd, and drive the handshake and
## read/write via OpenSSL, awaiting fd readiness only when OpenSSL asks for it
## (WANT_READ / WANT_WRITE). That is the same lean loop as the sync backend, made
## async: the per-connection cost drops to roughly the sync backend's.

import std/[asyncdispatch, nativesockets, strutils]
import ./api, ./openssl_ctx
import ../core/response  # for navi's TimeoutError
when defined(ssl):
  import std/openssl

export api, asyncdispatch

type
  BodySink* = proc(data: string): Future[void] {.closure.}
    ## Streaming download sink for the asyncdispatch backend. Awaitable: the engine
    ## and h2 mux `await` it, so a slow sink applies cooperative backpressure (it
    ## stalls the peer via the gated receive window) rather than buffering in memory.
    ## Takes an owned `string` (navi's native body type, an 8-bit-clean byte buffer):
    ## the chunk crosses an `await` so it must be owned, not a borrowed view; being
    ## navi's own body type lets the engine move each chunk in with no copy.

# Disable Nagle on the connection socket: without it the TLS handshake's final
# flight plus the first request stall ~40ms on the peer's delayed ACK, paid on
# every fresh (unpooled) connection.
when defined(windows):
  import std/winlean
  proc setNoDelay(fd: SocketHandle) =
    setSockOptInt(fd, winlean.IPPROTO_TCP.int, winlean.TCP_NODELAY.int, 1)
else:
  import std/posix
  proc setNoDelay(fd: SocketHandle) =
    setSockOptInt(fd, posix.IPPROTO_TCP.int, posix.TCP_NODELAY.int, 1)

const invalidFd = AsyncFD(-1)

type
  Conn* = object
    fd: AsyncFD
    protocol*: string   ## ALPN-negotiated protocol ("h2" or "", meaning http/1.1)
    readMs: int         ## per-read stall timeout in ms; 0 blocks indefinitely
    when defined(ssl):
      ssl: SslPtr       ## the TLS connection; nil for plain http
      ctx: SslContext   ## kept so `close` can free the SSL_CTX (destroyContext)
      slot: SessionSlot ## keeps the resumption link alive for the SSL's lifetime

var openedConnections*: int  ## diagnostic: TCP connections opened by this backend

# --- fd readiness ------------------------------------------------------

proc waitRead(fd: AsyncFD): owned(Future[void]) =
  ## Complete once `fd` is readable. One-shot: the callback returns true so the
  ## dispatcher drops it after firing.
  let fut = newFuture[void]("navi.waitRead")
  addRead(fd, proc(f: AsyncFD): bool =
    if not fut.finished: fut.complete()
    true)
  fut

proc waitWrite(fd: AsyncFD): owned(Future[void]) =
  let fut = newFuture[void]("navi.waitWrite")
  addWrite(fd, proc(f: AsyncFD): bool =
    if not fut.finished: fut.complete()
    true)
  fut

# --- TLS over the owned fd (ssl only) ----------------------------------

when defined(ssl):
  proc resumeSlot(cfg: TlsConfig, ctx: SslContext, origin: string): SessionSlot =
    ## When resumption is on and the client has a session cache, arm `ctx` to feed
    ## it and return a slot keyed by `origin`; otherwise nil (no resumption).
    if cfg.wantsResume and not cfg.sessionCache.isNil:
      let cache = cast[TlsSessionCache](cfg.sessionCache)
      enableResumption(ctx, cache)
      result = newSlot(cache, origin)

  proc driveHandshake(ssl: SslPtr, fd: AsyncFD, host: string) {.async.} =
    ## Non-blocking SSL_connect, awaiting readiness only when OpenSSL asks.
    while true:
      let r = SSL_connect(ssl)
      if r == 1: return
      case SSL_get_error(ssl, r)
      of SSL_ERROR_WANT_READ: await waitRead(fd)
      of SSL_ERROR_WANT_WRITE: await waitWrite(fd)
      else: raise newException(ValueError, "navi: TLS handshake failed for " & host)

  proc sslWrite(c: Conn, data: string) {.async.} =
    var off = 0
    while off < data.len:
      # OpenSSL requires the same buffer+len when retrying after WANT_WRITE; `data`
      # is captured by this async proc, so the pointer stays valid across awaits.
      let n = SSL_write(c.ssl, cast[cstring](unsafeAddr data[off]),
                        (data.len - off).cint).int
      if n > 0:
        off += n
      else:
        case SSL_get_error(c.ssl, n.cint)
        of SSL_ERROR_WANT_READ: await waitRead(c.fd)
        of SSL_ERROR_WANT_WRITE: await waitWrite(c.fd)
        else: raise newException(IOError, "navi: SSL_write failed")

  proc sslRead(c: Conn): Future[string] {.async.} =
    ## One chunk of up to 4096 bytes; "" means the peer closed.
    result = newString(4096)
    while true:
      let n = SSL_read(c.ssl, addr result[0], result.len.cint).int
      if n > 0:
        result.setLen(n); return
      case SSL_get_error(c.ssl, n.cint)
      of SSL_ERROR_WANT_READ: await waitRead(c.fd)
      of SSL_ERROR_WANT_WRITE: await waitWrite(c.fd)
      else: result.setLen(0); return   # ZERO_RETURN / reset -> EOF

proc proxyConnect(fd: AsyncFD, host: string, port: int) {.async.} =
  ## Establish a CONNECT tunnel to `host:port` through an already-connected proxy.
  let target = host & ":" & $port
  await send(fd, "CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n\r\n")
  let resp = await recv(fd, 1024)
  if not resp.startsWith("HTTP/1.1 200") and not resp.startsWith("HTTP/1.0 200"):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget, alpn: seq[string] = @[],
              connectMs = 0, readMs = 0): Future[Conn] {.async.} =
  ## Dial `host:port` (or the proxy), upgrading to TLS for https with a CONNECT
  ## tunnel when proxied. The handshake completes here so the ALPN result (h2 vs
  ## http/1.1) is known before any request. `connectMs` bounds establishment (TCP
  ## + TLS); `readMs` is stored for per-read timeouts. TLS requires `-d:ssl`.
  inc openedConnections
  var conn: Conn
  conn.fd = invalidFd
  conn.readMs = readMs

  proc establish() {.async.} =
    # Self-contained cleanup on failure: the SSL_CTX has no destructor, so a
    # failed handshake would otherwise leak it.
    try:
      let dialHost = if proxy.isSet: proxy.host else: host
      let dialPort = if proxy.isSet: proxy.port else: port
      let fd = await dial(dialHost, Port(dialPort), IPPROTO_TCP)
      conn.fd = fd
      setNoDelay(fd.SocketHandle)
      if proxy.isSet and tls:
        await proxyConnect(fd, host, port)
      if tls:
        when defined(ssl):
          # No ALPN over a proxy tunnel (the old path negotiated it lazily).
          let ctx = newTlsContext(cfg, if proxy.isSet: @[] else: alpn)
          conn.ctx = ctx
          conn.slot = resumeSlot(cfg, ctx, host & ":" & $port)
          conn.ssl = newClientSsl(ctx, fd.SocketHandle, host, conn.slot)
          await driveHandshake(conn.ssl, fd, host)
          verifyPeer(conn.ssl, host, cfg.wantsVerify)
          conn.protocol = negotiatedProtocol(conn.ssl)
        else:
          raise newException(ValueError, "navi: https requires compiling with -d:ssl")
    except CatchableError:
      when defined(ssl):
        if not conn.ssl.isNil: SSL_free(conn.ssl); conn.ssl = nil
        if not conn.ctx.isNil: conn.ctx.destroyContext(); conn.ctx = nil
      if conn.fd != invalidFd: closeSocket(conn.fd); conn.fd = invalidFd
      raise

  # On a connect timeout the establish future is abandoned (asyncdispatch has no
  # cancellation): it drains in the background and its socket is reclaimed later,
  # the same contract as the whole-request guard.
  let estFut = establish()
  if connectMs > 0 and not await withTimeout(estFut, connectMs):
    raise newException(response.TimeoutError,
                       "navi: connect timed out after " & $connectMs & " ms")
  await estFut
  return conn

proc sendAll*(c: Conn, data: string): Future[void] {.async.} =
  when defined(ssl):
    if not c.ssl.isNil:
      await sslWrite(c, data); return
  await send(c.fd, data)

proc recvSome*(c: Conn): Future[string] {.async.} =
  ## One chunk of up to 4096 bytes; "" means the peer closed. Bounded by `readMs`
  ## (the per-read stall timeout) when set; on expiry the pending read is abandoned
  ## and TimeoutError is raised.
  var readFut: Future[string]
  when defined(ssl):
    readFut = if not c.ssl.isNil: sslRead(c) else: recv(c.fd, 4096)
  else:
    readFut = recv(c.fd, 4096)
  if c.readMs > 0 and not await withTimeout(readFut, c.readMs):
    raise newException(response.TimeoutError,
                       "navi: read timed out after " & $c.readMs & " ms")
  return await readFut

proc closeSync*(c: Conn) =
  ## Synchronous close, for a destructor that cannot `await` (an abandoned
  ## streaming handle reclaimed by GC). Same teardown as `close`.
  when defined(ssl):
    if not c.ssl.isNil:
      discard SSL_shutdown(c.ssl)
      SSL_free(c.ssl)
  if c.fd != invalidFd: closeSocket(c.fd)
  when defined(ssl):
    # newContext leaves the SSL_CTX without a destructor; free it so a long-lived
    # client does not leak one context per connection.
    if not c.ctx.isNil: c.ctx.destroyContext()

proc close*(c: Conn): Future[void] {.async.} =
  c.closeSync()

proc sleep*(ms: int): Future[void] = sleepAsync(ms)

proc newTlsStore*(cfg: TlsConfig): RootRef =
  ## The per-client TLS session cache, or nil when resumption is off or
  ## unavailable (non-`-d:ssl` build). The entry puts it on `config.tls.sessionCache`.
  when defined(ssl):
    if cfg.wantsResume: result = newTlsSessionCache()
  else:
    discard cfg

proc closeTlsStore*(store: RootRef) =
  ## Free the sessions held by a `newTlsStore` cache. The entry calls this in `close`.
  when defined(ssl):
    if not store.isNil: close(cast[TlsSessionCache](store))
  else:
    discard store
