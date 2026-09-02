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

import std/[asyncdispatch, nativesockets, strutils, monotimes, times, base64]
import ./api, ./openssl_ctx, ./happyeyeballs
import ../core/response  # for navi's TimeoutError
import ../core/socks
when defined(ssl):
  import std/openssl
when defined(posix):
  from std/posix import Sockaddr_un, TSa_Family, EINPROGRESS, errno
  proc cConnect(fd: SocketHandle, sa: ptr SockAddr, sl: SockLen): cint
    {.importc: "connect", header: "<sys/socket.h>".}

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
    setSockOptInt(fd, nativesockets.IPPROTO_TCP.int, winlean.TCP_NODELAY.int, 1)
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
    closed: ref bool    ## shared across value copies: set by `close`, checked by a
                        ## parked `sslRead` so closing under an in-flight read yields
                        ## EOF instead of dereferencing the freed SSL (a UAF crash)
    when defined(ssl):
      ssl: SslPtr       ## the TLS connection; nil for plain http
      ctx: SslContext   ## the (usually shared) SSL_CTX this connection used
      ownsCtx: bool     ## true only for an unshared ctx `close` must destroy
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
  proc resumeSlot(cfg: TlsConfig, origin: string): SessionSlot =
    ## When resumption is on and the client has a session cache, return a slot keyed
    ## by `origin`; otherwise nil. The context is armed once in `obtainContext`, so
    ## this only mints the per-connection link.
    if cfg.wantsResume and not cfg.sessionCache.isNil:
      result = newSlot(cast[TlsSessionCache](cfg.sessionCache), origin)

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
      # If `close` ran while we were parked on waitRead, the SSL is already freed.
      # Raise rather than reading through the dangling pointer (a UAF crash) -- and
      # rather than returning "" (a clean peer-EOF), which the h1 body reader would
      # take as "read more" on an unfinished stream and spin. The stream layer
      # treats this as a drop.
      if not c.closed.isNil and c.closed[]:
        raise newException(IOError, "navi: connection closed")
      let n = SSL_read(c.ssl, addr result[0], result.len.cint).int
      if n > 0:
        result.setLen(n); return
      case SSL_get_error(c.ssl, n.cint)
      of SSL_ERROR_WANT_READ: await waitRead(c.fd)
      of SSL_ERROR_WANT_WRITE: await waitWrite(c.fd)
      else: result.setLen(0); return   # ZERO_RETURN / reset -> EOF

proc proxyConnect(fd: AsyncFD, host: string, port: int, user, pass: string) {.async.} =
  ## Establish a CONNECT tunnel to `host:port` through an already-connected HTTP
  ## proxy, sending Proxy-Authorization when credentials are supplied.
  let target = host & ":" & $port
  var req = "CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n"
  if user.len > 0 or pass.len > 0:
    req.add("Proxy-Authorization: Basic " & encode(user & ":" & pass) & "\r\n")
  req.add("\r\n")
  await send(fd, req)
  let resp = await recv(fd, 1024)
  if not resp.startsWith("HTTP/1.1 200") and not resp.startsWith("HTTP/1.0 200"):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

proc recvExactly(fd: AsyncFD, n: int): Future[string] {.async.} =
  ## Read exactly `n` bytes or raise; SOCKS5 replies are fixed-size frames.
  result = ""
  while result.len < n:
    let chunk = await recv(fd, n - result.len)
    if chunk.len == 0: raise newException(IOError, "navi: SOCKS5 proxy closed the connection")
    result.add chunk

proc socksConnect(fd: AsyncFD, host: string, port: int, user, pass: string) {.async.} =
  ## SOCKS5 handshake to tunnel to `host:port` (RFC 1928 + RFC 1929). The target is
  ## sent as a domain name so the proxy resolves DNS.
  let hasAuth = user.len > 0 or pass.len > 0
  await send(fd, greeting(hasAuth))
  case selectedMethod(await recvExactly(fd, 2))
  of methodUserPass:
    if not hasAuth:
      raise newException(ValueError, "navi: SOCKS5 proxy requires authentication")
    await send(fd, authRequest(user, pass))
    checkAuthReply(await recvExactly(fd, 2))
  of methodNoAuth: discard
  else: raise newException(ValueError, "navi: SOCKS5 proxy rejected the offered auth methods")
  await send(fd, connectRequest(host, port))
  let header = await recvExactly(fd, 4)
  let status = replyStatus(header)
  if status != 0: raiseReply(status)
  let tail = boundTailLen(int(uint8(header[3])))
  if tail >= 0: discard await recvExactly(fd, tail)
  else:
    let dlen = int(uint8((await recvExactly(fd, 1))[0]))
    discard await recvExactly(fd, dlen + 2)

proc unixConnect(path: string): Future[AsyncFD] {.async.} =
  ## Connect a non-blocking AF_UNIX/SOCK_STREAM socket to `path`, awaiting
  ## writability on EINPROGRESS just like the TCP async connect.
  when not defined(posix):
    raise newException(ValueError,
      "navi: Unix domain sockets are only supported on POSIX")
  else:
    var sa: Sockaddr_un
    if path.len >= sizeof(sa.sun_path):
      raise newException(ValueError,
        "navi: Unix socket path exceeds " & $(sizeof(sa.sun_path) - 1) &
        " bytes: " & path)
    sa.sun_family = TSa_Family(toInt(nativesockets.AF_UNIX))
    for i in 0 ..< path.len: sa.sun_path[i] = path[i]
    let sh = createNativeSocket(nativesockets.AF_UNIX, nativesockets.SOCK_STREAM,
                                nativesockets.IPPROTO_IP)  # protocol 0
    if sh == osInvalidSocket:
      raise newException(IOError, "navi: could not create a Unix socket")
    sh.setBlocking(false)
    register(sh.AsyncFD)
    let fd = sh.AsyncFD
    if cConnect(fd.SocketHandle, cast[ptr SockAddr](addr sa), SockLen(sizeof(sa))) != 0:
      if errno != EINPROGRESS:
        closeSocket(fd)
        raise newException(IOError, "navi: could not connect to Unix socket " & path)
      await waitWrite(fd)
      let err = getSockOptInt(fd.SocketHandle, SOL_SOCKET.int, SO_ERROR.int)
      if err != 0:
        closeSocket(fd)
        raise newException(IOError,
          "navi: could not connect to Unix socket " & path & " (errno " & $err & ")")
    return fd

proc happyConnect*(ips: seq[string], port: int):
    Future[tuple[fd: AsyncFD, idx: int]] {.async.} =
  ## Happy Eyeballs (RFC 8305): start non-blocking connects to `ips` (already
  ## interleaved by family) staggered by ~250ms, and return the (fd, index) of the
  ## first to complete, so a slow or blackholed address does not stall the others.
  ## Losing attempts are closed. The overall bound is applied by the caller
  ## (`withTimeout` on `establish`). asyncdispatch has no cancellation, so a loser's
  ## connect future drains in the background once its fd is closed.
  if ips.len == 0:
    raise newException(IOError, "navi: no address to connect to")
  var
    inflight: seq[tuple[fd: AsyncFD, fut: Future[void], idx: int]]
    nextIdx = 0
    lastStart: MonoTime
    lastErr = "no address"

  proc reap(fd: AsyncFD, fut: Future[void]) =
    ## Release a losing attempt's socket. Closing an fd that still has an in-flight
    ## asyncdispatch connect from outside its callback corrupts the dispatcher
    ## ("File descriptor not registered"), so if the connect is still pending we
    ## defer the close to its own completion (a true blackhole resolves when the OS
    ## connect times out); an already-finished attempt is closed at once.
    if fut.finished:
      closeSocket(fd)
    else:
      fut.callback = proc() = closeSocket(fd)

  proc launch() =
    let ip = ips[nextIdx]
    let domain = if ':' in ip: Domain.AF_INET6 else: Domain.AF_INET
    let idx = nextIdx
    inc nextIdx
    lastStart = getMonoTime()
    let fd = createAsyncNativeSocket(domain, SOCK_STREAM, IPPROTO_TCP)
    if fd == osInvalidSocket.AsyncFD:
      lastErr = "could not create socket"; return
    setNoDelay(fd.SocketHandle)
    inflight.add (fd, connect(fd, ip, Port(port), domain), idx)

  try:
    while true:
      # Start the next attempt: the first at once; the rest when nothing is in
      # flight or the stagger window has elapsed.
      if nextIdx < ips.len and
         (inflight.len == 0 or
          (getMonoTime() - lastStart).inMilliseconds >= heAttemptDelayMs):
        launch()
        continue
      if inflight.len == 0:
        break                       # nothing pending and nothing left to start
      # Wake on any inflight connect finishing, or -- if attempts remain -- the
      # stagger window, whichever is first.
      let waker = newFuture[void]("navi.he.wake")
      for e in inflight:
        e.fut.callback = proc() =
          if not waker.finished: waker.complete()
      var timer: Future[void] = nil
      if nextIdx < ips.len:
        timer = sleepAsync(heAttemptDelayMs)
        timer.callback = proc() =
          if not waker.finished: waker.complete()
      await waker
      if timer != nil: timer.clearCallbacks()
      # Harvest finished attempts: first success wins; failures are dropped.
      var i = 0
      while i < inflight.len:
        let e = inflight[i]
        if e.fut.finished:
          e.fut.clearCallbacks()
          if e.fut.failed:
            lastErr = e.fut.error.msg
            closeSocket(e.fd)                        # finished: safe to close now
            inflight.delete(i)
          else:
            for j in 0 ..< inflight.len:             # release the losing attempts
              if j != i:
                inflight[j].fut.clearCallbacks()
                reap(inflight[j].fd, inflight[j].fut)
            return (e.fd, e.idx)
        else:
          e.fut.clearCallbacks()                     # re-armed next iteration
          inc i
  except CatchableError:
    for e in inflight:
      e.fut.clearCallbacks()
      reap(e.fd, e.fut)
    raise
  raise newException(IOError, "navi: could not connect: " & lastErr)

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
  conn.closed = new(bool)   # shared teardown flag (see Conn.closed)

  proc establish() {.async.} =
    if proxy.kind == pkUnix:
      conn.fd = await unixConnect(proxy.host)
      if tls:
        when defined(ssl):
          (conn.ctx, conn.ownsCtx) = obtainContext(cfg.contextStore, cfg, alpn)
          conn.slot = resumeSlot(cfg, host & ":" & $port)
          conn.ssl = newClientSsl(conn.ctx, conn.fd.SocketHandle, host, conn.slot)
          await driveHandshake(conn.ssl, conn.fd, host)
          verifyPeer(conn.ssl, host, cfg.wantsVerify)
          postHandshakeVerify(conn.ssl, host, cfg)
          conn.protocol = negotiatedProtocol(conn.ssl)
        else:
          raise newException(ValueError, "navi: https requires compiling with -d:ssl")
      return
    let dialHost = if proxy.isSet: proxy.host else: host
    let dialPort = if proxy.isSet: proxy.port else: port
    var pool = resolveAddrs(dialHost, dialPort)
    if pool.len == 0:
      raise newException(IOError, "navi: could not resolve " & dialHost)
    var lastErr: ref CatchableError
    # Happy-Eyeballs TCP race, then proxy/TLS on the winner; on a *handshake*
    # failure drop that address and re-race the rest (as sync's connectAcross does).
    while pool.len > 0:
      let (fd, idx) = await happyConnect(pool, dialPort)
      conn.fd = fd
      try:
        # SOCKS5 tunnels every target; an HTTP proxy tunnels only https (CONNECT).
        if proxy.kind == pkSocks5:
          await socksConnect(fd, host, port, proxy.user, proxy.pass)
        elif proxy.isSet and tls:
          await proxyConnect(fd, host, port, proxy.user, proxy.pass)
        if tls:
          when defined(ssl):
            # A CONNECT tunnel (HTTP proxy) and a SOCKS5 tunnel are both transparent
            # once established: the TLS handshake runs end-to-end to the origin, so
            # ALPN belongs on it -- offer the normal list to reach h2 (parity with the
            # sync and chronos backends).
            (conn.ctx, conn.ownsCtx) = obtainContext(cfg.contextStore, cfg, alpn)
            conn.slot = resumeSlot(cfg, host & ":" & $port)
            conn.ssl = newClientSsl(conn.ctx, fd.SocketHandle, host, conn.slot)
            await driveHandshake(conn.ssl, fd, host)
            verifyPeer(conn.ssl, host, cfg.wantsVerify)
            postHandshakeVerify(conn.ssl, host, cfg)   # SPKI pin + verify callback
            conn.protocol = negotiatedProtocol(conn.ssl)
          else:
            raise newException(ValueError, "navi: https requires compiling with -d:ssl")
        return                                   # established
      except CatchableError as e:
        # Tear down this attempt (the SSL_CTX has no destructor, so a failed
        # handshake would leak an unshared one), then try the remaining addresses.
        when defined(ssl):
          if not conn.ssl.isNil: SSL_free(conn.ssl); conn.ssl = nil
          if conn.ownsCtx and not conn.ctx.isNil: conn.ctx.destroyContext()
          conn.ctx = nil
        closeSocket(fd); conn.fd = invalidFd
        pool.delete(idx)
        lastErr = e
    raise lastErr

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

proc shutdownConn*(c: Conn) =
  ## Shut the socket down in both directions so a pending read or write unblocks
  ## with EOF/error. Used to wake the h2 mux's background reader on client close so
  ## it exits its loop (and does the real close itself) instead of being left
  ## suspended on a closed fd, which would crash at process teardown. Does not free
  ## anything; `close`/`closeSync` still run afterward.
  if c.fd == invalidFd: return
  when defined(windows):
    discard winlean.shutdown(c.fd.SocketHandle, 2)          # SD_BOTH
  else:
    discard posix.shutdown(c.fd.SocketHandle, posix.SHUT_RDWR)

proc freeConn(c: Conn) =
  ## The raw teardown: free the SSL and close the fd. Callers set/guard the
  ## `closed` flag first.
  when defined(ssl):
    if not c.ssl.isNil:
      discard SSL_shutdown(c.ssl)
      SSL_free(c.ssl)
  if c.fd != invalidFd: closeSocket(c.fd)
  when defined(ssl):
    # A shared ctx is freed with the client's context store, not here; only an
    # unshared one (bare TlsConfig, e.g. interop tests) is destroyed per connection.
    if c.ownsCtx and not c.ctx.isNil: c.ctx.destroyContext()

proc closeSync*(c: Conn) =
  ## Synchronous close, for a destructor that cannot `await` (an abandoned
  ## streaming handle reclaimed by GC). No read is parked on a GC-reclaimed handle,
  ## so freeing directly is safe; `close` handles the read-in-flight case.
  if not c.closed.isNil:
    if c.closed[]: return               # idempotent; stops a double-free
    c.closed[] = true
  freeConn(c)

proc close*(c: Conn): Future[void] {.async.} =
  ## Close, safe to call while a `sslRead` is parked (e.g. stopping an SSE stream):
  ## flag the teardown, shut the socket down to wake the parked read, then yield one
  ## tick so the dispatcher delivers that wake (the read observes EOF via the flag)
  ## before we free the fd. Freeing in the same atomic step would lose the wake and
  ## hang the reader on an unregistered fd.
  if not c.closed.isNil:
    if c.closed[]: return
    c.closed[] = true
    shutdownConn(c)
    await sleepAsync(0)
  freeConn(c)

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

proc newTlsCtxStore*(cfg: TlsConfig): RootRef =
  ## The per-client shared TLS-context store (empty until the first TLS connect),
  ## or nil on a non-`-d:ssl` build. The entry puts it on `config.tls.contextStore`.
  when defined(ssl):
    result = newTlsContextStore()
  else:
    discard cfg

proc closeTlsCtxStore*(store: RootRef) =
  ## Free the shared contexts held by a `newTlsCtxStore`. The entry calls this in
  ## `close`, after `closeIdle` has shut the pooled connections.
  when defined(ssl):
    if not store.isNil: close(cast[TlsContextStore](store))
  else:
    discard store
