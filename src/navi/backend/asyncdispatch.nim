## Asynchronous transport backend built on std/asyncnet.

import std/[asyncdispatch, asyncnet, net, nativesockets, strutils]
import ./api, ./openssl_ctx

export api, asyncdispatch

# Disable Nagle on the connection socket: without it the TLS handshake's final
# flight plus the first request stall ~40ms on the peer's delayed ACK, paid on
# every fresh (unpooled) connection. asyncnet does not set this, so navi does
# (the sync backend does the same in its own tcpConnect).
when defined(windows):
  import std/winlean
  proc setNoDelay(fd: SocketHandle) =
    setSockOptInt(fd, winlean.IPPROTO_TCP.int, winlean.TCP_NODELAY.int, 1)
else:
  import std/posix
  proc setNoDelay(fd: SocketHandle) =
    setSockOptInt(fd, posix.IPPROTO_TCP.int, posix.TCP_NODELAY.int, 1)

type
  Conn* = object
    socket: AsyncSocket
    protocol*: string   ## ALPN-negotiated protocol ("h2" or "", meaning http/1.1)
    when defined(ssl):
      ctx: SslContext   ## kept so `close` can free the SSL_CTX (destroyContext)
      slot: SessionSlot ## keeps the resumption link alive for the SSL's lifetime

var openedConnections*: int  ## diagnostic: TCP connections opened by this backend

when defined(ssl):
  proc resumeSlot(cfg: TlsConfig, ctx: SslContext, origin: string): SessionSlot =
    ## When resumption is on and the client has a session cache, arm `ctx` to feed
    ## it and return a slot keyed by `origin`; otherwise nil (no resumption).
    if cfg.wantsResume and not cfg.sessionCache.isNil:
      let cache = cast[TlsSessionCache](cfg.sessionCache)
      enableResumption(ctx, cache)
      result = newSlot(cache, origin)

proc proxyConnect(socket: AsyncSocket, host: string, port: int) {.async.} =
  let target = host & ":" & $port
  await socket.send("CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n\r\n")
  let resp = await socket.recv(1024)
  if not resp.startsWith("HTTP/1.1 200") and not resp.startsWith("HTTP/1.0 200"):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

proc pickDomain(host: string, port: int): Domain =
  ## Resolve the address family so an IPv6 target gets an AF_INET6 socket.
  var ai = getAddrInfo(host, Port(port), AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP)
  result = if ai.ai_family == toInt(AF_INET6): AF_INET6 else: AF_INET
  freeAddrInfo(ai)

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget, alpn: seq[string] = @[]): Future[Conn] {.async.} =
  ## Dial `host:port` (or the proxy), upgrading to TLS for https with a CONNECT
  ## tunnel when proxied. Unbuffered so recv returns the available chunk instead
  ## of blocking to fill the buffer. TLS requires `-d:ssl`.
  inc openedConnections
  # Release the socket and TLS context if we don't finish connecting -- both
  # close ops are synchronous, so this works in an async proc. The SSL_CTX has no
  # destructor, so a failed handshake would otherwise leak it permanently.
  var established = false
  defer:
    if not established:
      if not result.socket.isNil: result.socket.close()
      when defined(ssl):
        if not result.ctx.isNil: result.ctx.destroyContext()

  when defined(ssl):
    if tls and not proxy.isSet:
      # Direct TLS: connect a wrapped socket so the handshake completes here and
      # the ALPN result (h2 vs http/1.1) is available before any request.
      # NB: no handshake-aware address fallback here (the sync backend has it):
      # asyncnet's `connect` binds SNI to the connect host and hides the handshake
      # loop, so falling through to another IP needs navi to own the async TLS
      # handshake -- a later increment.
      let ctx = newTlsContext(cfg, alpn)   # verify/CA + ALPN + any client cert
      result.ctx = ctx           # store before the handshake so cleanup frees it
      result.slot = resumeSlot(cfg, ctx, host & ":" & $port)  # arm ctx before SSL_new
      let socket = newAsyncSocket(pickDomain(host, port), SOCK_STREAM,
                                  IPPROTO_TCP, buffered = false)
      result.socket = socket
      setNoDelay(socket.getFd())
      wrapSocket(ctx, socket)
      applySession(socket.sslHandle, result.slot)  # present cached session pre-handshake
      await socket.connect(host, Port(port))
      result.protocol = negotiatedProtocol(socket.sslHandle)
      established = true
      return

  let socket =
    if proxy.isSet: await asyncnet.dial(proxy.host, Port(proxy.port), buffered = false)
    else: await asyncnet.dial(host, Port(port), buffered = false)
  result.socket = socket
  setNoDelay(socket.getFd())
  if proxy.isSet and tls:
    await proxyConnect(socket, host, port)
    when defined(ssl):
      # TLS over the proxy tunnel; the handshake (and any ALPN) completes lazily
      # on first I/O, so this path stays http/1.1.
      # No ALPN here: the tunnelled handshake completes lazily on first I/O, so
      # this path stays http/1.1.
      let ctx = newTlsContext(cfg)         # verify/CA + any client cert, no ALPN
      result.ctx = ctx           # store before the handshake so cleanup frees it
      ctx.wrapConnectedSocket(socket, handshakeAsClient, host)
  established = true

proc sendAll*(c: Conn, data: string): Future[void] =
  c.socket.send(data)

proc recvSome*(c: Conn): Future[string] =
  ## One chunk of up to 4096 bytes; "" means the peer closed.
  c.socket.recv(4096)

proc close*(c: Conn): Future[void] {.async.} =
  c.socket.close()
  when defined(ssl):
    # Free the SSL_CTX std/net leaves behind (see the sync backend); otherwise a
    # long-lived client leaks one context per connection.
    if not c.ctx.isNil: c.ctx.destroyContext()

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
