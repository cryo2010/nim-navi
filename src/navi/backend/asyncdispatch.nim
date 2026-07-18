## Asynchronous transport backend built on std/asyncnet.

import std/[asyncdispatch, asyncnet, net, nativesockets, strutils]
import ./api, ./openssl_alpn

export api, asyncdispatch

type
  Conn* = object
    socket: AsyncSocket
    protocol*: string   ## ALPN-negotiated protocol ("h2" or "", meaning http/1.1)
    when defined(ssl):
      ctx: SslContext   ## kept so `close` can free the SSL_CTX (destroyContext)

var openedConnections*: int  ## diagnostic: TCP connections opened by this backend

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
      let ctx = newContext(
        verifyMode = if cfg.wantsVerify: CVerifyPeer else: CVerifyNone,
        certFile = cfg.certFile, keyFile = cfg.clientKeyFile,
        caFile = cfg.caFile)
      result.ctx = ctx           # store before the handshake so cleanup frees it
      setAlpn(ctx.context, alpn)
      let socket = newAsyncSocket(pickDomain(host, port), SOCK_STREAM,
                                  IPPROTO_TCP, buffered = false)
      result.socket = socket
      wrapSocket(ctx, socket)
      await socket.connect(host, Port(port))
      result.protocol = negotiatedProtocol(socket.sslHandle)
      established = true
      return

  let socket =
    if proxy.isSet: await asyncnet.dial(proxy.host, Port(proxy.port), buffered = false)
    else: await asyncnet.dial(host, Port(port), buffered = false)
  result.socket = socket
  if proxy.isSet and tls:
    await proxyConnect(socket, host, port)
    when defined(ssl):
      # TLS over the proxy tunnel; the handshake (and any ALPN) completes lazily
      # on first I/O, so this path stays http/1.1.
      let ctx = newContext(
        verifyMode = if cfg.wantsVerify: CVerifyPeer else: CVerifyNone,
        certFile = cfg.certFile, keyFile = cfg.clientKeyFile,
        caFile = cfg.caFile)
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
