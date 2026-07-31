## Synchronous transport backend: blocking sockets we own directly.
##
## This does not use std/net's `Socket`/`dial`/`wrapConnectedSocket`. It owns the
## raw socket (std/nativesockets + the platform connect/send/recv), drives the
## TLS handshake and read/write itself (OpenSSL via openssl_ctx), and keeps only
## std/net's `newContext` (the verified TLS context, reached through
## openssl_ctx). `await` is an identity template so the shared engine's
## `await`-shaped body compiles to straight-line blocking code.

import std/[os, strutils, nativesockets, monotimes, times]
import ./api, ./openssl_ctx
import ../core/response  # for navi's TimeoutError
when defined(ssl):
  import std/openssl

export api

when defined(windows):
  import std/winlean
  proc sysConnect(fd: SocketHandle, sa: ptr SockAddr, sl: SockLen): cint =
    winlean.connect(fd, sa, cint(sl))
  proc sysSend(fd: SocketHandle, buf: pointer, n: int): int =
    winlean.send(fd, cast[cstring](buf), cint(n), 0'i32).int
  proc sysRecv(fd: SocketHandle, buf: pointer, n: int): int =
    winlean.recv(fd, cast[cstring](buf), cint(n), 0'i32).int
  proc setNoDelay(fd: SocketHandle) =
    setSockOptInt(fd, winlean.IPPROTO_TCP.int, winlean.TCP_NODELAY.int, 1)
  proc connectInProgress(): bool = osLastError().int32 == WSAEWOULDBLOCK
  proc setIoTimeout(fd: SocketHandle, ms: int) =
    setSockOptInt(fd, SOL_SOCKET.int, SO_RCVTIMEO.int, ms)  # win: DWORD ms
    setSockOptInt(fd, SOL_SOCKET.int, SO_SNDTIMEO.int, ms)
else:
  import std/posix
  proc sysConnect(fd: SocketHandle, sa: ptr SockAddr, sl: SockLen): cint =
    posix.connect(fd, sa, sl)
  proc sysSend(fd: SocketHandle, buf: pointer, n: int): int =
    posix.send(fd, buf, n, 0'i32)
  proc sysRecv(fd: SocketHandle, buf: pointer, n: int): int =
    posix.recv(fd, buf, n, 0'i32)
  proc setNoDelay(fd: SocketHandle) =
    setSockOptInt(fd, posix.IPPROTO_TCP.int, posix.TCP_NODELAY.int, 1)
  proc connectInProgress(): bool = errno == EINPROGRESS
  proc setIoTimeout(fd: SocketHandle, ms: int) =
    var tv = Timeval(tv_sec: posix.Time(ms div 1000),
                     tv_usec: Suseconds((ms mod 1000) * 1000))
    discard setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, addr tv, SockLen(sizeof tv))
    discard setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, addr tv, SockLen(sizeof tv))

proc waitWritable(fd: SocketHandle, ms: int): bool =
  ## True once the (non-blocking) socket becomes writable within `ms` ms -- i.e.
  ## the async connect finished. selectWrite mutates its seq, so pass a fresh one.
  var fds = @[fd]
  selectWrite(fds, ms) > 0

type
  Conn* = object
    fd: SocketHandle
    protocol*: string   ## ALPN-negotiated protocol ("h2" or "", meaning http/1.1)
    readMs: int         ## per-read stall timeout in ms; 0 blocks indefinitely
    deadline: MonoTime  ## absolute overall deadline; enforced when `bounded`
    bounded: bool       ## whether `deadline` is active (total timeout set)
    when defined(ssl):
      ssl: SslPtr       ## the TLS connection; nil for plain http
      ctx: SslContext   ## kept so `close` can free the SSL_CTX (destroyContext)
      slot: SessionSlot ## keeps the resumption link alive for the SSL's lifetime

template await*(x: untyped): untyped = x

proc sleep*(ms: int) = os.sleep(ms)

proc tcpConnect(host: string, port: int, connectMs = 0): SocketHandle =
  ## Resolve `host` and connect to the first address that accepts a TCP
  ## connection (IPv4 or IPv6, in the resolver's order). With `connectMs` > 0 the
  ## connect is bounded (non-blocking connect + select); otherwise it blocks (the
  ## OS default). Raises `TimeoutError` on a connect timeout, else `IOError`.
  var ai = getAddrInfo(host, Port(port), AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP)
  var it = ai
  var lastErr = "no address"
  var timedOut = false
  result = osInvalidSocket
  while it != nil:
    let fd = createNativeSocket(it.ai_family, it.ai_socktype, it.ai_protocol)
    if fd != osInvalidSocket:
      # Disable Nagle: HTTP is request/response, and with Nagle on, the TLS
      # handshake's final flight plus the first request stall ~40ms on the peer's
      # delayed ACK -- paid on every fresh (unpooled) connection.
      setNoDelay(fd)
      if connectMs <= 0:
        if sysConnect(fd, it.ai_addr, it.ai_addrlen) == 0'i32:
          result = fd; break
        lastErr = osErrorMsg(osLastError()); close(fd)
      else:
        fd.setBlocking(false)
        if sysConnect(fd, it.ai_addr, it.ai_addrlen) == 0'i32:
          fd.setBlocking(true); result = fd; break     # connected immediately
        elif not connectInProgress():
          lastErr = osErrorMsg(osLastError()); close(fd)
        elif not waitWritable(fd, connectMs):
          timedOut = true
          lastErr = "connect timed out after " & $connectMs & " ms"; close(fd)
        elif getSockOptInt(fd, SOL_SOCKET.int, SO_ERROR.int) != 0:
          lastErr = "connection refused"; close(fd)
        else:
          fd.setBlocking(true); result = fd; break     # async connect succeeded
    it = it.ai_next
  freeAddrInfo(ai)
  if result == osInvalidSocket:
    if timedOut:
      raise newException(response.TimeoutError, "navi: " & lastErr)
    raise newException(IOError, "navi: could not connect to " & host & ": " & lastErr)

proc sendRaw(fd: SocketHandle, data: string) =
  var off = 0
  while off < data.len:
    let n = sysSend(fd, unsafeAddr data[off], data.len - off)
    if n <= 0: raise newException(IOError, "navi: socket write failed")
    off += n

proc proxyConnect(fd: SocketHandle, host: string, port: int) =
  ## Establish a CONNECT tunnel to `host:port` through an already-connected proxy.
  let target = host & ":" & $port
  sendRaw(fd, "CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n\r\n")
  var resp = newString(1024)
  let n = sysRecv(fd, addr resp[0], resp.len)
  resp.setLen(max(n, 0))
  if not resp.startsWith("HTTP/1.1 200") and not resp.startsWith("HTTP/1.0 200"):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

when defined(ssl):
  proc resolveAddrs(host: string, port: int): seq[string] =
    ## Numeric addresses for `host`, in the resolver's order (RFC 6724). We
    ## resolve ourselves so the connect loop can iterate them one at a time.
    var ai = getAddrInfo(host, Port(port), AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP)
    var it = ai
    while it != nil:
      result.add getAddrString(it.ai_addr)
      it = it.ai_next
    freeAddrInfo(ai)

  proc resumeSlot(cfg: TlsConfig, ctx: SslContext, origin: string): SessionSlot =
    ## When resumption is on and the client has a session cache, arm `ctx` to feed
    ## it and return a slot keyed by `origin`; otherwise nil (no resumption).
    if cfg.wantsResume and not cfg.sessionCache.isNil:
      let cache = cast[TlsSessionCache](cfg.sessionCache)
      enableResumption(ctx, cache)
      result = newSlot(cache, origin)

  proc connectAcross*(ctx: SslContext, ips: openArray[string], sni: string,
                      port: int, verify: bool, slot: SessionSlot = nil,
                      connectMs = 0): Conn =
    ## TCP-connect + TLS-handshake across `ips` in order, returning the first that
    ## completes the handshake; the connection uses each IP while SNI and
    ## verification use `sni`. Falls through on a connect *or handshake* failure,
    ## so a dead endpoint on a multi-homed host (a partially-broken CDN pool) no
    ## longer fails the whole request. `connectMs` > 0 bounds each address's TCP
    ## connect and TLS handshake. `ctx`/`slot` are set on the returned Conn by the
    ## caller. Exported for the fallback test; navi reaches it through `connect`.
    result.fd = osInvalidSocket
    if ips.len == 0:
      raise newException(IOError, "navi: could not resolve " & sni)
    var lastErr: ref CatchableError
    for ip in ips:
      var fd = osInvalidSocket
      try:
        fd = tcpConnect(ip, port, connectMs)
        if connectMs > 0: setIoTimeout(fd, connectMs)   # bound the blocking handshake
        result.ssl = startClientTls(ctx, fd, sni, verify, slot)
        if connectMs > 0: setIoTimeout(fd, 0)           # clear; reads use selectRead
        result.fd = fd
        result.slot = slot
        result.protocol = negotiatedProtocol(result.ssl)
        return
      except CatchableError as e:
        if fd != osInvalidSocket: close(fd)
        lastErr = e
    raise lastErr

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget, alpn: seq[string] = @[],
              connectMs = 0, readMs = 0, totalMs = 0): Conn =
  ## Connect to `host:port` (IPv4 or IPv6), upgrading to TLS for https. Through a
  ## proxy, https targets get a CONNECT tunnel and http targets connect directly
  ## to the proxy (the engine sends an absolute-URI). `connectMs` bounds TCP
  ## connect + TLS handshake; `readMs` is the per-read stall limit; `totalMs` is
  ## the overall (per-attempt) deadline. TLS requires `-d:ssl`.
  # A total deadline also caps establishment when no explicit connect limit is set.
  let establishMs = if connectMs > 0: connectMs else: totalMs
  result.fd = osInvalidSocket
  result.readMs = readMs
  if totalMs > 0:
    result.deadline = getMonoTime() + initDuration(milliseconds = totalMs)
    result.bounded = true
  # Release the socket and TLS context if we don't finish connecting -- a failed
  # proxy CONNECT or TLS handshake would otherwise leak the fd and the SSL_CTX
  # (which has no destructor).
  var established = false
  defer:
    if not established:
      when defined(ssl):
        if not result.ssl.isNil: SSL_free(result.ssl)
        if not result.ctx.isNil: result.ctx.destroyContext()
      if result.fd != osInvalidSocket:
        close(result.fd)
  if proxy.isSet:
    result.fd = tcpConnect(proxy.host, proxy.port, establishMs)
    if tls:
      if establishMs > 0: setIoTimeout(result.fd, establishMs)
      proxyConnect(result.fd, host, port)
      when defined(ssl):
        result.ctx = newTlsContext(cfg, alpn)   # verify/CA + ALPN + any client cert
        result.slot = resumeSlot(cfg, result.ctx, host & ":" & $port)
        result.ssl = startClientTls(result.ctx, result.fd, host, cfg.wantsVerify,
                                    result.slot)
        if establishMs > 0: setIoTimeout(result.fd, 0)
        result.protocol = negotiatedProtocol(result.ssl)
      else:
        raise newException(ValueError, "navi: https requires compiling with -d:ssl")
  elif tls:
    when defined(ssl):
      let ctx = newTlsContext(cfg, alpn)   # verify/CA + ALPN + any client cert
      let slot = resumeSlot(cfg, ctx, host & ":" & $port)
      let dl = result.deadline
      let bd = result.bounded
      # Handshake-aware fallback across the resolved addresses (see connectAcross).
      # connectAcross returns a fresh Conn, so `result.ctx` is nil until we set it
      # below; free `ctx` here if the handshake fails, before `result` is assigned
      # (the connect-cleanup defer cannot see it yet).
      try:
        result = connectAcross(ctx, resolveAddrs(host, port), host, port,
                               cfg.wantsVerify, slot, establishMs)
      except CatchableError:
        ctx.destroyContext()
        raise
      result.ctx = ctx
      result.readMs = readMs
      result.deadline = dl
      result.bounded = bd
    else:
      raise newException(ValueError, "navi: https requires compiling with -d:ssl")
  else:
    result.fd = tcpConnect(host, port, establishMs)
  established = true

proc sendAll*(c: Conn, data: string) =
  if data.len == 0: return
  when defined(ssl):
    if not c.ssl.isNil:
      var off = 0
      while off < data.len:
        let n = SSL_write(c.ssl, cast[cstring](unsafeAddr data[off]), data.len - off).int
        if n <= 0: raise newException(IOError, "navi: SSL_write failed")
        off += n
      return
  sendRaw(c.fd, data)

proc waitReadable(c: Conn, ms: int): bool =
  ## True when the socket has data ready within `ms`. Checks OpenSSL's decrypted
  ## buffer first: `select` sees the raw fd, so bytes SSL already drained off it
  ## and buffered would otherwise be missed.
  when defined(ssl):
    if not c.ssl.isNil and SSL_pending(c.ssl) > 0:
      return true
  var fds = @[c.fd]
  selectRead(fds, ms) > 0

proc recvSome*(c: Conn): string =
  ## One chunk of up to 4096 bytes; "" means the peer closed. Waits up to the read
  ## stall limit -- capped by the time left to the overall deadline -- then reads
  ## what is available in a single read (no fill-the-buffer loop). Raises navi's
  ## TimeoutError on a per-read stall or an expired total deadline.
  result = newString(4096)
  var waitMs = c.readMs
  if c.bounded:
    let remaining = (c.deadline - getMonoTime()).inMilliseconds.int
    if remaining <= 0:
      raise newException(response.TimeoutError, "navi: request timed out")
    waitMs = if waitMs <= 0: remaining else: min(waitMs, remaining)
  if waitMs > 0 and not c.waitReadable(waitMs):
    if c.bounded and (c.deadline - getMonoTime()).inMilliseconds.int <= 0:
      raise newException(response.TimeoutError, "navi: request timed out")
    raise newException(response.TimeoutError, "navi: read timed out")
  var n: int
  when defined(ssl):
    if not c.ssl.isNil:
      n = SSL_read(c.ssl, addr result[0], result.len).int
    else:
      n = sysRecv(c.fd, addr result[0], result.len)
  else:
    n = sysRecv(c.fd, addr result[0], result.len)
  if n <= 0:
    result.setLen(0)
  else:
    result.setLen(n)

proc close*(c: Conn) =
  when defined(ssl):
    if not c.ssl.isNil:
      discard SSL_shutdown(c.ssl)
      SSL_free(c.ssl)
  if c.fd != osInvalidSocket:
    close(c.fd)
  when defined(ssl):
    # newContext leaves the SSL_CTX without a destructor; free it so a long-lived
    # client does not leak one context (~85 KB) per connection.
    if not c.ctx.isNil: c.ctx.destroyContext()

proc newTlsStore*(cfg: TlsConfig): RootRef =
  ## The per-client TLS session cache for this backend, or nil when resumption is
  ## off or unavailable (a non-`-d:ssl` build). Entries put it on
  ## `config.tls.sessionCache` in `newNavi`.
  when defined(ssl):
    if cfg.wantsResume: result = newTlsSessionCache()
  else:
    discard cfg

proc closeTlsStore*(store: RootRef) =
  ## Free the sessions held by a `newTlsStore` cache. Entries call this in `close`.
  when defined(ssl):
    if not store.isNil: close(cast[TlsSessionCache](store))
  else:
    discard store
