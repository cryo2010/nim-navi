## Synchronous transport backend: blocking sockets we own directly.
##
## This does not use std/net's `Socket`/`dial`/`wrapConnectedSocket`. It owns the
## raw socket (std/nativesockets + the platform connect/send/recv), drives the
## TLS handshake and read/write itself (OpenSSL via openssl_ctx), and keeps only
## std/net's `newContext` (the verified TLS context, reached through
## openssl_ctx). `await` is an identity template so the shared engine's
## `await`-shaped body compiles to straight-line blocking code.

import std/[os, strutils, nativesockets, monotimes, times, base64]
import ./api, ./openssl_ctx, ./happyeyeballs
import ../core/response  # for navi's TimeoutError
import ../core/socks
when defined(ssl):
  import std/openssl
when defined(posix):
  from std/posix import Sockaddr_un, TSa_Family

export api

type
  BodySink* = proc(data: string) {.closure, raises: [CatchableError].}
    ## Streaming download sink for the sync backend: receives decoded response
    ## body chunks as they arrive. Synchronous (no backpressure needed: a blocking
    ## read only pulls the next chunk once this returns). `data` is navi's native
    ## body type (`string`, an 8-bit-clean byte buffer), so the engine moves each
    ## chunk in with no copy; write it to a stream/file or index it as bytes.

when defined(windows):
  import std/winlean
  # winlean exports SOL_SOCKET/TCP_NODELAY but not the timeout options; pull them
  # from winsock2.h the same way, so the values come from the platform headers.
  var
    SO_RCVTIMEO {.importc, header: "winsock2.h".}: cint
    SO_SNDTIMEO {.importc, header: "winsock2.h".}: cint
  proc sysConnect(fd: SocketHandle, sa: ptr SockAddr, sl: SockLen): cint =
    winlean.connect(fd, sa, sl)
  proc sysSend(fd: SocketHandle, buf: pointer, n: int): int =
    winlean.send(fd, cast[cstring](buf), cint(n), 0'i32).int
  proc sysRecv(fd: SocketHandle, buf: pointer, n: int): int =
    winlean.recv(fd, cast[cstring](buf), cint(n), 0'i32).int
  proc setNoDelay(fd: SocketHandle) =
    setSockOptInt(fd, nativesockets.IPPROTO_TCP.int, winlean.TCP_NODELAY.int, 1)
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
      ctx: SslContext   ## the (usually shared) SSL_CTX this connection used
      ownsCtx: bool     ## true only for an unshared ctx `close` must destroy
      slot: SessionSlot ## keeps the resumption link alive for the SSL's lifetime

template await*(x: untyped): untyped = x

proc sleep*(ms: int) = os.sleep(ms)

proc tcpConnect(host: string, port: int, connectMs = 0): SocketHandle =
  ## Resolve `host` and connect to the first address that accepts a TCP
  ## connection (IPv4 or IPv6, in the resolver's order). With `connectMs` > 0 the
  ## connect is bounded (non-blocking connect + select); otherwise it blocks (the
  ## OS default). Raises `TimeoutError` on a connect timeout, else `IOError`.
  var ai = getAddrInfo(host, Port(port), AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP)
  defer: freeAddrInfo(ai)   # freed on every exit, including a raise mid-loop
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
        if sysConnect(fd, it.ai_addr, SockLen(it.ai_addrlen)) == 0'i32:
          result = fd; break
        lastErr = osErrorMsg(osLastError()); close(fd)
      else:
        fd.setBlocking(false)
        if sysConnect(fd, it.ai_addr, SockLen(it.ai_addrlen)) == 0'i32:
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

proc proxyConnect(fd: SocketHandle, host: string, port: int, user, pass: string) =
  ## Establish a CONNECT tunnel to `host:port` through an already-connected HTTP
  ## proxy, sending Proxy-Authorization when credentials are supplied.
  let target = host & ":" & $port
  var req = "CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n"
  if user.len > 0 or pass.len > 0:
    req.add("Proxy-Authorization: Basic " & encode(user & ":" & pass) & "\r\n")
  req.add("\r\n")
  sendRaw(fd, req)
  var resp = newString(1024)
  let n = sysRecv(fd, addr resp[0], resp.len)
  resp.setLen(max(n, 0))
  if not resp.startsWith("HTTP/1.1 200") and not resp.startsWith("HTTP/1.0 200"):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

proc recvExactly(fd: SocketHandle, n: int): string =
  ## Read exactly `n` bytes or raise; SOCKS5 replies are fixed-size frames.
  result = newString(n)
  var off = 0
  while off < n:
    let r = sysRecv(fd, addr result[off], n - off)
    if r <= 0: raise newException(IOError, "navi: SOCKS5 proxy closed the connection")
    off += r

proc socksConnect(fd: SocketHandle, host: string, port: int, user, pass: string) =
  ## Perform the SOCKS5 handshake to tunnel to `host:port` through a connected
  ## proxy (RFC 1928 + RFC 1929 user/pass). The target is sent as a domain name so
  ## the proxy resolves DNS.
  let hasAuth = user.len > 0 or pass.len > 0
  sendRaw(fd, greeting(hasAuth))
  case selectedMethod(recvExactly(fd, 2))
  of methodUserPass:
    if not hasAuth:
      raise newException(ValueError, "navi: SOCKS5 proxy requires authentication")
    sendRaw(fd, authRequest(user, pass))
    checkAuthReply(recvExactly(fd, 2))
  of methodNoAuth: discard
  else: raise newException(ValueError, "navi: SOCKS5 proxy rejected the offered auth methods")
  sendRaw(fd, connectRequest(host, port))
  let header = recvExactly(fd, 4)
  let status = replyStatus(header)
  if status != 0: raiseReply(status)
  let tail = boundTailLen(int(uint8(header[3])))   # discard BND.ADDR + BND.PORT
  if tail >= 0: discard recvExactly(fd, tail)
  else:
    let dlen = int(uint8(recvExactly(fd, 1)[0]))
    discard recvExactly(fd, dlen + 2)

# --- Happy Eyeballs (RFC 8305) -----------------------------------------
# `heAttemptDelayMs`, `interleaveFamilies`, and `resolveAddrs` are shared with the
# async backends in ./happyeyeballs; `happyConnect` below is the sync racer.

proc unixConnect(path: string, connectMs = 0): SocketHandle =
  ## Connect a blocking AF_UNIX/SOCK_STREAM socket to `path`, honoring `connectMs`
  ## (non-blocking connect + select) exactly as `tcpConnect` does for TCP.
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
    let fd = createNativeSocket(nativesockets.AF_UNIX, nativesockets.SOCK_STREAM,
                                nativesockets.IPPROTO_IP)  # protocol 0
    if fd == osInvalidSocket:
      raise newException(IOError, "navi: could not create a Unix socket")
    let sap = cast[ptr SockAddr](addr sa)
    let sl = SockLen(sizeof(sa))
    if connectMs <= 0:
      if sysConnect(fd, sap, sl) == 0'i32: return fd
      let e = osErrorMsg(osLastError()); close(fd)
      raise newException(IOError,
        "navi: could not connect to Unix socket " & path & ": " & e)
    fd.setBlocking(false)
    if sysConnect(fd, sap, sl) == 0'i32:
      fd.setBlocking(true); return fd
    elif not connectInProgress():
      let e = osErrorMsg(osLastError()); close(fd)
      raise newException(IOError,
        "navi: could not connect to Unix socket " & path & ": " & e)
    elif not waitWritable(fd, connectMs):
      close(fd)
      raise newException(response.TimeoutError,
        "navi: Unix socket connect timed out after " & $connectMs & " ms")
    elif getSockOptInt(fd, SOL_SOCKET.int, SO_ERROR.int) != 0:
      close(fd)
      raise newException(IOError, "navi: connection refused on Unix socket " & path)
    else:
      fd.setBlocking(true); return fd

proc happyConnect(ips: seq[string], port: int,
                  connectMs = 0): (SocketHandle, int) =
  ## Happy Eyeballs: start non-blocking TCP connects to `ips` in order, staggered
  ## by ~250ms, and return the (fd, index) of the first to complete -- so a slow or
  ## blackholed address does not stall the others. Losing attempts are closed.
  ## `connectMs` > 0 bounds the whole race. Raises `TimeoutError` / `IOError`.
  var
    inflight: seq[tuple[fd: SocketHandle, idx: int]]
    nextIdx = 0
    lastStart: MonoTime
    began = false
    lastErr = "no address"
    timedOut = false
  let start = getMonoTime()

  proc begin() =
    let ip = ips[nextIdx]
    var ai = getAddrInfo(ip, Port(port), AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP)
    defer: freeAddrInfo(ai)   # freed even if setNoDelay/setBlocking raises
    let fd = createNativeSocket(ai.ai_family, ai.ai_socktype, ai.ai_protocol)
    if fd != osInvalidSocket:
      setNoDelay(fd); fd.setBlocking(false)
      if sysConnect(fd, ai.ai_addr, SockLen(ai.ai_addrlen)) == 0'i32 or connectInProgress():
        inflight.add (fd, nextIdx)       # completes now or is in progress
      else:
        lastErr = osErrorMsg(osLastError()); close(fd)
    inc nextIdx
    lastStart = getMonoTime()
    began = true

  while true:
    # Start the next attempt: the first at once; the rest when nothing is in flight
    # or the stagger window has elapsed.
    if nextIdx < ips.len and
       (not began or inflight.len == 0 or
        (getMonoTime() - lastStart).inMilliseconds >= heAttemptDelayMs):
      begin()
      continue
    if inflight.len == 0: break          # nothing pending and nothing left to start
    var waitMs = heAttemptDelayMs
    if connectMs > 0:
      let remaining = connectMs - (getMonoTime() - start).inMilliseconds.int
      if remaining <= 0: timedOut = true; break
      waitMs = min(waitMs, remaining)
    var fds = newSeq[SocketHandle](inflight.len)
    for i, e in inflight: fds[i] = e.fd
    if selectWrite(fds, waitMs) > 0:
      for ready in fds:                  # `fds` now holds only the writable sockets
        var pos = -1
        for i, e in inflight:
          if e.fd == ready: pos = i; break
        if pos < 0: continue
        if getSockOptInt(ready, SOL_SOCKET.int, SO_ERROR.int) == 0:
          ready.setBlocking(true)
          let idx = inflight[pos].idx
          for i, e in inflight:
            if i != pos: close(e.fd)      # cancel the losing attempts
          return (ready, idx)
        else:
          lastErr = "connection refused"; close(ready); inflight.delete(pos)
  for e in inflight: close(e.fd)
  if timedOut:
    raise newException(response.TimeoutError,
                       "navi: connect timed out after " & $connectMs & " ms")
  raise newException(IOError, "navi: could not connect: " & lastErr)

when defined(ssl):
  proc resumeSlot(cfg: TlsConfig, origin: string): SessionSlot =
    ## When resumption is on and the client has a session cache, return a slot keyed
    ## by `origin`; otherwise nil. The context itself is armed once in
    ## `obtainContext`, so this only mints the per-connection link.
    if cfg.wantsResume and not cfg.sessionCache.isNil:
      result = newSlot(cast[TlsSessionCache](cfg.sessionCache), origin)

  proc connectAcross*(ctx: SslContext, ips: openArray[string], sni: string,
                      port: int, verify: bool, slot: SessionSlot = nil,
                      connectMs = 0): Conn =
    ## Happy-Eyeballs TCP connect across `ips` (raced, staggered), then a TLS
    ## handshake on the winner; the connection uses each IP while SNI and
    ## verification use `sni`. On a *handshake* failure the winning address is
    ## dropped and the remaining addresses are re-raced, so a partially-broken CDN
    ## pool still connects. `connectMs` > 0 bounds each race and handshake.
    ## `ctx`/`slot` are set on the returned Conn by the caller. Exported for the
    ## interop tests; navi reaches it through `connect`.
    result.fd = osInvalidSocket
    if ips.len == 0:
      raise newException(IOError, "navi: could not resolve " & sni)
    var pool = @ips
    var lastErr: ref CatchableError
    while pool.len > 0:
      let (fd, idx) = happyConnect(pool, port, connectMs)   # raise = nothing connected
      try:
        if connectMs > 0: setIoTimeout(fd, connectMs)   # bound the blocking handshake
        result.ssl = startClientTls(ctx, fd, sni, verify, slot)
        if connectMs > 0: setIoTimeout(fd, 0)           # clear; reads use selectRead
        result.fd = fd
        result.slot = slot
        result.protocol = negotiatedProtocol(result.ssl)
        return
      except CatchableError as e:
        close(fd); lastErr = e
        pool.delete(idx)   # TLS failed on this address; re-race the rest
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
  # proxy CONNECT or TLS handshake would otherwise leak the fd (and an unshared
  # SSL_CTX, which has no destructor). A shared ctx is owned by the client's store,
  # so it is not freed here.
  var established = false
  defer:
    if not established:
      when defined(ssl):
        if not result.ssl.isNil: SSL_free(result.ssl)
        if result.ownsCtx and not result.ctx.isNil: result.ctx.destroyContext()
      if result.fd != osInvalidSocket:
        close(result.fd)
  if proxy.kind == pkUnix:
    result.fd = unixConnect(proxy.host, establishMs)
    if tls:
      when defined(ssl):
        if establishMs > 0: setIoTimeout(result.fd, establishMs)
        (result.ctx, result.ownsCtx) = obtainContext(cfg.contextStore, cfg, alpn)
        result.slot = resumeSlot(cfg, host & ":" & $port)
        result.ssl = startClientTls(result.ctx, result.fd, host, cfg.wantsVerify,
                                    result.slot)
        if establishMs > 0: setIoTimeout(result.fd, 0)
        result.protocol = negotiatedProtocol(result.ssl)
      else:
        raise newException(ValueError, "navi: https requires compiling with -d:ssl")
  elif proxy.isSet:
    result.fd = tcpConnect(proxy.host, proxy.port, establishMs)
    if establishMs > 0: setIoTimeout(result.fd, establishMs)
    # SOCKS5 tunnels every target; an HTTP proxy tunnels only https (CONNECT) and
    # relays http via an absolute-URI request (no tunnel needed here).
    if proxy.kind == pkSocks5:
      socksConnect(result.fd, host, port, proxy.user, proxy.pass)
    elif tls:
      proxyConnect(result.fd, host, port, proxy.user, proxy.pass)
    if tls:
      when defined(ssl):
        (result.ctx, result.ownsCtx) = obtainContext(cfg.contextStore, cfg, alpn)
        result.slot = resumeSlot(cfg, host & ":" & $port)
        result.ssl = startClientTls(result.ctx, result.fd, host, cfg.wantsVerify,
                                    result.slot)
        result.protocol = negotiatedProtocol(result.ssl)
      else:
        raise newException(ValueError, "navi: https requires compiling with -d:ssl")
    if establishMs > 0: setIoTimeout(result.fd, 0)
  elif tls:
    when defined(ssl):
      let (ctx, owned) = obtainContext(cfg.contextStore, cfg, alpn)
      let slot = resumeSlot(cfg, host & ":" & $port)
      let dl = result.deadline
      let bd = result.bounded
      # Handshake-aware fallback across the resolved addresses (see connectAcross).
      # connectAcross returns a fresh Conn, so `result.ctx` is nil until we set it
      # below; free `ctx` here (only if we own it) on failure, before `result` is
      # assigned (the connect-cleanup defer cannot see it yet).
      try:
        result = connectAcross(ctx, resolveAddrs(host, port), host, port,
                               cfg.wantsVerify, slot, establishMs)
      except CatchableError:
        if owned: ctx.destroyContext()
        raise
      result.ctx = ctx
      result.ownsCtx = owned
      result.readMs = readMs
      result.deadline = dl
      result.bounded = bd
    else:
      raise newException(ValueError, "navi: https requires compiling with -d:ssl")
  else:
    result.fd = happyConnect(resolveAddrs(host, port), port, establishMs)[0]
  when defined(ssl):
    # SPKI pinning + the user's verify callback (no-op unless configured). Runs
    # before `established`, so a rejection cleans up the fd/SSL via the defer.
    if tls: postHandshakeVerify(result.ssl, host, cfg)
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

proc dataWaiting*(c: Conn, ms: int): bool =
  ## True when the socket has data ready within `ms` (the WebSocket keepalive poll
  ## reads only once this says so, so it never blocks past the interval). Also sees
  ## bytes already buffered inside OpenSSL, which a raw `select` would miss.
  c.waitReadable(ms)

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
    # A shared ctx is freed with the client's context store, not here; only an
    # unshared one (bare TlsConfig, e.g. interop tests) is destroyed per connection.
    if c.ownsCtx and not c.ctx.isNil: c.ctx.destroyContext()

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

proc newTlsCtxStore*(cfg: TlsConfig): RootRef =
  ## The per-client shared TLS-context store (empty until the first TLS connect),
  ## or nil on a non-`-d:ssl` build. Entries put it on `config.tls.contextStore`.
  when defined(ssl):
    result = newTlsContextStore()
  else:
    discard cfg

proc closeTlsCtxStore*(store: RootRef) =
  ## Free the shared contexts held by a `newTlsCtxStore`. Entries call this in
  ## `close`, after `closeIdle` has shut the pooled connections.
  when defined(ssl):
    if not store.isNil: close(cast[TlsContextStore](store))
  else:
    discard store
