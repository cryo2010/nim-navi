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
import ./api, ./openssl_ctx, ./happyeyeballs, ./tls_store, ./tunnel, ./timing
import ../core/response  # for navi's TimeoutError
import ../core/socks
when defined(ssl):
  import std/openssl
when defined(posix):
  from std/posix import Sockaddr_un, TSa_Family, EINPROGRESS, EAGAIN,
    EWOULDBLOCK, errno
  proc cConnect(fd: SocketHandle, sa: ptr SockAddr, sl: SockLen): cint
    {.importc: "connect", header: "<sys/socket.h>".}

export api, asyncdispatch, tls_store

type
  BodySink* = proc(data: string): Future[void] {.closure.}
    ## Streaming download sink for the asyncdispatch backend. Awaitable: the engine
    ## and h2 mux `await` it, so a slow sink applies cooperative backpressure (it
    ## stalls the peer via the gated receive window) rather than buffering in memory.
    ## Takes an owned `string` (navi's native body type, an 8-bit-clean byte buffer):
    ## the chunk crosses an `await` so it must be owned, not a borrowed view; being
    ## navi's own body type lets the engine move each chunk in with no copy.

  GatedBodySink* = proc(data: string): Future[bool] {.closure.}
    ## A response sink for `request()` that can stop the download early. Like
    ## `BodySink` it receives decoded body chunks of the FINAL surfaced response and
    ## is awaited (backpressure), but returns `Future[bool]`: `true` keeps the
    ## transfer going, `false` stops it cleanly (the request returns normally with
    ## `res.body == ""` and `res.bodyTruncated == true`). Only the final response's
    ## body is delivered; redirect/retry/digest/thrown-error bodies never reach it.

  AsyncBodyProducer* = proc(): Future[string] {.closure.}
    ## Pull-based upload source for the asyncdispatch backend: returns the next body
    ## chunk (or "" at end of body). The engine `await`s each call, so producing a
    ## chunk may itself await -- e.g. reading from a streaming download to pipe it
    ## into the upload in constant memory. The async analog of the sync
    ## `BodyProducer` (`core/request.nim`); its Future type is backend-specific, so
    ## it lives here rather than on `Request`, and is threaded through the send paths
    ## (mirroring `BodySink`). Not replayable: like `bodyStream`, a request carrying
    ## one is sent once and never retried/redirected/digest-replayed.

# --- bounded waits (issue #468) -----------------------------------------------
#
# std/asyncdispatch has no timer cancellation: `sleepAsync(ms)` pushes its future
# onto the global dispatcher's timer heap (`p.timers`) and nothing can take it off
# again, so until it FIRES everything that future still reaches is reachable --
# live memory, not cyclic garbage, so no `GC_fullCollect` can reclaim it. Two costs
# follow, and `withinMs` addresses both:
#
#   1. What hangs off the timer, which is `or`'s problem specifically. `complete`
#      runs a future's callbacks and then nils the list (asyncfutures' `call`), so
#      the WINNER's side unlinks itself; `or` never unlinks the LOSER's. A spent `or`
#      timer therefore kept the race it lost: timer -> its callback -> `or`'s `cb`
#      closure -> the `or` future, and on through that future's callbacks to the
#      awaiting proc's continuation and env whenever it had NOT itself completed.
#      `guard`'s `await fut or cancelFut or sleepAsync(ms)` core was navi's one such
#      race on the hot path, and it is where the 2243 bytes per request came from.
#      std/asyncdispatch's `withTimeout` does NOT have this half: as of Nim 2.2.10 it
#      clears the loser's callbacks whichever side wins, so the four `withTimeout`
#      bounds converted with it (connect, the two per-read bounds, the h2 GOAWAY
#      grace) retained only the bare entry of (2); for those, the win here is the
#      slicing, not the unlinking. `withinMs` covers both halves: it keeps the timer
#      in a local, races it explicitly, and when `fut` wins clears the timer's
#      callbacks and drops every reference its own closure env holds, so the entry
#      that outlives the wait reaches nothing but itself (see `guard`, which spells
#      out the full chain its old `or` core left behind).
#
#   2. The timer entry ITSELF, which is small but is not nothing (a `Future[void]`,
#      its heap slot, and in a non-release build the stack trace `newFuture`
#      captures: ~0.2 KB release, ~1 KB debug). One per request that lingers for the
#      full `timeouts.total` is, with (1), what made the resident heap grow with
#      rate x totalMs -- 610 MB at 12k req/s with a 60 s total (issue #468) -- so a
#      timeout longer than `naviTimerSliceMs` is armed in slices: each slice's
#      entry expires within a second and re-arms for what is left of a MonoTime
#      deadline (no drift, so the expiry lands at the same instant as before). A
#      finished request's spent slice is therefore reclaimed in at most a second
#      instead of in `totalMs`, and retention stops scaling with the timeout. This
#      is the half that applies to every bound, however it was previously written.
const
  naviTimerSliceMs = 1000
    ## Longest single `sleepAsync` a bounded wait arms; longer bounds are re-armed
    ## in slices (see above). A wait at or under this is armed exactly once, so
    ## every sub-second timeout behaves identically to a plain `withTimeout`.
  naviTimerMaxMs = 2_000_000_000
    ## Clamp on a bound before it becomes a `Duration`: `initDuration(milliseconds
    ## = int.high)` overflows its nanosecond conversion. Also a 32-bit-safe literal
    ## (it fits a 32-bit `int`, so navi still compiles for i386/armv7, where a bound
    ## cannot exceed `int.high` = 2_147_483_647 ms anyway). About 23 days: longer
    ## than any real deadline, and the conversion stays 4 billion-fold short of the
    ## nanosecond edge.

proc withinMs*[T](fut: Future[T]; ms: int;
                  alt: Future[void] = nil): Future[bool] =
  ## Wait for `fut`, giving up after `ms` milliseconds (`ms <= 0`: no timer) or when
  ## `alt` completes (`nil`: no alternative). Completes `true` if `fut` finished
  ## first and `false` if the timer or `alt` won; it never fails, so the caller reads
  ## the value and the error off `fut` itself.
  ##
  ## The loser is NOT cancelled -- asyncdispatch cannot -- so an abandoned `fut`
  ## drains in the background and the caller still has to dispose of it (park it,
  ## retire it, or shut the socket down), exactly as with `withTimeout`. What this
  ## adds over `withTimeout` is the SLICING: a long bound is armed a second at a
  ## time, so no spent timer outlives the exchange by more than `naviTimerSliceMs`
  ## instead of by the whole `ms`. (Unlinking the loser it shares with `withTimeout`,
  ## which has cleared both sides since Nim 2.2.10; what neither `or` nor
  ## `withTimeout` does is empty the shared closure env, which `settle` below does on
  ## either outcome.)
  ##
  ## It also APPENDS its callback instead of replacing `fut`'s callback list, so it
  ## never drops a callback the caller installed -- but for the same reason the same
  ## future must not be raced through it over and over (each lost race leaves one
  ## spent, reference-free callback behind on it); a future that is re-raced per
  ## interval, like `kaRecv`'s parked read, stays on `withTimeout`.
  var res = newFuture[bool]("navi.withinMs")
  result = res
  if fut.finished or (alt != nil and alt.finished):
    res.complete(fut.finished)
    return
  var
    target = fut                      # NOT `fut` itself: the env must be droppable
    timer: Future[void] = nil
    deadline: MonoTime

  proc settle() {.closure, gcsafe.} =
    ## The race is over: unlink the timer and empty this env, so nothing the
    ## dispatcher still holds can reach the exchange.
    if res == nil: return             # already decided
    let won = target.finished
    if timer != nil:
      timer.clearCallbacks()          # its heap entry can no longer reach us
      timer = nil
    target = nil
    let r = res
    res = nil
    r.complete(won)

  proc tick() {.closure, gcsafe.} =
    ## A slice elapsed. Expire the wait if the deadline is up, else re-arm for what
    ## is left; the slice that just fired is spent and collectable either way.
    if res == nil: return
    let left = (deadline - getMonoTime()).inMilliseconds.int
    if left <= 0 or target.finished:
      settle()
      return
    timer = sleepAsync(min(left, naviTimerSliceMs))
    timer.addCallback(tick)

  if ms > 0:
    deadline = getMonoTime() + initDuration(milliseconds = min(ms, naviTimerMaxMs))
    timer = sleepAsync(min(ms, naviTimerSliceMs))
    timer.addCallback(tick)
  fut.addCallback(settle)
  if alt != nil: alt.addCallback(settle)

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
  ConnectionState = enum
    ## Teardown state for a `Conn`, checked by a parked `sslRead` to tell an error
    ## caused by our OWN teardown apart from a genuine peer/protocol fault.
    ## `shutdownConn` flips `csOpen -> csShutdown` before shutting the socket down to
    ## wake a parked read (h2 mux close, keepalive death, GOAWAY grace, connect
    ## timeout); `close`/`closeSync` then set `csClosed` and free the fd. A read that
    ## wakes to a decrypt error (SSL_ERROR_SSL on the truncated record our SHUT_RDWR
    ## leaves) after we initiated teardown must NOT raise: a failed asyncdispatch
    ## future orphans its injected stack trace at process exit (a valgrind leak), and
    ## the error is expected teardown noise, not peer corruption. So sslRead returns a
    ## clean EOF once state is past csOpen, and only raises a genuine decrypt error
    ## seen while the connection is still fully open.
    csOpen
    csShutdown
    csClosed
  ParkedRead = object
    ## One-slot holder for an in-flight read abandoned by an expired `recvWithin`
    ## (see `Conn.parked`). At most one read is ever outstanding per connection, so
    ## a single slot is the whole bookkeeping.
    fut: Future[string]

  Conn* = object
    fd: AsyncFD
    protocol*: string   ## ALPN-negotiated protocol ("h2" or "", meaning http/1.1)
    readMs: int         ## per-read stall timeout in ms; 0 blocks indefinitely
    parked: ref ParkedRead
                        ## the read a `recvWithin` started and abandoned when its bound
                        ## expired. asyncdispatch cannot cancel, so an abandoned read
                        ## stays registered on the socket and WILL consume the bytes the
                        ## next read needs; handing it to the next read instead is what
                        ## makes a bounded read non-destructive here. A `ref` so every
                        ## value copy of the Conn shares the one slot, like `state`.
    state: ref ConnectionState
                        ## shared across value copies: set to csClosed by `close`,
                        ## checked by a parked `sslRead` so closing under an in-flight
                        ## read yields EOF instead of dereferencing the freed SSL (a
                        ## UAF crash). A `ref` so all copies of the Conn value observe
                        ## the same transition.
    when defined(ssl):
      ssl: SslPtr       ## the TLS connection; nil for plain http
      ctx: SslContext   ## the (usually shared) SSL_CTX this connection used
      ownsCtx: bool     ## true only for an unshared ctx `close` must destroy
      slot: SessionSlot ## keeps the resumption link alive for the SSL's lifetime
      uncleanEof: ref bool
                        ## set when the transport ended without a TLS close_notify (a
                        ## RST, a bare FIN, a dead peer). Shared across value copies
                        ## like `state`, because the read that sees the unclean close
                        ## and the engine that has to reject a read-until-close body
                        ## hold different copies of the Conn

var openedConnections* {.threadvar.}: int
  ## diagnostic: TCP connections opened by this backend, counted per thread (one navi
  ## client runs per thread, so a plain global would race; readers are single-threaded)

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
  proc driveHandshake(ssl: SslPtr, fd: AsyncFD, host: string) {.async.} =
    ## Non-blocking SSL_connect, awaiting readiness only when OpenSSL asks.
    while true:
      ErrClearError()   # see `sslRead`: SSL_get_error is only reliable on an empty queue
      let r = SSL_connect(ssl)
      if r == 1: return
      case SSL_get_error(ssl, r)
      of SSL_ERROR_WANT_READ: await waitRead(fd)
      of SSL_ERROR_WANT_WRITE: await waitWrite(fd)
      else: raise newException(ValueError, "navi: TLS handshake failed for " & host)

  proc sslWrite(c: Conn, data: string) {.async.} =
    var off = 0
    while off < data.len:
      # If `close` ran while we were parked, the SSL is already freed and SSL_write
      # would dereference the dangling pointer -- the same UAF `sslRead` guards
      # against, reached from the write side. A parked write survives the teardown
      # on Linux: a peer reset reports {Read, Error} with no EPOLLOUT, so the
      # dispatcher never walks the writeList, and the wake finally arrives from
      # `closeSocket` inside `freeConn`, i.e. after SSL_free. The check is at the
      # top of the loop, so it is re-run after every WANT_READ/WANT_WRITE await as
      # well as on entry. The stream layer treats the raise as a drop.
      if not c.state.isNil and c.state[] == csClosed:
        raise newException(IOError, "navi: connection closed")
      # OpenSSL requires the same buffer+len when retrying after WANT_WRITE; `data`
      # is captured by this async proc, so the pointer stays valid across awaits.
      ErrClearError()   # see `sslRead`: SSL_get_error is only reliable on an empty queue
      let n = SSL_write(c.ssl, cast[cstring](unsafeAddr data[off]),
                        (data.len - off).cint).int
      if n > 0:
        off += n
      else:
        case SSL_get_error(c.ssl, n.cint)
        of SSL_ERROR_WANT_READ: await waitRead(c.fd)
        of SSL_ERROR_WANT_WRITE: await waitWrite(c.fd)
        else: raise newException(IOError, "navi: SSL_write failed")

  proc markUncleanEof(c: Conn) {.inline.} =
    ## Remember that the TLS stream ended without a close_notify. Writes through the
    ## shared cell, so the flag survives the Conn being passed by value.
    if not c.uncleanEof.isNil: c.uncleanEof[] = true

  proc sslRead(c: Conn): Future[string] {.async.} =
    ## One chunk of up to `naviReadBufSize` bytes; "" means the peer closed.
    result = newStringUninit(naviReadBufSize)   # overwritten by SSL_read then setLen: no zero-fill
    while true:
      # If `close` ran while we were parked on waitRead, the SSL is already freed.
      # Raise rather than reading through the dangling pointer (a UAF crash) -- and
      # rather than returning "" (a clean peer-EOF), which the h1 body reader would
      # take as "read more" on an unfinished stream and spin. The stream layer
      # treats this as a drop.
      if not c.state.isNil and c.state[] == csClosed:
        raise newException(IOError, "navi: connection closed")
      # OpenSSL's error queue is per THREAD, not per SSL, and `SSL_get_error` is
      # documented to be reliable only when that queue was empty before the I/O
      # call: a stale entry left by ANY earlier OpenSSL call makes it report
      # SSL_ERROR_SSL for what is really a would-block. One event loop drives every
      # connection here, and a teardown leaves entries behind -- the decrypt error
      # on the truncated final record after `shutdownConn`'s SHUT_RDWR (swallowed as
      # teardown noise below), and `SSL_shutdown` in `freeConn`. Without this clear,
      # closing ONE connection made the next read on every other live connection
      # raise "TLS read failed": a WebSocket soak lost 7 of 8 healthy h2 muxes the
      # moment the first socket closed. Clear the queue before every SSL_* call.
      ErrClearError()
      let n = SSL_read(c.ssl, addr result[0], result.len.cint).int
      if n > 0:
        result.setLen(n); return
      case SSL_get_error(c.ssl, n.cint)
      of SSL_ERROR_WANT_READ: await waitRead(c.fd)
      of SSL_ERROR_WANT_WRITE: await waitWrite(c.fd)
      of SSL_ERROR_ZERO_RETURN:
        result.setLen(0); return       # peer sent close_notify: clean EOF
      of SSL_ERROR_SYSCALL:
        # OpenSSL read the fd directly (SSL_set_fd), so a non-blocking socket that
        # would block surfaces here as SSL_ERROR_SYSCALL with errno EAGAIN/EWOULDBLOCK
        # -- NOT a close. This races under load: the readable event that woke
        # waitRead can be drained by another connection's callback before this
        # SSL_read runs. Treating it as EOF truncates a mid-body response (the
        # `response truncated` failure the chaos soak surfaced). It is recoverable:
        # wait for readability and retry, exactly like WANT_READ. Only a genuine
        # transport EOF (n == 0, or any other errno) is a real close. Chronos never
        # hits this because it drives OpenSSL over memory BIOs (no fd for OpenSSL to
        # EAGAIN on), which is why only the asyncdispatch backend was affected.
        #
        # A genuine EOF here carries no close_notify, so it is recorded before it is
        # reported: the engine still gets "" (an EOF before any response must keep
        # being classified as a keep-alive race and replayed), but it can now refuse
        # a read-until-close body that ends on it (issue #426).
        when defined(posix):
          if n < 0 and (errno == EAGAIN or errno == EWOULDBLOCK):
            await waitRead(c.fd)
          else:
            c.markUncleanEof()
            result.setLen(0); return
        else:
          c.markUncleanEof()
          result.setLen(0); return
      of SSL_ERROR_SSL:
        # A decrypt/protocol error. When WE initiated teardown (shutdownConn flipped
        # the state past csOpen and SHUT_RDWR the socket under this parked read), the
        # next SSL_read decrypts the truncated final record and reports SSL_ERROR_SSL:
        # expected teardown noise, not peer corruption. Return a clean EOF -- raising
        # here fails the read future with an injected stack trace that the h2 reader's
        # teardown path then leaves orphaned at process exit (a definite valgrind leak,
        # 21 blocks in the streamdown/up/sse cells). A genuine mid-stream decrypt error
        # on a still-open connection (state == csOpen) is still surfaced as before.
        if not c.state.isNil and c.state[] != csOpen:
          result.setLen(0); return
        raise newException(IOError, "navi: TLS read failed")
      else:
        c.markUncleanEof()
        result.setLen(0); return   # any other code -> treat as EOF, unauthenticated

# Byte-I/O primitives the shared tunnel drivers (backend/tunnel) mix in.
proc sockWrite(fd: AsyncFD, s: string): Future[void] = send(fd, s)

proc sockReadExactly(fd: AsyncFD, n: int): Future[string] {.async.} =
  ## Read exactly `n` bytes or raise; SOCKS5 replies are fixed-size frames.
  result = ""
  while result.len < n:
    let chunk = await recv(fd, n - result.len)
    if chunk.len == 0: raise newException(IOError, "navi: SOCKS5 proxy closed the connection")
    result.add chunk

proc sockReadSome(fd: AsyncFD, max: int): Future[string] = recv(fd, max)
  ## One read of up to `max` bytes; returns "" at EOF. The CONNECT driver loops
  ## on this until the reply head is terminated, so a short read is expected.

proc proxyConnect(fd: AsyncFD, host: string, port: int, user, pass: string) {.async.} =
  proxyConnectDriver(fd, host, port, user, pass)

proc socksConnect(fd: AsyncFD, host: string, port: int, user, pass: string) {.async.} =
  socksConnectDriver(fd, host, port, user, pass)

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
  ## (`withinMs` on `establish`). asyncdispatch has no cancellation, so a loser's
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

proc closeSync*(c: Conn)      # forward decls: connect's timeout cleanup wakes + reclaims
proc shutdownConn*(c: Conn)   # an abandoned establish via these (both defined below).

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget, alpn: seq[string] = @[],
              connectMs = 0, readMs = 0, totalMs = 0): Future[Conn] {.async.} =
  ## Dial `host:port` (or the proxy), upgrading to TLS for https with a CONNECT
  ## tunnel when proxied. The handshake completes here so the ALPN result (h2 vs
  ## http/1.1) is known before any request. `connectMs` bounds establishment (TCP
  ## + TLS); `readMs` is stored for per-read timeouts. `totalMs` is enforced by the
  ## async entry's `guard`, so it is accepted for signature parity with the other
  ## backends but unused here. TLS requires `-d:ssl`.
  discard totalMs
  inc openedConnections
  var conn: Conn
  conn.fd = invalidFd
  conn.readMs = readMs
  conn.state = new(ConnectionState)   # csOpen; shared teardown state (see Conn.state)
  conn.parked = new(ParkedRead)       # empty; filled only by an expired `recvWithin`
  when defined(ssl):
    if tls: conn.uncleanEof = new(bool)   # close_notify verdict (see closedCleanly)
  # Set by the connectMs path below when it gives up on `establish` and raises
  # TimeoutError. asyncdispatch cannot cancel, so this cell is the only way to tell
  # the still-running `establish` that its result has no owner any more (issue #443).
  # A `ref` rather than a plain local so the value is shared no matter how the async
  # transform captures the two closures, and an explicit flag rather than
  # `conn.fd`/`conn.state` because the deadline can fire while `happyConnect` is still
  # racing, when there is no fd to shut down and nothing else records the giving up.
  let abandoned = new(bool)

  proc abandonedNow(): bool =
    ## Whether the caller has already stopped waiting for this connect. The flag
    ## covers the timeout, and `conn.state` covers a teardown that flipped the shared
    ## state off csOpen (`shutdownConn` on the timeout path does both). `establish`
    ## checks this wherever it would otherwise start NEW work -- another
    ## Happy-Eyeballs race, another TLS handshake -- so an abandoned connect cannot
    ## keep hitting the origin behind the caller's back.
    abandoned[] or (not conn.state.isNil and conn.state[] != csOpen)

  proc abandonedErr(): ref response.TimeoutError =
    ## The error the abandoned `establish` fails with. Nobody awaits that future (the
    ## caller is already unwinding on its own TimeoutError and the backstop callback
    ## just observes the outcome), so this only ever shows up in a debugger; it is the
    ## caller's error type so the message stays truthful if it ever does surface.
    newException(response.TimeoutError, connectTimeoutMsg(connectMs))

  proc tearDownAttempt(fd: AsyncFD) =
    ## Release a half-built connection. Nothing here has a destructor and a
    ## value-type `Conn` is simply dropped when `establish` raises, so a failed
    ## handshake or verification must hand back the SSL, an UNSHARED SSL_CTX (a
    ## shared one belongs to the client's context store) and the socket itself, or
    ## every retry leaks one of each. Used by both branches of `establish`.
    when defined(ssl):
      if not conn.ssl.isNil: SSL_free(conn.ssl); conn.ssl = nil
      if conn.ownsCtx and not conn.ctx.isNil: conn.ctx.destroyContext()
      conn.ctx = nil
      conn.ownsCtx = false
    if fd != invalidFd: closeSocket(fd)
    conn.fd = invalidFd

  proc establish() {.async.} =
    if proxy.kind == pkUnix:
      conn.fd = await unixConnect(proxy.host)
      # The deadline can fire while the Unix connect is in flight, i.e. while there is
      # no fd for `shutdownConn` to wake. Nothing owns this socket any more, so hand it
      # back instead of running a TLS handshake (SSL_CTX, mTLS, pin checks) on it.
      if abandonedNow():
        tearDownAttempt(conn.fd)
        raise abandonedErr()
      if tls:
        try:
          when defined(ssl):
            (conn.ctx, conn.ownsCtx) = obtainContext(cfg.contextStore, cfg, alpn)
            conn.slot = resumeSlot(cfg, host & ":" & $port)
            conn.ssl = newClientSsl(conn.ctx, conn.fd.SocketHandle, host,
                                    cfg.wantsVerify, conn.slot)
            await driveHandshake(conn.ssl, conn.fd, host)
            verifyPeer(conn.ssl, host, cfg.wantsVerify, conn.slot)
            postHandshakeVerify(conn.ssl, host, cfg, conn.slot)
            conn.protocol = negotiatedProtocol(conn.ssl)
          else:
            raise newException(ValueError, "navi: https requires compiling with -d:ssl")
        except CatchableError:
          # Unlike the TCP branch there is no second address to fall back to, so
          # reclaim the attempt and let the failure propagate. The caller is
          # unwinding on it and cannot reach the conn, which is why this has to
          # happen here (the connectMs backstop below only covers a TIMED-OUT
          # establish, not a failed one).
          tearDownAttempt(conn.fd)
          raise
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
      # Never start another attempt for a caller that has already given up: a
      # connectMs that fired mid-handshake would otherwise land here via the `except`
      # below and run a whole new TCP race plus a new TLS handshake (SSL_CTX, mTLS,
      # SPKI pin) against the origin, once per remaining address, after the caller
      # raised TimeoutError and moved on (issue #443).
      if abandonedNow(): raise abandonedErr()
      let (fd, idx) = await happyConnect(pool, dialPort)
      # The race itself is unbounded from here, so the deadline may well have fired
      # while it ran -- with `conn.fd` still invalidFd, so `shutdownConn` had no socket
      # to shut down and this freshly won one would otherwise proceed through the full
      # handshake unowned. Close it and stop.
      if abandonedNow():
        closeSocket(fd)
        conn.fd = invalidFd
        raise abandonedErr()
      conn.fd = fd
      try:
        # SOCKS5 tunnels every target; an HTTP proxy tunnels only https (CONNECT).
        if proxy.kind == pkSocks5:
          await socksConnect(fd, host, port, proxy.user, proxy.pass)
        elif proxy.isSet and tls:
          await proxyConnect(fd, host, port, proxy.user, proxy.pass)
        # A tunnel that completed just as the deadline fired must not be followed by a
        # fresh handshake either; `tearDownAttempt` in the `except` closes the fd.
        if abandonedNow(): raise abandonedErr()
        if tls:
          when defined(ssl):
            # A CONNECT tunnel (HTTP proxy) and a SOCKS5 tunnel are both transparent
            # once established: the TLS handshake runs end-to-end to the origin, so
            # ALPN belongs on it -- offer the normal list to reach h2 (parity with the
            # sync and chronos backends).
            (conn.ctx, conn.ownsCtx) = obtainContext(cfg.contextStore, cfg, alpn)
            conn.slot = resumeSlot(cfg, host & ":" & $port)
            conn.ssl = newClientSsl(conn.ctx, fd.SocketHandle, host,
                                    cfg.wantsVerify, conn.slot)
            await driveHandshake(conn.ssl, fd, host)
            # The slot makes a rejection evict this peer's cached session (#440).
            verifyPeer(conn.ssl, host, cfg.wantsVerify, conn.slot)
            postHandshakeVerify(conn.ssl, host, cfg, conn.slot)  # SPKI pin + callback
            conn.protocol = negotiatedProtocol(conn.ssl)
          else:
            raise newException(ValueError, "navi: https requires compiling with -d:ssl")
        return                                   # established
      except CatchableError as e:
        # Tear down this attempt (the SSL_CTX has no destructor, so a failed
        # handshake would leak an unshared one), then try the remaining addresses.
        tearDownAttempt(fd)
        pool.delete(idx)
        lastErr = e
    raise lastErr

  # On a connect timeout the establish future is abandoned (asyncdispatch has no
  # cancellation): it keeps running in the background. If it later SUCCEEDS, conn.fd
  # is a live socket (and conn.ssl a live TLS session) with no owner -- the caller
  # is unwinding on the TimeoutError, and a value-type Conn has no destructor, so
  # nothing else will ever close it and the fd leaks. This bites hard when a chaos
  # flood (h2 headerbomb) starves the event loop enough to push otherwise-fast
  # loopback connects past connectMs: every such connect leaks a socket, which the
  # chaos FD assertion catches. Reclaim the conn when the abandoned establish
  # settles; a failed establish already closed its own fd, so closeSync then no-ops.
  let estFut = establish()
  if connectMs > 0 and not await withinMs(estFut, connectMs):
    # The establish future is abandoned here (asyncdispatch has no cancellation). It
    # is typically parked in the TLS handshake against a peer too busy to answer -- an
    # h2 headerbomb flood can starve even loopback handshakes past connectMs -- and if
    # left alone it never completes, pinning its socket for the whole process: a
    # value-type Conn has no destructor to reclaim it, so the fd leaks (the chaos FD
    # assertion catches exactly this). Shut the socket down so the parked handshake
    # errors out and runs establish's own teardown (which closes the fd); the callback
    # is the backstop for the race where establish instead SUCCEEDS right at the
    # deadline, leaving a fully-built conn with no owner.
    #
    # The flag goes first and is what actually stops the orphan: `shutdownConn` only
    # wakes an attempt we already hold an fd for, and does nothing at all when the
    # deadline lands while `happyConnect` is racing. With the flag set, `establish`
    # stops at its next checkpoint instead of walking the remaining addresses with a
    # full TCP race and TLS handshake each (issue #443).
    abandoned[] = true
    shutdownConn(conn)
    estFut.addCallback(proc() {.gcsafe.} =
      {.cast(gcsafe).}:
        # The orphan now fails by design, and an asyncdispatch future that fails with
        # nobody reading its error orphans the injected stack trace at process exit (a
        # valgrind-visible leak, the same reason `retireRead` exists), so observe it
        # here. `closeSync` is idempotent on `state`, so this reclaims the conn exactly
        # once however establish ended.
        if estFut.failed: discard estFut.error
        closeSync(conn))
    raise newException(response.TimeoutError, connectTimeoutMsg(connectMs))
  await estFut
  return conn

proc sendAll*(c: Conn, data: string): Future[void] {.async.} =
  when defined(ssl):
    if not c.ssl.isNil:
      await sslWrite(c, data); return
  # A conn with no TLS session and no valid fd is closed/half-established (e.g. a
  # connection whose ALPN never resolved under event-loop starvation, mis-routed
  # onto the h1 path). Raise a typed transport error instead of writing to a dead
  # fd, so the h1 write-time classifier tears it down and retries on a fresh conn.
  # `freeConn` closes the fd without clearing the value copies' `fd` field, so a
  # send that raced a close still holds a live-looking descriptor number that the
  # process may already have handed to something else: the shared state flag, not
  # the number, is the authority (the plaintext twin of the `sslWrite` guard).
  if c.fd == invalidFd or (not c.state.isNil and c.state[] == csClosed):
    raise newException(IOError, "navi: send on a closed connection")
  await send(c.fd, data)

proc rearm*(c: var Conn, readMs = 0, totalMs = 0) =
  ## Re-apply the current config's read timeout to a connection taken from the idle
  ## pool, so a reused connection honors navi's live-config contract rather than the
  ## value it was opened with (issue #360). `totalMs` is accepted for signature parity
  ## with the sync backend but ignored: the async entry's outer `guard` enforces the
  ## whole-request deadline, so there is no per-conn deadline to re-arm here.
  discard totalMs
  c.readMs = readMs

proc closedCleanly*(c: Conn): bool =
  ## Whether the end of this connection was authenticated. True until a TLS read
  ## sees the transport die without a close_notify, and always true for plain http,
  ## which has no close_notify to look for. The engine consults it before accepting
  ## a body that is delimited by the close itself (issue #426).
  when defined(ssl):
    if not c.uncleanEof.isNil: return not c.uncleanEof[]
  true

proc startRead(c: Conn): Future[string] =
  ## Begin one read, resuming the one a previous `recvWithin` parked rather than
  ## starting a second read on the same socket (which would race it for the bytes).
  if not c.parked.isNil and not c.parked.fut.isNil:
    result = c.parked.fut
    c.parked.fut = nil
    return
  when defined(ssl):
    if not c.ssl.isNil: return sslRead(c)
  recv(c.fd, naviReadBufSize)

proc retireRead(fut: Future[string]) =
  ## Observe an abandoned read's outcome from a callback and drop it. Nobody will
  ## await it, and an asyncdispatch future that fails unobserved orphans its
  ## injected stack trace at process exit (a valgrind-visible leak).
  if fut.isNil: return
  fut.addCallback(proc() {.gcsafe.} =
    if fut.failed: discard fut.error)

proc recvSome*(c: Conn): Future[string] {.async.} =
  ## One chunk of up to `naviReadBufSize` bytes; "" means the peer closed. Bounded by
  ## `readMs` (the per-read stall timeout) when set; on expiry the pending read is
  ## abandoned and TimeoutError is raised (terminal: the caller tears the connection
  ## down). A read parked by an earlier `recvWithin` is resumed here, so its bytes are
  ## delivered to this caller instead of being lost.
  let readFut = c.startRead()
  if c.readMs > 0 and not await withinMs(readFut, c.readMs):
    raise newException(response.TimeoutError, readTimeoutMsg(c.readMs))
  return await readFut

proc recvWithin*(c: Conn, ms: int): Future[tuple[timedOut: bool, data: string]] {.async.} =
  ## A bounded read that leaves the connection usable when it expires: waits at most
  ## `ms` for a chunk, otherwise reports `timedOut` with the read PARKED rather than
  ## dropped. The `Expect: 100-continue` gate uses it to wait for the interim response
  ## and then keep reading the same connection, which a plain `recvSome` + read
  ## timeout cannot do (that one is terminal).
  ##
  ## asyncdispatch has no cancellation, so the abandoned `recvSome`/`sslRead` stays
  ## parked on the socket and would swallow the bytes the real response read then
  ## needs. Parking it in `Conn.parked` hands those bytes to the next read instead.
  if ms <= 0: return (true, "")
  let readFut = c.startRead()
  if not await withinMs(readFut, ms):
    # A `close` that landed while we were waiting has already run `discardParked`,
    # so parking here would leave the read unowned on a freed connection and its
    # failure unobserved (the orphaned-stack-trace leak). Retire it instead.
    if c.parked.isNil or (not c.state.isNil and c.state[] == csClosed):
      retireRead(readFut)
    else:
      c.parked.fut = readFut
    return (true, "")
  return (false, await readFut)

proc discardParked(c: Conn) =
  ## Retire a parked read on teardown. Nobody will await it, and an asyncdispatch
  ## future that fails unobserved orphans its injected stack trace at process exit
  ## (a valgrind-visible leak), so observe the outcome from a callback and drop it.
  if c.parked.isNil or c.parked.fut.isNil: return
  let fut = c.parked.fut
  c.parked.fut = nil
  retireRead(fut)

proc shutdownFd(c: Conn) =
  ## The raw SHUT_RDWR, with no state check: `close` flags `csClosed` before it
  ## shuts the socket down to wake a parked read, so it cannot go through the
  ## guarded `shutdownConn` below.
  if c.fd == invalidFd: return
  when defined(windows):
    discard winlean.shutdown(c.fd.SocketHandle, 2)          # SD_BOTH
  else:
    discard posix.shutdown(c.fd.SocketHandle, posix.SHUT_RDWR)

proc shutdownConn*(c: Conn) =
  ## Shut the socket down in both directions so a pending read or write unblocks
  ## with EOF/error. Used to wake the h2 mux's background reader on client close so
  ## it exits its loop (and does the real close itself) instead of being left
  ## suspended on a closed fd, which would crash at process teardown. Does not free
  ## anything; `close`/`closeSync` still run afterward. Flags `csShutdown` first so a
  ## parked sslRead woken by the shutdown treats the decrypt error on our truncated
  ## final record as clean EOF (teardown noise) rather than raising a failed future
  ## whose stack trace would be orphaned at exit; the SSL itself is still valid here.
  ##
  ## A no-op once the connection is closed: `freeConn` does not clear the value
  ## copies' `fd`, so shutting down after it ran would hit whatever descriptor the
  ## process was handed next. The h2 mux drives this from several teardown paths,
  ## which can land after `close` has already freed the conn.
  if not c.state.isNil and c.state[] == csClosed: return
  if not c.state.isNil and c.state[] == csOpen: c.state[] = csShutdown
  c.shutdownFd()

proc freeConn(c: Conn) =
  ## The raw teardown: free the SSL and close the fd. Callers set/guard the
  ## `state` flag first.
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
  if not c.state.isNil:
    if c.state[] == csClosed: return    # idempotent; stops a double-free
    c.state[] = csClosed
  c.discardParked()
  freeConn(c)

proc close*(c: Conn): Future[void] {.async.} =
  ## Close, safe to call while a `sslRead` is parked (e.g. stopping an SSE stream):
  ## flag the teardown, shut the socket down to wake the parked read, then yield one
  ## tick so the dispatcher delivers that wake (the read observes EOF via the flag)
  ## before we free the fd. Freeing in the same atomic step would lose the wake and
  ## hang the reader on an unregistered fd.
  if not c.state.isNil:
    if c.state[] == csClosed: return
    c.state[] = csClosed
    c.shutdownFd()          # state is already csClosed, so shutdownConn would no-op
    await sleepAsync(0)
  c.discardParked()
  freeConn(c)

proc sleep*(ms: int): Future[void] = sleepAsync(ms)
