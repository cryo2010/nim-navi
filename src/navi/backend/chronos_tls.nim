## OpenSSL TLS over a chronos `StreamTransport` (memory-BIO pump).
##
## chronos's bundled TLS is BearSSL, which has no TLS 1.3, no client ALPN, and no
## client certificates. To bring the chronos backend to parity with navi's other
## OpenSSL backends we drive an OpenSSL `SSL` through a pair of memory BIOs and
## move the ciphertext over the chronos transport ourselves: the SSL never touches
## a socket. All record framing (handshake and application data) is OpenSSL's; we
## only shuttle bytes between its write-BIO/read-BIO and the transport.
##
## Compiled only with `-d:ssl` (it links OpenSSL, exactly as the sync and
## asyncdispatch backends do). The context, ALPN, mTLS credential, version bounds,
## ciphers, and session resumption all come from `openssl_ctx`; this file is only
## the async pump.

when defined(ssl):
  import pkg/chronos, pkg/chronos/transports/stream
  import std/openssl
  import ./openssl_ctx

  export openssl_ctx

  type
    ChronosTls* = ref object
      transport: StreamTransport
      sslp: SslPtr
      rbio, wbio: BIO       ## owned by `sslp` (freed by SSL_free); handles for pumping
      slot: SessionSlot     ## resumption slot, retained for the ssl's lifetime: its
                            ## address lives in the SSL's ex_data, and the new-session
                            ## callback (TLS 1.3 tickets arrive post-handshake, during
                            ## reads) dereferences it -- so it must outlive the ssl
      writeLock: AsyncLock  ## serialize SSL_write + wbio drains so concurrent streams
                            ## (and post-handshake output) never interleave on the wire
      inBuf: string         ## reusable ciphertext scratch for the READ path's feedIn,
                            ## allocated once per connection instead of per read
      wrInBuf: string       ## the same for the WRITE path's feedIn, which is not
                            ## mutually exclusive with the read path (see FeedSide).
                            ## Allocated lazily: only a renegotiation/key update
                            ## during a write ever needs it

  const tlsBufSize = 65536   # drain multiple TLS records per read (see naviReadBufSize)

  type FeedSide = enum
    ## Which pump is reading ciphertext, and so which scratch buffer it owns.
    ##
    ## `feedIn` is reachable from two paths that are NOT mutually exclusive: the read
    ## path (`handshake`, then `readSome`, which on an h2 connection is a background
    ## mux reader parked for as long as the connection is idle) and the write path's
    ## SSL_ERROR_WANT_READ branch, which holds `writeLock` but no read lock. A TLS 1.3
    ## key update or a renegotiation can therefore start a second `readOnce` while the
    ## first is still parked. The two must never target the same buffer: whichever
    ## completed second would overwrite bytes the first had not yet handed to the
    ## read-BIO, and the corrupted ciphertext would fail the connection. One buffer per
    ## side is the invariant that keeps every in-flight `readOnce` on its own memory.
    fsRead
    fsWrite

  proc sslPtr*(t: ChronosTls): SslPtr = t.sslp
    ## The underlying SSL, for `negotiatedProtocol` / `verifyPeer` after handshake.

  proc newChronosTls*(transport: StreamTransport, ctx: SslContext, host: string,
                      slot: SessionSlot = nil): ChronosTls =
    ## Build a client TLS pump over `transport` using the shared `ctx` (ALPN,
    ## versions, ciphers, client cert already wired). SNI and any cached session
    ## are set here; the caller then `await`s `handshake`.
    let (ssl, rbio, wbio) = newClientSslMem(ctx, host, slot)
    ChronosTls(transport: transport, sslp: ssl, rbio: rbio, wbio: wbio,
               slot: slot, writeLock: newAsyncLock(),
               inBuf: newStringUninit(tlsBufSize))   # overwritten by every readOnce

  proc drainOut(t: ChronosTls) {.async.} =
    ## Push any ciphertext OpenSSL queued in the write-BIO onto the transport.
    ## Caller holds `writeLock`.
    while true:
      let pending = bioCtrlPending(t.wbio)
      if pending <= 0: break
      var buf = newStringUninit(pending)   # bioRead fills it, then setLen(n): no zero-fill
      let n = bioRead(t.wbio, cast[cstring](addr buf[0]), pending.cint)
      if n <= 0: break
      buf.setLen(n)
      discard await t.transport.write(buf)

  proc flushOut(t: ChronosTls) {.async.} =
    await t.writeLock.acquire()
    try: await t.drainOut()
    finally: t.writeLock.release()

  proc feedIn(t: ChronosTls, side: FeedSide): Future[bool] {.async.} =
    ## Read one chunk of ciphertext from the transport into the read-BIO. Returns
    ## false on EOF -- a clean peer close, or the transport being closed under us
    ## during teardown. Swallowing the closed/errored-transport exception (rather
    ## than letting it propagate up through readSome/recvSome) lets chronos retire
    ## the in-flight read cleanly instead of leaking its future + our stack trace.
    ##
    ## The ciphertext lands in one of the connection's own scratch buffers rather than
    ## a fresh 64 KiB string per read: the bytes are copied straight into the read-BIO
    ## and the buffer is never handed to a caller, so reuse cannot alias anything.
    ## `side` picks the buffer, and the two sides must stay separate: see FeedSide.
    if side == fsWrite and t.wrInBuf.len == 0:
      t.wrInBuf = newStringUninit(tlsBufSize)     # first renegotiation on this conn
    let buf = if side == fsWrite: addr t.wrInBuf else: addr t.inBuf
    var n = 0
    try:
      n = await t.transport.readOnce(addr buf[][0], buf[].len)
    except CatchableError:
      return false
    if n <= 0: return false
    discard bioWrite(t.rbio, cast[cstring](addr buf[][0]), n.cint)
    return true

  proc handshake*(t: ChronosTls) {.async.} =
    ## Drive the TLS handshake to completion. Raises on failure (a truncated
    ## exchange or a protocol/verification error); the caller runs `verifyPeer`
    ## afterwards. Wrap the whole call in a timeout at the connect site.
    while true:
      ErrClearError()   # see `readSome`: SSL_get_error needs an empty error queue
      let rc = sslDoHandshake(t.sslp)
      if rc == 1:
        await t.flushOut()          # e.g. the client's final Finished
        return
      let err = SSL_get_error(t.sslp, rc)
      case err
      of SSL_ERROR_WANT_READ:
        await t.flushOut()          # send what we have (ClientHello) first
        if not await t.feedIn(fsRead):
          raise newException(IOError, "navi: TLS peer closed during handshake")
      of SSL_ERROR_WANT_WRITE:
        await t.flushOut()
      else:
        raise newException(IOError, "navi: TLS handshake failed")

  proc write*(t: ChronosTls, data: string) {.async.} =
    ## Encrypt and send `data`. Serialized so concurrent h2 streams (and the mux
    ## reader's control frames) never interleave records.
    if data.len == 0: return
    await t.writeLock.acquire()
    try:
      var off = 0
      while off < data.len:
        ErrClearError()   # see `readSome`: SSL_get_error needs an empty error queue
        let n = SSL_write(t.sslp, cast[cstring](addr data[off]), data.len - off)
        if n > 0:
          off += n
          await t.drainOut()
        else:
          let err = SSL_get_error(t.sslp, n)
          case err
          of SSL_ERROR_WANT_WRITE:
            await t.drainOut()
          of SSL_ERROR_WANT_READ:            # renegotiation / key update wants input
            await t.drainOut()
            # fsWrite: a read-path readOnce may be parked right now (see FeedSide).
            if not await t.feedIn(fsWrite):
              raise newException(IOError, "navi: TLS closed during write")
          else:
            raise newException(IOError, "navi: TLS write failed")
    finally:
      t.writeLock.release()

  proc readSome*(t: ChronosTls): Future[string] {.async.} =
    ## Decrypt and return one chunk of application data, or "" on a clean close
    ## (close_notify or peer EOF). Raises on a genuine protocol error.
    # Not the shared `inBuf`: this buffer becomes the caller's chunk, so it must be
    # its own allocation. Uninitialized, though -- SSL_read overwrites what it uses
    # and `setLen` drops the rest, so the 64 KiB zero-fill was pure waste per read.
    var buf = newStringUninit(tlsBufSize)
    while true:
      # OpenSSL's error queue is per THREAD, not per SSL, and `SSL_get_error` is
      # documented to be reliable only when that queue was empty before the I/O call:
      # one event loop drives every connection, so a stale entry from another
      # connection's teardown would otherwise report SSL_ERROR_SSL for what is really
      # a would-block and kill a healthy read. Clear it before every SSL_* call.
      ErrClearError()
      let n = SSL_read(t.sslp, addr buf[0], buf.len)
      if n > 0:
        buf.setLen(n)
        return buf
      let err = SSL_get_error(t.sslp, n)
      case err
      of SSL_ERROR_WANT_READ:
        await t.flushOut()                   # rare post-handshake output first
        if not await t.feedIn(fsRead): return ""  # peer closed: EOF for the parser
      of SSL_ERROR_WANT_WRITE:
        await t.flushOut()
      of SSL_ERROR_ZERO_RETURN:
        return ""                            # peer sent close_notify
      of SSL_ERROR_SSL:
        raise newException(IOError, "navi: TLS read failed")
      else:
        return ""                            # SYSCALL/unexpected EOF: EOF for the parser

  proc close*(t: ChronosTls) {.async.} =
    ## Close the transport, then free the SSL (which frees its BIOs). Idempotent.
    ## Order matters: closing the transport first lets a background reader parked
    ## in `readOnce` complete with a clean EOF and unwind, rather than racing a
    ## freed SSL; it also avoids leaking the reader's in-flight read future.
    # FIN before closesocket, so the close_notify (and any last record) written just
    # before this is delivered rather than dropped with the socket (see
    # `gracefulShutdown` in chronos.nim for the Windows failure this prevents).
    if not t.transport.isNil:
      try:
        discard await withTimeout(t.transport.shutdownWait(), 1000.milliseconds)
      except CatchableError: discard
    try: await t.transport.closeWait()
    except CatchableError: discard
    if not t.sslp.isNil:
      SSL_free(t.sslp); t.sslp = nil

  proc closeSync*(t: ChronosTls) =
    ## Non-awaiting teardown for a GC-reclaimed handle: free the SSL and initiate
    ## transport close (the loop frees it afterwards).
    if not t.sslp.isNil:
      SSL_free(t.sslp); t.sslp = nil
    if not t.transport.isNil: t.transport.close()

  proc shutdownTransport*(t: ChronosTls) =
    ## Initiate transport close without awaiting, to unblock a reader parked on a
    ## pending read (used by the h2 mux's `close`). The reader then observes EOF.
    if not t.transport.isNil: t.transport.close()
