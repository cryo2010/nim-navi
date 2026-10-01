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
      writers: int          ## tasks inside `write` (holding `writeLock` or queued for
                            ## it). The read path reads this instead of ever waiting on
                            ## the lock: see `tryFlushOut`
      inBuf: string         ## reusable ciphertext scratch for `feedIn`, allocated once
                            ## per connection instead of per read. One buffer is enough
                            ## because only the read path ever reads the transport
      uncleanEof: bool      ## the transport ended before OpenSSL reported a
                            ## close_notify, so the end of the byte stream is not
                            ## authenticated (see `closedCleanly`)

  const tlsBufSize = 65536   # drain multiple TLS records per read (see naviReadBufSize)
  const closeNotifyMs = 1000
    ## upper bound on pushing the close_notify alert out in `close`, so a peer that
    ## has stopped reading cannot stall a teardown

  proc sslPtr*(t: ChronosTls): SslPtr = t.sslp
    ## The underlying SSL, for `negotiatedProtocol` / `verifyPeer` after handshake.

  proc closedCleanly*(t: ChronosTls): bool {.inline.} =
    ## False once the transport died without a TLS close_notify. The backend Conn
    ## forwards this to the engine, which refuses a read-until-close body that ends
    ## on such a close (issue #426): with no framing of its own, that body's only
    ## proof of completeness is the alert the peer never sent.
    not t.uncleanEof

  proc newChronosTls*(transport: StreamTransport, ctx: SslContext, host: string,
                      verify: bool, slot: SessionSlot = nil): ChronosTls =
    ## Build a client TLS pump over `transport` using the shared `ctx` (ALPN,
    ## versions, ciphers, client cert already wired). SNI, the expected peer
    ## identity (when `verify` is on) and any cached session are set here; the
    ## caller then `await`s `handshake`.
    let (ssl, rbio, wbio) = newClientSslMem(ctx, host, verify, slot)
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
    ## Drain the write-BIO under `writeLock`, waiting for the lock if a writer holds
    ## it. Only for paths where that wait is harmless: the handshake (the only task
    ## on the connection; no writer exists yet) and `close` (bounded by its own
    ## timeout). The read path uses `tryFlushOut` instead.
    ##
    ## The pending check comes before the acquire. Taking the lock only to find
    ## there is nothing to send is what used to park the h2 mux reader behind every
    ## outbound write (issue #444).
    if bioCtrlPending(t.wbio) <= 0: return
    await t.writeLock.acquire()
    try: await t.drainOut()
    finally: t.writeLock.release()

  proc tryFlushOut(t: ChronosTls) {.async.} =
    ## `flushOut` for the READ path: drains only when no writer owns or wants
    ## `writeLock`, and never WAITS for it. It can still do the send itself, so it
    ## is not a promise that the reader never blocks; see the residual at the end.
    ##
    ## The reader must not park on that lock. `write` holds it across `drainOut`'s
    ## `await transport.write`, so a reader that waited for it would stop pulling
    ## inbound ciphertext for as long as an outbound write is in flight. On an h2
    ## connection that delays every other stream's frames (HEADERS, DATA,
    ## WINDOW_UPDATE, RST_STREAM, GOAWAY) for the duration of one large upload and
    ## starves the keepalive's frame tick; against a peer that stops reading while
    ## its own write is blocked it is a two-sided stall, since our write cannot
    ## finish until the peer drains and the peer cannot drain until we read (#444).
    ## The asyncdispatch TLS pump and the chronos plaintext path both keep reading
    ## during a write; this is what brings the chronos TLS pump in line.
    ##
    ## Skipping the drain loses nothing. Post-handshake the read path queues output
    ## only for a TLS 1.3 KeyUpdate answer or the no_renegotiation alert OpenSSL
    ## sends for a HelloRequest, and neither has to be on the wire before our next
    ## record: a writer's `drainOut` re-checks `bioCtrlPending` after every
    ## `transport.write`, so it picks those bytes up (in order, still under the
    ## lock, so records never interleave). If the last writer releases the lock
    ## without noticing them, the next SSL_read returns WANT_READ again and this
    ## retries -- and if nothing is ever written again, nothing needed them sent.
    ##
    ## One residual stays, much narrower than the stall above. When `writers` is 0
    ## this proc sends the queued bytes itself, and `drainOut`'s `await
    ## transport.write` can park the reader if the peer has stopped draining its
    ## socket and our send buffer has filled. It takes the read path having output of
    ## its own (the KeyUpdate answer or the no_renegotiation alert, tens of bytes) at
    ## a moment when no writer is there to carry it, so ordinary traffic cannot
    ## provoke it the way the old unconditional acquire could -- but it is a block on
    ## the reader, not merely a skipped drain. Handing that output to the writer path
    ## instead of sending it here would remove the residual outright.
    if bioCtrlPending(t.wbio) <= 0: return
    if t.writers > 0: return       # a writer owns or is queued for the lock
    # `writers` is 0, so the only other acquirer can be `close`, whose flush is
    # bounded by a timeout. No await between the check and the acquire.
    await t.writeLock.acquire()
    try: await t.drainOut()
    finally: t.writeLock.release()

  proc feedIn(t: ChronosTls): Future[bool] {.async.} =
    ## Read one chunk of ciphertext from the transport into the read-BIO. Returns
    ## false on EOF -- a clean peer close, or the transport being closed under us
    ## during teardown. Swallowing the closed/errored-transport exception (rather
    ## than letting it propagate up through readSome/recvSome) lets chronos retire
    ## the in-flight read cleanly instead of leaking its future + our stack trace.
    ##
    ## `CancelledError` is the one exception that must NOT become an EOF. In chronos
    ## it derives from CatchableError, so the blanket handler used to turn a
    ## structured cancel -- a `timeouts.total` guard, a CancelToken, or the
    ## `timeouts.connect` bound -- into a clean EOF here: `readSome` returned "", the
    ## engine saw an EOF before any response and raised KeepAliveRaceError, and the
    ## retry layer replayed the request on a fresh connection while the cancel that
    ## was supposed to stop it waited in `cancelAndWait`. Re-raising keeps the
    ## cancellation flowing out through `handshake`/`readSome` so the guard returns
    ## promptly and reports the real error.
    ##
    ## The ciphertext lands in the connection's own scratch buffer rather than a
    ## fresh 64 KiB string per read: the bytes are copied straight into the read-BIO
    ## and the buffer is never handed to a caller, so reuse cannot alias anything.
    ## One buffer is sound because this is the only path that reads the transport --
    ## chronos permits exactly one pending read per transport, so `write` must never
    ## start its own (see the SSL_ERROR_WANT_READ branch there).
    var n = 0
    try:
      n = await t.transport.readOnce(addr t.inBuf[0], t.inBuf.len)
    except CancelledError:
      raise               # never an EOF: see the note above
    except CatchableError:
      t.uncleanEof = true
      return false
    if n <= 0:
      # The transport ended before OpenSSL reported a close_notify (SSL_ERROR_ZERO_
      # RETURN would have come back from SSL_read without ever reaching feedIn), so
      # this EOF is unauthenticated. It is still reported as an EOF -- one before any
      # response has to stay a keep-alive race the engine replays -- but the flag lets
      # the engine reject a read-until-close body that ends here (issue #426).
      t.uncleanEof = true
      return false
    discard bioWrite(t.rbio, cast[cstring](addr t.inBuf[0]), n.cint)
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
        if not await t.feedIn():
          raise newException(IOError, "navi: TLS peer closed during handshake")
      of SSL_ERROR_WANT_WRITE:
        await t.flushOut()
      else:
        raise newException(IOError, "navi: TLS handshake failed")

  proc write*(t: ChronosTls, data: string) {.async.} =
    ## Encrypt and send `data`. Serialized so concurrent h2 streams (and the mux
    ## reader's control frames) never interleave records.
    if data.len == 0: return
    # `writers` tells the read path a writer owns (or is queued for) `writeLock`,
    # so it can skip its opportunistic flush instead of waiting for the lock. It
    # must cover the acquire too, and a cancelled acquire must still clear it.
    inc t.writers
    try:
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
            of SSL_ERROR_WANT_READ:
              # SSL_write wants input: post-handshake that means the peer asked for a
              # TLS 1.2 renegotiation. This path cannot read the transport itself --
              # chronos allows one pending read per transport and the read path (the
              # h2 mux reader, or a ws receive) normally owns it, so a second
              # `readOnce` raises "Read operation already pending!" at once, which
              # used to surface as a bogus EOF and tear the connection down (#444).
              # Renegotiation is now off (SSL_OP_NO_RENEGOTIATION on the context, as
              # RFC 9113 9.2.1 requires for h2), so a peer that still drives us here
              # is misbehaving: drain whatever alert OpenSSL queued and report it.
              await t.drainOut()
              raise newException(IOError,
                "navi: TLS peer requested renegotiation during a write, which navi " &
                "does not allow")
            else:
              raise newException(IOError, "navi: TLS write failed")
      finally:
        t.writeLock.release()
    finally:
      dec t.writers

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
        await t.tryFlushOut()                # rare post-handshake output, never waiting
        if not await t.feedIn(): return ""   # peer closed: EOF for the parser
      of SSL_ERROR_WANT_WRITE:
        # Unreachable with a memory write-BIO, which grows instead of filling up.
        # `flushOut` (which may wait for the lock) rather than `tryFlushOut`: this
        # branch has to make progress or SSL_read spins.
        await t.flushOut()
      of SSL_ERROR_ZERO_RETURN:
        return ""                            # peer sent close_notify
      of SSL_ERROR_SSL:
        raise newException(IOError, "navi: TLS read failed")
      else:
        t.uncleanEof = true
        return ""                            # SYSCALL/unexpected EOF: EOF for the parser

  proc close*(t: ChronosTls) {.async.} =
    ## Close the transport, then free the SSL (which frees its BIOs). Idempotent.
    ## Order matters: closing the transport first lets a background reader parked
    ## in `readOnce` complete with a clean EOF and unwind, rather than racing a
    ## freed SSL; it also avoids leaking the reader's in-flight read future.
    # close_notify first, and not only out of politeness to the peer: SSL_free on an
    # SSL that never shut down runs OpenSSL's ssl_clear_bad_session, which marks the
    # SSL_SESSION not_resumable. That object is the very one navi cached for this
    # origin (openssl_ctx's new-session callback stores the pointer the callback was
    # handed), so re-presenting it would get a full handshake every time and TLS
    # session resumption would be silently dead on this backend. SSL_shutdown queues
    # the alert into the write BIO; the drain below is what puts it on the wire.
    # Best effort and bounded: a dead or blocked peer must not turn `close` into a
    # raise or a hang, and the SSL_SENT_SHUTDOWN flag SSL_shutdown sets is what
    # preserves resumability even if the alert itself never gets out.
    if not t.sslp.isNil:
      ErrClearError()          # see `readSome`: keep the thread's error queue clean
      discard SSL_shutdown(t.sslp)
      try:
        discard await noCancel withTimeout(t.flushOut(), closeNotifyMs.milliseconds)
      except CatchableError: discard
    # FIN before closesocket, so the close_notify (and any last record) written just
    # before this is delivered rather than dropped with the socket (see
    # `gracefulShutdown` in chronos.nim for the Windows failure this prevents).
    # `noCancel`, not a blanket `except CatchableError`: this runs from teardown
    # paths that are themselves often being cancelled, and a CancelledError caught
    # here would either be swallowed (hiding the cancel) or re-raised before the SSL
    # is freed (leaking it and its BIOs). Shielding the awaits instead means the
    # teardown always finishes and no cancellation is lost.
    if not t.transport.isNil:
      try:
        discard await noCancel withTimeout(t.transport.shutdownWait(),
                                           1000.milliseconds)
      except CatchableError: discard
    try: await noCancel t.transport.closeWait()
    except CatchableError: discard
    if not t.sslp.isNil:
      SSL_free(t.sslp); t.sslp = nil

  proc closeSync*(t: ChronosTls) =
    ## Non-awaiting teardown for a GC-reclaimed handle: free the SSL and initiate
    ## transport close (the loop frees it afterwards).
    if not t.sslp.isNil:
      # As in `close`: without SSL_shutdown, SSL_free marks the cached SSL_SESSION
      # not_resumable. Nothing can be drained here (there is nothing to await on),
      # so the peer may not see the alert, but SSL_SENT_SHUTDOWN is still set and
      # the session stays resumable. Same order as the sync and asyncdispatch
      # backends' teardown.
      ErrClearError()
      discard SSL_shutdown(t.sslp)
      SSL_free(t.sslp); t.sslp = nil
    if not t.transport.isNil: t.transport.close()

  proc shutdownTransport*(t: ChronosTls) =
    ## Initiate transport close without awaiting, to unblock a reader parked on a
    ## pending read (used by the h2 mux's `close`). The reader then observes EOF.
    if not t.transport.isNil: t.transport.close()
