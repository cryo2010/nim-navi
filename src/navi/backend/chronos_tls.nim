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
      writeLock: AsyncLock  ## serialize SSL_write + wbio drains so concurrent streams
                            ## (and post-handshake output) never interleave on the wire

  const tlsBufSize = 16384

  proc sslPtr*(t: ChronosTls): SslPtr = t.sslp
    ## The underlying SSL, for `negotiatedProtocol` / `verifyPeer` after handshake.

  proc newChronosTls*(transport: StreamTransport, ctx: SslContext, host: string,
                      slot: SessionSlot = nil): ChronosTls =
    ## Build a client TLS pump over `transport` using the shared `ctx` (ALPN,
    ## versions, ciphers, client cert already wired). SNI and any cached session
    ## are set here; the caller then `await`s `handshake`.
    let (ssl, rbio, wbio) = newClientSslMem(ctx, host, slot)
    ChronosTls(transport: transport, sslp: ssl, rbio: rbio, wbio: wbio,
               writeLock: newAsyncLock())

  proc drainOut(t: ChronosTls) {.async.} =
    ## Push any ciphertext OpenSSL queued in the write-BIO onto the transport.
    ## Caller holds `writeLock`.
    while true:
      let pending = bioCtrlPending(t.wbio)
      if pending <= 0: break
      var buf = newString(pending)
      let n = bioRead(t.wbio, cast[cstring](addr buf[0]), pending.cint)
      if n <= 0: break
      buf.setLen(n)
      discard await t.transport.write(buf)

  proc flushOut(t: ChronosTls) {.async.} =
    await t.writeLock.acquire()
    try: await t.drainOut()
    finally: t.writeLock.release()

  proc feedIn(t: ChronosTls): Future[bool] {.async.} =
    ## Read one chunk of ciphertext from the transport into the read-BIO. Returns
    ## false on a clean EOF (peer closed), so callers can end rather than spin.
    var buf = newString(tlsBufSize)
    let n = await t.transport.readOnce(addr buf[0], buf.len)
    if n <= 0: return false
    discard bioWrite(t.rbio, cast[cstring](addr buf[0]), n.cint)
    return true

  proc handshake*(t: ChronosTls) {.async.} =
    ## Drive the TLS handshake to completion. Raises on failure (a truncated
    ## exchange or a protocol/verification error); the caller runs `verifyPeer`
    ## afterwards. Wrap the whole call in a timeout at the connect site.
    while true:
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
    await t.writeLock.acquire()
    try:
      var off = 0
      while off < data.len:
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
            if not await t.feedIn():
              raise newException(IOError, "navi: TLS closed during write")
          else:
            raise newException(IOError, "navi: TLS write failed")
    finally:
      t.writeLock.release()

  proc readSome*(t: ChronosTls): Future[string] {.async.} =
    ## Decrypt and return one chunk of application data, or "" on a clean close
    ## (close_notify or peer EOF). Raises on a genuine protocol error.
    var buf = newString(tlsBufSize)
    while true:
      let n = SSL_read(t.sslp, addr buf[0], buf.len)
      if n > 0:
        buf.setLen(n)
        return buf
      let err = SSL_get_error(t.sslp, n)
      case err
      of SSL_ERROR_WANT_READ:
        await t.flushOut()                   # rare post-handshake output first
        if not await t.feedIn(): return ""   # peer closed: EOF for the parser
      of SSL_ERROR_WANT_WRITE:
        await t.flushOut()
      of SSL_ERROR_ZERO_RETURN:
        return ""                            # peer sent close_notify
      of SSL_ERROR_SSL:
        raise newException(IOError, "navi: TLS read failed")
      else:
        return ""                            # SYSCALL/unexpected EOF: EOF for the parser

  proc close*(t: ChronosTls) {.async.} =
    ## Free the SSL (which frees its BIOs) and close the transport. Idempotent.
    if not t.sslp.isNil:
      SSL_free(t.sslp); t.sslp = nil
    try: await t.transport.closeWait()
    except CatchableError: discard

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
