## The chronos TLS pump must keep reading while one of its own writes is in
## flight (issue #444).
##
## Driven by tests/interop/tls_read_during_write.sh, which starts a TLS server
## that greets the client and then stops reading, and exports NAVI_RDW_PORT /
## NAVI_RDW_CA. The test drives `ChronosTls` directly, because the defect is in
## the pump rather than in any HTTP layer above it: a multi-MiB `write` to a peer
## that has stopped reading parks inside `transport.write` holding the pump's
## write lock, and `readSome` must still deliver the bytes the peer already sent.
##
## Before the fix `readSome` called `flushOut`, which took that same lock before
## looking at whether the write-BIO had anything to flush, so the only reader on
## the connection was parked for the whole duration of the write: on an h2
## connection every other stream's frames (and the keepalive's frame tick) stall
## behind one large upload, and against a peer that stops reading while its own
## send is blocked neither side can make progress.

import unittest
import std/[os, strutils]
import pkg/chronos, pkg/chronos/transports/stream
import navi/backend/api
import navi/backend/chronos_tls

const
  greeting = "PING"
  uploadSize = 4 * 1024 * 1024   ## far more than the server's receive window

type Outcome = object
  got: string            ## what `readSome` returned ("" on EOF)
  timedOut: bool         ## the read never completed: the reader was parked
  parkedBefore: bool     ## the write was still in flight when the read was issued
  parkedAfter: bool      ## and still in flight once the read had returned

proc readDuringWrite(ctx: SslContext, port: Port): Future[Outcome] {.async.} =
  let transport = await connect(initTAddress("127.0.0.1", port))
  let tls = newChronosTls(transport, ctx, "127.0.0.1", true)
  try:
    await tls.handshake()
    # A buffered send far larger than the peer's receive window, to a peer that
    # never reads: it cannot complete, so it stays inside `transport.write` with
    # the pump's write lock held.
    let wfut = tls.write(newString(uploadSize))
    await sleepAsync(chronos.milliseconds(250))
    result.parkedBefore = not wfut.finished()
    try:
      result.got = await wait(tls.readSome(), chronos.seconds(5))
    except AsyncTimeoutError:
      result.timedOut = true
    result.parkedAfter = not wfut.finished()
    await wfut.cancelAndWait()
  finally:
    await tls.close()

when isMainModule:
  var cfg = TlsConfig(caFile: getEnv("NAVI_RDW_CA"))
  let ctx = newTlsContext(cfg)        # raises Exception, so it stays out of the async proc
  let o = waitFor readDuringWrite(ctx, getEnv("NAVI_RDW_PORT").parseInt.Port)
  ctx.destroyContext()

  suite "chronos TLS pump: reading while a write is in flight (#444)":
    test "the large write really is parked (otherwise the case is not exercised)":
      check o.parkedBefore
      check o.parkedAfter

    test "the peer's data arrives while that write is still blocked":
      check not o.timedOut
      check o.got == greeting
