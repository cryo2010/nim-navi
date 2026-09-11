## HTTP/2 connection multiplexer, asyncdispatch backend. The whole mux body is the
## shared `h2mux_common.nim` include; only the async-runtime-specific pieces below
## (fire-and-forget send, keepAlive timer, reader teardown, `newH2Mux`, `close`) are
## written per backend. See `h2mux_common.nim` for the design.

import std/[asyncdispatch, tables, deques, sets]
import ../proto/h2/conn
from ../proto/h2/frame import encodePing   # keepalive PING (a non-stream control frame)
import ../core/response          # for ResponseTooLargeError
import ../core/request           # for BodyProducer
import ../core/decompress        # for streaming response decompression
import ./asyncdispatch as be     # for Conn / BodySink

include ./h2mux_common

proc fireSend(mux: H2Mux, data: string) {.gcsafe, raises: [].} =
  ## asyncdispatch has no untracked spawn, so `asyncCheck` the serialized send and
  ## drop the returned future; `send` already swallows nothing, so guard the call.
  ## Fully non-raising (catches Exception): the stream-teardown paths that fire a
  ## best-effort RST cannot handle a scheduler failure here.
  try: asyncCheck mux.send(data)
  except Exception: discard

proc keepAlive(mux: H2Mux) {.async.} =
  ## Once per interval (not per chunk): if a connection with active streams has gone a
  ## whole interval with no inbound frame, PING it; if the next interval is still
  ## silent, treat the connection as dead and tear it down so its streams fail over.
  ## Any inbound frame -- not only a PING ACK -- counts as liveness (the reader sets
  ## `sawFrameSinceTick`), so this tracks a live transport, not a responsive app.
  ## Waking on `readerDone` too lets it exit promptly when the connection closes.
  try:
    while mux.alive:
      await (sleepAsync(mux.keepAliveMs) or mux.readerDone)
      if not mux.alive or mux.readerDone.finished: break
      if mux.sawFrameSinceTick:
        mux.sawFrameSinceTick = false
        mux.pingOutstanding = false          # a frame arrived this interval: alive
      elif mux.activeStreams == 0 and mux.settingsSeen.finished:
        mux.pingOutstanding = false           # nothing to protect: idle without pinging
        # ... but a connection still waiting for the peer's SETTINGS IS being waited on
        # (openConnect parks on settingsSeen with zero active streams), so keep probing
        # while settingsSeen is unfinished: a peer that completes ALPN=h2 then goes dark
        # before its SETTINGS would otherwise never be torn down (issue #265).
      elif mux.pingOutstanding:               # pinged last interval, still silent: dead
        mux.alive = false
        be.shutdownConn(mux.transport)        # wake the reader; it fails streams + closes
        break
      else:
        # Fire-and-forget: do NOT join the (possibly blocked) send chain. On a network
        # partition with a request body in flight the kernel send buffer fills and
        # `sendAll` never completes; awaiting the PING here would chain behind that
        # blocked tail and park the timer loop forever, so the "pinged last interval,
        # still silent: dead" branch could never fire (issue #264). Letting the loop
        # keep ticking is what detects the dead peer, whether or not the PING gets out.
        mux.fireSend(encodePing(h2KeepAlivePayload))
        mux.pingOutstanding = true
  except CatchableError:
    discard   # a failed send/transport tears down via the reader; nothing to do here

proc reader(mux: H2Mux) {.async.} =
  try:
    while mux.alive:
      let recvFut = be.recvSome(mux.transport)
      if mux.h2.goneAway and mux.activeStreams > 0:
        if not await withTimeout(recvFut, goAwayGraceMs):        # peer went silent
          # asyncdispatch withTimeout does not cancel `recvFut`, so it is still parked
          # on the fd. Closing the transport under it (the teardown below) crashes --
          # the exact pattern this module's `close` doc warns of. EOF the read via
          # shutdownConn and drain it first, so the fd is closed with no read pending
          # (issue #267; the chronos twin cancels via withTimeout instead).
          be.shutdownConn(mux.transport)
          try: discard await recvFut
          except CatchableError: discard
          break
      let chunk = await recvFut
      if chunk.len == 0: break                 # peer closed
      mux.sawFrameSinceTick = true             # inbound bytes: liveness for the keepalive
      let toSend = mux.h2.feed(chunk)
      if toSend.len > 0: await mux.send(toSend)   # includes a GOAWAY on a conn error
      if mux.h2.sawPeerSettings and not mux.settingsSeen.finished:
        mux.settingsSeen.complete()               # unblocks a waiting Extended CONNECT
      mux.queueBodies()                           # move sink-stream body into recvq
      mux.wakeRecvers()                           # let sink drains pull new body + ack
      mux.dispatch()                              # complete finished buffered streams
      mux.wakeSenders()                           # a WINDOW_UPDATE may have drained a send
      if mux.h2.connError.len > 0: break          # fatal: fail all in-flight below
      if mux.h2.goneAway and mux.activeStreams == 0: break
  except CatchableError:
    discard
  mux.failAll("navi: http/2 connection closed")
  try: await be.close(mux.transport)   # the reader owns the transport close
  except CatchableError: discard
  if not mux.settingsSeen.finished: mux.settingsSeen.complete()  # unblock a pending
  if not mux.readerDone.finished: mux.readerDone.complete()      # openConnect (dead conn)

proc newH2Mux*(transport: be.Conn, maxBody = 0, decompress = false,
               keepAliveMs = 0): Future[H2Mux] {.async.} =
  ## Take ownership of a freshly connected h2 transport, send the preface, and
  ## start the background reader.
  let mux = H2Mux(transport: transport, h2: initH2Conn(maxBody), alive: true,
                  decompress: decompress, cap: maxBody, keepAliveMs: keepAliveMs,
                  readerDone: newFuture[void]("h2mux.readerDone"),
                  settingsSeen: newFuture[void]("h2mux.settingsSeen"),
                  waiters: initTable[uint32, Future[H2Response]](),
                  sendReady: initTable[uint32, seq[Future[void]]](),
                  sinkStreams: initHashSet[uint32](),
                  recvq: initTable[uint32, Deque[string]](),
                  recvReady: initTable[uint32, Future[void]](),
                  decoders: initTable[uint32, CappedDecoder](),
                  pendingSlots: initDeque[Future[void]]())
  try:
    await be.sendAll(transport, mux.h2.preamble())
  except CatchableError:
    # The reader (which owns closing the transport) has not started yet, so if the
    # preface send fails nothing else closes the transport we just took ownership of.
    # Close it here before re-raising, or the fd/TLS handle leaks -- the caller's
    # `except` frees the pending-future, not this connection (issue #311).
    await be.close(transport)
    raise
  asyncCheck reader(mux)
  if keepAliveMs > 0: asyncCheck keepAlive(mux)
  result = mux

proc close*(mux: H2Mux) {.async.} =
  ## Shut the shared connection down: fail any in-flight streams, wake the
  ## background reader (socket shutdown), and wait for it to exit and close the
  ## transport. Joining the reader (rather than closing the transport out from
  ## under it) avoids leaving it suspended on a dead fd, which crashes at teardown.
  if mux.readerDone.finished: return   # reader already exited (e.g. peer closed)
  mux.alive = false
  mux.failAll("navi: client closed")
  be.shutdownConn(mux.transport)       # unblock the reader's pending read/write
  await mux.readerDone                  # it observes EOF, closes the transport, exits
