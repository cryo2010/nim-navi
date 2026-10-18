## A cancelled h2 request whose HEADERS never reached the transport must not be
## RST (chronos; review of #462). The mux's send has cancellable waits before the
## write: the serialized send chain and the fairness yield. A guard timeout or
## CancelToken landing there used to reap the stream with a RST_STREAM for an id
## the peer never saw, which RFC 9113 6.4 makes a connection error: the peer sends
## GOAWAY(PROTOCOL_ERROR) and every sibling stream on the mux dies. The mux now
## records a stream as on the wire only once nothing can stop its first write, and
## RSTs only those. A sender cancelled while queued on the chain also hands its link
## on only once the sender ahead of it has finished, so the senders behind it cannot
## overtake. Control: a request cancelled after its HEADERS went out is still RST, so
## the peer can free it.
##
## The same cancel points also abandon DATA frames that were already framed, and
## framing debits the connection send window: the window must get those bytes back,
## or every such cancel shrinks it for good and the mux eventually cannot upload.
##
## asyncdispatch has no cancellation, so no request can be abandoned before its
## first write there; this is chronos only.
import unittest
import std/[monotimes, times, atomics, strutils]
import pkg/chronos
import navi/backend/chronos as be
import navi/backend/h2mux_chronos
import navi/backend/fairness       # naviFairSliceUs
import navi/backend/api            # TlsConfig / ProxyTarget
import ./support_h2rst

const basePort = 9520

proc hdrs(meth, path: string): seq[(string, string)] =
  @[(":method", meth), (":scheme", "http"), (":authority", "127.0.0.1"),
    (":path", path)]

proc connectMux(port: int): Future[H2Mux] {.async.} =
  ## Retry until the peer thread has bound its listener.
  var lastErr: ref CatchableError
  for _ in 0 ..< 100:
    try:
      let conn = await be.connect("127.0.0.1", port, false, TlsConfig(), ProxyTarget())
      return await newH2Mux(conn)
    except CatchableError as e:
      lastErr = e
      await sleepAsync(chronos.milliseconds(20))
  raise lastErr

proc spendSlice() =
  ## Hold the thread for longer than one fairness slice without an await, as a
  ## saturated transfer does, so the next send has to yield before it writes.
  let t0 = getMonoTime()
  while (getMonoTime() - t0).inMicroseconds <= naviFairSliceUs: discard

proc waitUntil(cond: proc(): bool {.gcsafe, raises: [].}) {.async.} =
  for _ in 0 ..< 200:
    if cond(): return
    await sleepAsync(chronos.milliseconds(10))

when naviFairSliceUs <= 100_000:
  suite "h2 mux: cancelling an unwritten request (chronos)":
    test "a request cancelled before its HEADERS were written sends no RST_STREAM":
      var th: Thread[RstArg]
      startRstPeer(th, basePort)
      proc run(): Future[(int, bool, bool, bool, int)] {.async.} =
        let mux = await connectMux(basePort)
        let warm = await mux.request(hdrs("GET", "/warm"), "")
        spendSlice()
        # `a` parks on the fairness yield before its write; `b` queues behind it
        # on the send chain. Neither has written a byte.
        let a = mux.request(hdrs("GET", "/a"), "")
        let b = mux.request(hdrs("GET", "/b"), "")
        let parked = not a.finished() and not b.finished()
        # Cancel both before the loop turns again (a turn would let `a` write).
        let cb = b.cancelAndWait()
        let ca = a.cancelAndWait()
        await cb
        await ca
        # Anything the reap fired is queued on the send chain: let it flush and the
        # peer react before judging the connection.
        await sleepAsync(chronos.milliseconds(200))
        var after = 0
        try:
          let resp = await mux.request(hdrs("GET", "/after"), "")
          after = resp.status
        except CatchableError:
          after = -1
        await mux.close()
        return (warm.status, parked, a.cancelled(), b.cancelled(), after)
      let (warm, parked, aCancelled, bCancelled, after) = waitFor run()
      joinThread(th)
      check warm == 200
      check parked
      check aCancelled
      check bCancelled
      check idleRstSeen.load == 0     # no RST for a stream the peer never saw
      check rstSeen.load == 0
      check headersSeen.load == 2     # /warm and /after only
      check after == 200              # the mux survived: no GOAWAY

    test "cancelling a sender queued on the send chain keeps the chain in order":
      # chronos hands a cancellation to the future being awaited. If the queued
      # sender's chain wait owned the previous link, cancelling it would release
      # the sender behind it while the first is still parked before its write, and
      # the later stream's HEADERS would overtake the earlier one's.
      var th: Thread[RstArg]
      startRstPeer(th, basePort + 2)
      proc run(): Future[(int, int, int)] {.async.} =
        let mux = await connectMux(basePort + 2)
        discard await mux.request(hdrs("GET", "/warm"), "")
        spendSlice()
        let a = mux.request(hdrs("GET", "/a"), "")   # parked on the fairness yield
        let b = mux.request(hdrs("GET", "/b"), "")   # queued behind a
        let c = mux.request(hdrs("GET", "/c"), "")   # queued behind b
        await b.cancelAndWait()
        var sa, sc = 0
        try: sa = (await a).status
        except CatchableError: sa = -1
        try: sc = (await c).status
        except CatchableError: sc = -1
        var after = 0
        try: after = (await mux.request(hdrs("GET", "/after"), "")).status
        except CatchableError: after = -1
        await mux.close()
        return (sa, sc, after)
      let (sa, sc, after) = waitFor run()
      joinThread(th)
      check orderErrors.load == 0
      check idleRstSeen.load == 0
      check sa == 200
      check sc == 200
      check after == 200

    test "an upload chunk cancelled before its write gives the connection window back":
      var th: Thread[RstArg]
      startRstPeer(th, basePort + 3)
      proc run(): Future[(bool, bool, int)] {.async.} =
        let mux = await connectMux(basePort + 3)
        discard await mux.request(hdrs("GET", "/warm"), "")
        # The first chunk comes back at once but only after a slice of CPU, so its
        # send (framed: 20000 bytes debited from the 65535 connection window) parks
        # on the fairness yield before writing. The second never comes.
        let never = newFuture[string]("test.never")
        var calls = 0
        let producer = AsyncBodyProducer(proc(): Future[string] =
          inc calls
          if calls > 1: return never
          spendSlice()
          let f = newFuture[string]("test.chunk")
          f.complete(repeat('x', 20_000))
          f)
        sliceRestart()               # the HEADERS go straight out
        let a = mux.request(hdrs("POST", "/a"), "", asyncStream = producer)
        let parked = calls == 1 and not a.finished()
        await a.cancelAndWait()
        # The peer never sends WINDOW_UPDATE: a body of exactly the default window
        # completes only if the cancelled chunk's 20000 bytes were given back.
        let b = mux.request(hdrs("POST", "/b"), repeat('z', 65535))
        var status = -1
        if await withTimeout(b, chronos.seconds(3)):
          try: status = (await b).status
          except CatchableError: discard
        await mux.close()
        return (parked, a.cancelled(), status)
      let (parked, aCancelled, status) = waitFor run()
      joinThread(th)
      check parked
      check aCancelled
      check status == 200
      check dataSeen.load == 65535   # /b's body only: the cancelled chunk never left
      check idleRstSeen.load == 0

    test "a request cancelled after its HEADERS were written is still RST":
      var th: Thread[RstArg]
      startRstPeer(th, basePort + 1)
      proc run(): Future[(bool, int)] {.async.} =
        let mux = await connectMux(basePort + 1)
        # A body that never comes: HEADERS (no END_STREAM) go out, then the
        # request parks in the producer with the stream open on both ends.
        let never = newFuture[string]("test.never")
        let producer = AsyncBodyProducer(proc(): Future[string] = never)
        let r = mux.request(hdrs("POST", "/hang"), "", asyncStream = producer)
        await waitUntil(proc(): bool = headersSeen.load == 1)
        await r.cancelAndWait()
        await waitUntil(proc(): bool = rstSeen.load == 1)
        var after = 0
        try:
          let resp = await mux.request(hdrs("GET", "/after"), "")
          after = resp.status
        except CatchableError:
          after = -1
        await mux.close()
        return (r.cancelled(), after)
      let (cancelled, after) = waitFor run()
      joinThread(th)
      check cancelled
      check rstSeen.load == 1         # the peer is told to free the live stream
      check idleRstSeen.load == 0
      check after == 200
else:
  suite "h2 mux: cancelling an unwritten request (chronos)":
    test "skipped: needs a fairness slice short enough to spend in a test":
      skip()
