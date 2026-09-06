## HTTP/2 PING keepalive, chronos backend. The mirror of `test_h2_keepalive` (see it
## for the scenario): a blackhole peer that stops answering PINGs must have its
## connection torn down so the parked request fails over, while a peer that keeps
## answering PINGs must keep the connection (and the parked request) alive. This
## exercises the chronos mux's keepalive ticker, which times the idle interval with
## `race`/`cancelAndWait` rather than asyncdispatch's `withTimeout`.
##
## The peer runs on its own thread with blocking sockets, off navi's event loop.
import unittest
import pkg/chronos
import std/times
import navi/backend/chronos as be
import navi/backend/h2mux_chronos
import navi/backend/api            # TlsConfig / ProxyTarget
import ./support_h2peer            # blocking-socket blackhole / ping-answering peer

proc requestTornDown(mux: H2Mux): Future[bool] {.async.} =
  ## True when the request fails (the connection was torn down); never raises.
  let headers = @[(":method", "GET"), (":scheme", "http"),
                  (":authority", "127.0.0.1"), (":path", "/")]
  try:
    discard await mux.request(headers, "")
    result = false               # unexpectedly completed: the peer never answered
  except CatchableError:
    result = true

proc connectMux(port: int): Future[H2Mux] {.async.} =
  ## Retry until the peer thread has bound and is listening (it now owns the
  ## listener, so the port may not be up the instant this is called).
  var lastErr: ref CatchableError
  for _ in 0 ..< 100:
    try:
      let conn = await be.connect("127.0.0.1", port, false, TlsConfig(), ProxyTarget())
      return await newH2Mux(conn, keepAliveMs = 200)
    except CatchableError as e:
      lastErr = e
      await sleepAsync(chronos.milliseconds(20))
  raise lastErr

var blackholeThread, pingThread: Thread[PeerArg]

type
  DeadOutcome = tuple[finished, tornDown: bool, elapsed: float]
  ParkOutcome = tuple[parked, drained: bool]

proc driveDead(): Future[DeadOutcome] {.async.} =
  ## chronos strict-effects forbid `check` inside {.async.}; collect facts here and
  ## assert them in the test body (the convention in the other chronos suites).
  let mux = await connectMux(9332)
  let start = epochTime()
  let reqFut = requestTornDown(mux)
  let timer = sleepAsync(chronos.milliseconds(5000))
  discard await race(reqFut, timer)     # bounded wait without cancelling reqFut
  result.finished = reqFut.finished     # not finished => it hung: keepalive dead
  if reqFut.finished:
    result.tornDown = reqFut.read()     # torn down => the request failed
    await timer.cancelAndWait()
  result.elapsed = epochTime() - start
  await mux.close()

proc drivePark(): Future[ParkOutcome] {.async.} =
  let mux = await connectMux(9333)
  let reqFut = requestTornDown(mux)
  # 1.5s is > 7 keepalive intervals: a false teardown would have fired long ago.
  await sleepAsync(chronos.milliseconds(1500))
  result.parked = not reqFut.finished   # answered pings keep it alive: still parked
  await mux.close()                     # now tear it down deliberately and drain it
  result.drained = await reqFut

suite "http/2 PING keepalive (chronos)":
  test "a request should fail over when the connection stops answering pings":
    startPeer(blackholeThread, 9332, answerPings = false)
    let o = waitFor driveDead()
    check o.finished             # keepalive tore the dead connection down
    check o.tornDown             # so the parked request failed
    check o.elapsed > 0.2        # it pinged and waited an interval, not failed instantly
    check o.elapsed < 3.0        # ~2x the 200ms interval, nowhere near hanging
    joinThread(blackholeThread)

  test "a request should stay parked when the peer keeps answering pings":
    startPeer(pingThread, 9333, answerPings = true)
    let o = waitFor drivePark()
    check o.parked              # answered pings kept it alive
    check o.drained            # and the deliberate close then tore it down
    joinThread(pingThread)
