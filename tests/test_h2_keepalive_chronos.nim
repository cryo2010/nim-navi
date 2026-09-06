## HTTP/2 PING keepalive, chronos backend. The mirror of `test_h2_keepalive` (see it
## for the scenario): a blackhole peer that stops answering PINGs must have its
## connection torn down so the parked request fails over, while a peer that keeps
## answering PINGs must keep the connection (and the parked request) alive. This
## exercises the chronos mux's own keepalive path, which detects the idle interval
## with `race`/`cancelAndWait` rather than asyncdispatch's `withTimeout`.
##
## The peer runs on its own thread with blocking sockets, off navi's event loop.
import unittest
import pkg/chronos
import std/[net, times]
import navi/backend/chronos as be
import navi/backend/h2mux_chronos
import navi/backend/api            # TlsConfig / ProxyTarget

const
  settingsFrame = "\x00\x00\x00\x04\x00\x00\x00\x00\x00"   # empty SETTINGS (a real h2 peer)
  clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"        # 24 bytes, precedes the frames
  pingAck = "\x00\x00\x08\x06\x01\x00\x00\x00\x00"          # PING+ACK header (len=8), then payload

type PeerArg = tuple[port: int, answerPings: bool]

proc runPeer(arg: PeerArg) {.thread.} =
  ## Own the whole listener on this thread so no socket fd is shared across threads
  ## (a shared fd would be closed by the owning thread's Socket destructor at scope
  ## exit, out from under the other). Accept one client, send SETTINGS, then read
  ## forever; when `answerPings` is set, reply PING+ACK to every PING (still never
  ## delivering the response), otherwise stay a blackhole. Exits when navi closes.
  let listener = newSocket(buffered = false)   # unbuffered: buffered recv batches and
  listener.setSockOpt(OptReuseAddr, true)       # stalls the PING/ACK exchange (deadlock)
  listener.bindAddr(Port(arg.port))
  listener.listen()
  var client: Socket
  listener.accept(client)                        # inherits the listener's unbuffered mode
  client.setSockOpt(OptNoDelay, true)
  client.send(settingsFrame)
  var rest: string                # unparsed bytes after the preface
  var prefaceDropped = false
  while true:
    let data = client.recv(4096)
    if data.len == 0: break
    rest.add data
    if not prefaceDropped:
      if rest.len < clientPreface.len: continue
      rest = rest[clientPreface.len .. ^1]
      prefaceDropped = true
    while rest.len >= 9:                                # parse whole frames
      let length = (rest[0].int shl 16) or (rest[1].int shl 8) or rest[2].int
      if rest.len < 9 + length: break
      let (ftype, flags) = (rest[3].int, rest[4].int)
      let payload = rest[9 ..< 9 + length]
      rest = rest[9 + length .. ^1]
      if arg.answerPings and ftype == 0x6 and (flags and 0x1) == 0:  # a PING, not an ACK
        client.send(pingAck & payload)
  client.close()
  listener.close()

proc startPeer(t: var Thread[PeerArg], port: int, answerPings: bool) =
  createThread(t, runPeer, (port, answerPings))

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
