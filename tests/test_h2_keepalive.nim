## HTTP/2 PING keepalive. Two peers, both silent at the application layer (neither
## ever answers the request), differ only in whether they answer PINGs:
##
## * blackhole -- never responds again after its SETTINGS. The keepalive pings, gets
##   no answer within an interval, and tears the connection down so the parked
##   request fails over (rather than hanging forever: no read timeout is set here).
## * ping-answering -- replies PING+ACK but still never delivers the response. Any
##   inbound frame proves liveness, so the connection must stay up and the request
##   must stay parked. (Catching an app-silent-but-live stream is the SSE idle
##   timeout's job, not the keepalive's -- see the `sse` interop.)
##
## Each peer runs on its own thread with blocking sockets: keeping it off navi's
## event loop avoids the single-loop scheduling coupling that would otherwise stall
## the request/PING/ACK exchange the ping-answering case depends on.
import unittest
import std/[asyncdispatch, times]
import navi/backend/asyncdispatch as be
import navi/backend/h2mux
import navi/backend/api            # TlsConfig / ProxyTarget
import ./support_h2peer            # blocking-socket blackhole / ping-answering peer

proc requestTornDown(mux: H2Mux): Future[bool] {.async.} =
  ## True when the request fails (the connection was torn down); never raises, so a
  ## timeout wait around it can tell "failed fast" from "still parked" without
  ## re-raising the failure.
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
      await sleepAsync(20)
  raise lastErr

var blackholeThread, pingThread: Thread[PeerArg]

suite "http/2 PING keepalive":
  test "a request should fail over when the connection stops answering pings":
    startPeer(blackholeThread, 9330, answerPings = false)
    proc run(): Future[float] {.async.} =
      let mux = await connectMux(9330)
      let start = epochTime()
      let reqFut = requestTornDown(mux)
      let finished = await withTimeout(reqFut, 5000)   # false => it hung: keepalive dead
      result = epochTime() - start
      check finished
      if finished: check reqFut.read()                 # torn down => the request failed
      await mux.close()

    let elapsed = waitFor run()
    check elapsed > 0.2          # it pinged and waited an interval, not failed instantly
    check elapsed < 3.0          # ~2x the 200ms interval, nowhere near hanging
    joinThread(blackholeThread)

  test "a request should stay parked when the peer keeps answering pings":
    startPeer(pingThread, 9331, answerPings = true)
    proc run() {.async.} =
      let mux = await connectMux(9331)
      let reqFut = requestTornDown(mux)
      # 1.5s is > 7 keepalive intervals: a false teardown would have fired long ago.
      let finished = await withTimeout(reqFut, 1500)
      check not finished          # answered pings keep it alive: still parked
      await mux.close()           # now tear it down deliberately and drain the request
      check (await reqFut)

    waitFor run()
    joinThread(pingThread)
