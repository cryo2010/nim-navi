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
import std/[asyncdispatch, net, nativesockets, times]
import navi/backend/asyncdispatch as be
import navi/backend/h2mux
import navi/backend/api            # TlsConfig / ProxyTarget

const
  settingsFrame = "\x00\x00\x00\x04\x00\x00\x00\x00\x00"
    ## a valid empty SETTINGS frame (len=0, type=0x4, stream=0): proves a real h2
    ## peer before it goes dark, so a teardown is the keepalive's doing, not a
    ## malformed-first-frame error.
  clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"   # 24 bytes, precedes the frames
  pingAck = "\x00\x00\x08\x06\x01\x00\x00\x00\x00"     # PING+ACK header (len=8), then payload

type PeerArg = tuple[fd: int, answerPings: bool]

proc runPeer(arg: PeerArg) {.thread.} =
  ## Accept one client, send SETTINGS, then read forever. When `answerPings` is set,
  ## reply PING+ACK to every PING (still never delivering the response); otherwise
  ## stay a blackhole. Exits when navi closes the connection.
  let listener = newSocket(arg.fd.SocketHandle, AF_INET, SOCK_STREAM, IPPROTO_TCP,
                           buffered = false)
  var client: Socket
  listener.accept(client)
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

proc startPeer(t: var Thread[PeerArg], port: int, answerPings: bool) =
  ## Bind+listen on the calling thread (so the port is up before navi connects),
  ## then hand the listener to a peer thread.
  let listener = newSocket()
  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(port))
  listener.listen()
  createThread(t, runPeer, (listener.getFd().int, answerPings))

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
  let conn = await be.connect("127.0.0.1", port, false, TlsConfig(), ProxyTarget())
  result = await newH2Mux(conn, keepAliveMs = 200)

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
