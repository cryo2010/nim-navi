## A blocking-socket HTTP/2 peer for the keepalive tests, run on its own thread so
## it stays off navi's event loop. It sends an initial SETTINGS frame and then reads
## forever, either staying a blackhole (never responds again) or answering every
## inbound PING with a PING+ACK -- in both cases it never delivers a response, so a
## parked request only survives if the connection itself stays up.
##
## The peer owns its whole listener (create/bind/listen/accept/close): sharing a
## socket fd across threads would let the owning thread's Socket destructor close it
## out from under the other. It binds to 127.0.0.1 explicitly (the CI sandbox
## rejects the 0.0.0.0 wildcard) and unbuffered (a buffered recv batches reads and
## stalls the PING/ACK exchange).
import std/net
from navi/proto/h2/frame import encodeSettings, encodePing

type PeerArg* = tuple[port: int, answerPings: bool]

const clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"   # 24 bytes, precedes the frames

proc runPeer*(arg: PeerArg) {.thread.} =
  let listener = newSocket(buffered = false)
  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(arg.port), "127.0.0.1")
  listener.listen()
  var client: Socket
  listener.accept(client)                        # inherits the listener's unbuffered mode
  client.send(encodeSettings([]))                # empty SETTINGS: a real h2 peer, then dark
  var rest: string                               # unparsed bytes after the preface
  var prefaceDropped = false
  while true:
    let data = client.recv(4096)
    if data.len == 0: break
    rest.add data
    if not prefaceDropped:
      if rest.len < clientPreface.len: continue
      rest = rest[clientPreface.len .. ^1]
      prefaceDropped = true
    while rest.len >= 9:                          # parse whole frames off the buffer
      let length = (rest[0].int shl 16) or (rest[1].int shl 8) or rest[2].int
      if rest.len < 9 + length: break
      let (ftype, flags) = (rest[3].int, rest[4].int)
      let payload = rest[9 ..< 9 + length]
      rest = rest[9 + length .. ^1]
      if arg.answerPings and ftype == 0x6 and (flags and 0x1) == 0:  # a PING, not an ACK
        client.send(encodePing(payload, ack = true))
  client.close()
  listener.close()

proc startPeer*(t: var Thread[PeerArg], port: int, answerPings: bool) =
  createThread(t, runPeer, (port, answerPings))
