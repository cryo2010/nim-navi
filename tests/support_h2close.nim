## A blocking-socket HTTP/2 peer that answers the first request with its whole body at
## once (16 KiB DATA frames, all inside the client's 8 MiB stream window), optionally a
## GOAWAY, then waits `closeDelayMs` and closes the TCP connection, the way a server
## recycling a connection (nginx `keepalive_requests`, a graceful shutdown) does.
## `endStream = false` leaves the stream open, like a tunnel peer that sends its last
## bytes (a WebSocket Close frame) and drops the connection. Run on its own thread so
## it stays off navi's event loop. Used by `h2_close_drain_spec` (#461 round 2).
import std/[net, os]
from navi/proto/h2/frame import encodeSettings, encodeSettingsAck, encodeHeaders,
  encodeData, encodeGoAway

type ClosePeerArg* = tuple[port: int, bodyBytes: int, closeDelayMs: int,
                           goaway: bool, endStream: bool]

const clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

proc runClosePeer*(arg: ClosePeerArg) {.thread.} =
  let listener = newSocket(buffered = false)
  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(arg.port), "127.0.0.1")
  listener.listen()
  var client: Socket
  listener.accept(client)
  client.send(encodeSettings([]))
  var rest: string
  var prefaceDropped = false
  var chunk = newString(16384)
  block serve:
    while true:
      let data = client.recv(65536)
      if data.len == 0: break serve
      rest.add data
      if not prefaceDropped:
        if rest.len < clientPreface.len: continue
        rest = rest[clientPreface.len .. ^1]
        prefaceDropped = true
      while rest.len >= 9:
        let length = (rest[0].int shl 16) or (rest[1].int shl 8) or rest[2].int
        if rest.len < 9 + length: break
        let ftype = rest[3].int
        let flags = rest[4].int
        let sid = (uint32(rest[5].uint8 and 0x7f) shl 24) or (uint32(rest[6].uint8) shl 16) or
                  (uint32(rest[7].uint8) shl 8) or uint32(rest[8].uint8)
        rest = rest[9 + length .. ^1]
        if ftype == 4 and (flags and 1) == 0:
          client.send(encodeSettingsAck())
        elif ftype == 1:
          client.send(encodeHeaders(sid, "\x88", endStream = false, endHeaders = true))
          var left = arg.bodyBytes
          while left > 0:
            let n = min(left, chunk.len)
            left -= n
            client.send(encodeData(sid, chunk[0 ..< n],
                                   endStream = arg.endStream and left == 0))
          if arg.goaway: client.send(encodeGoAway(sid, 0))
          sleep(arg.closeDelayMs)
          break serve
  client.close()
  listener.close()
