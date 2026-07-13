## Sans-io HTTP/2 client connection tests, driven by simulated server frames.

import unittest
import std/strutils
import navi/proto/h2/[conn, frame, hpack]

suite "h2 client connection":
  test "sends preface and SETTINGS":
    var c = initH2Client()
    let pre = c.clientPreamble()
    check pre.startsWith(connectionPreface)
    var d: FrameDecoder
    d.feed(pre[connectionPreface.len ..< pre.len])
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftSettings)
    check d.next(f)                       # the WINDOW_UPDATE
    check f.typ == uint8(ftWindowUpdate)

  test "encodes a request as a HEADERS frame with pseudo-headers":
    var c = initH2Client()
    let wire = c.encodeRequest(@[
      (":method", "GET"), (":scheme", "https"), (":path", "/"),
      (":authority", "example.com")], body = "")
    var d: FrameDecoder
    d.feed(wire)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftHeaders)
    check (f.flags and flagEndStream) != 0   # no body -> END_STREAM on HEADERS
    check (f.flags and flagEndHeaders) != 0
    var dec = initHpackDecoder()
    check dec.decode(f.payload) == @[
      (":method", "GET"), (":scheme", "https"), (":path", "/"),
      (":authority", "example.com")]

  test "assembles a response from server frames and acks control frames":
    var c = initH2Client()
    discard c.clientPreamble()
    discard c.encodeRequest(@[(":method", "GET"), (":scheme", "https"),
                             (":path", "/"), (":authority", "example.com")], "")

    # Build a server-side frame stream: SETTINGS, HEADERS(:status 200, ct), DATA.
    let enc = HpackEncoder()
    var server = encodeSettings([])
    server.add encodeHeaders(1,
      enc.encode(@[(":status", "200"), ("content-type", "text/plain")]),
      endStream = false, endHeaders = true)
    server.add encodeData(1, "hello", endStream = true)

    let toSend = c.feed(server)
    check c.done
    check not c.failed
    let resp = c.response()
    check resp.status == 200
    check resp.body == "hello"
    check resp.headers == @[("content-type", "text/plain")]

    # The client should have acked SETTINGS and replenished flow-control windows.
    var d: FrameDecoder
    d.feed(toSend)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftSettings)
    check (f.flags and flagAck) != 0

  test "reports a stream reset as a failure":
    var c = initH2Client()
    let server = encodeRstStream(1, 1)  # PROTOCOL_ERROR
    discard c.feed(server)
    check c.done
    check c.failed

  test "responds to a server PING with an ACK":
    var c = initH2Client()
    let toSend = c.feed(encodePing("01234567"))
    var d: FrameDecoder
    d.feed(toSend)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftPing)
    check (f.flags and flagAck) != 0
    check f.payload == "01234567"
