## HTTP/2 Extended CONNECT (RFC 8441) foundation (#190): the peer-capability
## setting and the CONNECT + :protocol pseudo-header block for WebSocket-over-h2.

import unittest
import std/strutils
import navi/proto/h2/[conn, frame, hpack]
import navi/core/[h2glue, url, headers]

suite "h2 Extended CONNECT (RFC 8441)":
  test "peerAllowsConnect should reflect SETTINGS_ENABLE_CONNECT_PROTOCOL":
    block:                                    # absent -> not allowed
      let c = initH2Conn()
      discard c.feed(encodeSettings([]))
      check not c.peerAllowsConnect
    block:                                    # =1 -> allowed
      let c = initH2Conn()
      discard c.feed(encodeSettings({settingsEnableConnectProtocol: 1'u32}))
      check c.peerAllowsConnect
    block:                                    # =0 -> not allowed
      let c = initH2Conn()
      discard c.feed(encodeSettings({settingsEnableConnectProtocol: 0'u32}))
      check not c.peerAllowsConnect

  test "sawPeerSettings should flip only once the peer's initial SETTINGS lands":
    block:                                    # nothing fed yet
      let c = initH2Conn()
      check not c.sawPeerSettings
    block:                                    # the peer's non-ACK SETTINGS
      let c = initH2Conn()
      discard c.feed(encodeSettings([]))
      check c.sawPeerSettings
    block:                                    # a bare SETTINGS ACK is not the peer's settings
      let c = initH2Conn()
      discard c.feed(encodeSettingsAck())
      check not c.sawPeerSettings

  test "a non-SETTINGS preface sets connError without sawPeerSettings (handshake fails fast)":
    # The WebSocket-over-h2 SETTINGS-wait loop reads until sawPeerSettings, but must
    # raise on a fatal connection error instead of spinning on a dead-but-open peer.
    # A preface violation is the reachable pre-SETTINGS fatal, so the loop's
    # `connError` guard must see it while sawPeerSettings stays false.
    let c = initH2Conn()
    discard c.feed(encodePing(newString(8)))    # RFC 9113 3.4: first frame must be SETTINGS
    check not c.sawPeerSettings
    check c.connError.len > 0

  test "a GOAWAY after the tunnel stream opens marks it done (CONNECT-headers wait fails fast)":
    # The CONNECT-headers wait loop must exit on a terminal connection state, not
    # block; streamDone folds in GOAWAY/fatal, so it flips once the peer goes away.
    let c = initH2Conn()
    discard c.feed(encodeSettings({settingsEnableConnectProtocol: 1'u32}))
    let sid = c.openStream()
    check not c.streamDone(sid)
    discard c.feed(encodeGoAway(0, errNoError))
    check c.streamDone(sid)

  test "h2ConnectHeaderList should build the RFC 8441 pseudo-headers":
    let u = parseUrl("https://example.com/chat")
    let hs = h2ConnectHeaderList(u, "websocket",
      initHeaders({"sec-websocket-version": "13"}))
    check hs[0] == (":method", "CONNECT")     # method + protocol lead the block
    check hs[1] == (":protocol", "websocket")
    check (":scheme", "https") in hs
    check (":path", "/chat") in hs            # :path kept, unlike a plain CONNECT
    check (":authority", "example.com") in hs
    check ("sec-websocket-version", "13") in hs

  test "h2ConnectHeaderList should drop connection-specific headers":
    let u = parseUrl("https://example.com/chat")
    let hs = h2ConnectHeaderList(u, "websocket",
      initHeaders({"connection": "upgrade", "upgrade": "websocket",
                   "x-app": "1"}))
    check ("connection", "upgrade") notin hs  # h1 Upgrade machinery has no place in h2
    check ("upgrade", "websocket") notin hs
    check ("x-app", "1") in hs                # ordinary headers pass through

suite "ws-over-h2 tunnel receive-window backpressure (#407)":
  ## The sync WebSocket-over-h2 driver (src/navi/private/websocket.nim) puts its
  ## Extended CONNECT stream in sink mode at open and acks the bytes the application
  ## consumes, exactly as the async tunnel does through the mux. Without that, `feed`
  ## replenishes the stream window per DATA frame, so a peer flooding the tunnel while
  ## `h2Send` spins on the send window grows the driver's read buffer without bound.
  ## These drive the same H2Conn calls the driver makes, sans-io (the driver itself
  ## needs a TLS + ALPN socket, so it is exercised end to end by the h2 interop test).

  proc advertisedStreamWindow(c: H2Conn): int =
    ## The per-stream receive window the client advertises in its preface SETTINGS,
    ## read off the wire so the flood below never hardcodes conn.nim's constant.
    var d: FrameDecoder
    let pre = c.preamble()
    d.feed(pre[connectionPreface.len ..< pre.len])
    var f: Frame
    while d.next(f):
      if f.typ == uint8(ftSettings):
        var i = 0
        while i + 6 <= f.payload.len:
          let id = (uint16(ord(f.payload[i])) shl 8) or uint16(ord(f.payload[i + 1]))
          if id == settingsInitialWindowSize: return int(readU32(f.payload, i + 2))
          i += 6

  proc streamWindowCredit(wire: string, sid: uint32): int =
    ## Total WINDOW_UPDATE increment the client handed back on `sid` (0 = the peer
    ## got no room to keep flooding).
    var d: FrameDecoder
    d.feed(wire)
    var f: Frame
    while d.next(f):
      if f.typ == uint8(ftWindowUpdate) and f.streamId == sid and f.payload.len == 4:
        result += int(readU32(f.payload, 0) and 0x7fffffff'u32)

  proc openTunnel(sink: bool): (H2Conn, uint32) =
    ## A connection whose Extended CONNECT stream has been accepted with a 2xx, set
    ## up like `websocketH2`: open the stream, gate its receive window (`sink`), send
    ## the CONNECT header block with the send side left open.
    let c = initH2Conn()
    discard c.preamble()
    discard c.feed(encodeSettings({settingsEnableConnectProtocol: 1'u32}))
    # openTunnelStream is what `websocketH2` opens the tunnel with; `sink = false`
    # is the plain stream the driver used before #407.
    let sid = if sink: c.openTunnelStream() else: c.openStream()
    discard c.encodeRequestHead(sid,
      h2ConnectHeaderList(parseUrl("https://example.com/chat"), "websocket",
                          initHeaders()))
    let enc = HpackEncoder()
    discard c.feed(encodeHeaders(sid, enc.encode(@[(":status", "200")]),
                                 endStream = false, endHeaders = true))
    (c, sid)

  proc floodWhileBlocked(c: H2Conn, sid: uint32,
                         bytes: int): tuple[ctrl: string, buffered: int] =
    ## Feed `bytes` of tunnel DATA in 16 KiB frames, draining each into the driver's
    ## read buffer the way `pumpH2` does (takeBody -> inbuf) and acking nothing: the
    ## state the driver is in while a send waits for the peer's WINDOW_UPDATE.
    let payload = repeat('x', 16384)
    var sent = 0
    while sent < bytes:
      result.ctrl.add c.feed(encodeData(sid, payload, endStream = false))
      result.buffered += c.takeBody(sid).len
      sent += payload.len

  test "a flood while a send is blocked should not replenish the tunnel stream window":
    let (c, sid) = openTunnel(sink = true)
    let win = advertisedStreamWindow(c)
    let flood = floodWhileBlocked(c, sid, win)
    check flood.buffered == win                     # the window, and not a byte more
    check streamWindowCredit(flood.ctrl, sid) == 0  # no credit while nothing is consumed
    check c.connError.len == 0
    check not c.streamReset(sid)
    # The peer is now out of stream window: a compliant one stops here, and one that
    # keeps flooding is cut off instead of growing the buffer (RFC 9113 6.9.1).
    discard c.feed(encodeData(sid, "x", endStream = false))
    check c.streamReset(sid)

  test "an eagerly replenished tunnel lets the same flood grow without bound (control)":
    # The pre-fix sync driver: no sink mode, so `feed` hands the credit straight back
    # and the peer can put twice the window (and then some) into the read buffer.
    let (c, sid) = openTunnel(sink = false)
    let win = advertisedStreamWindow(c)
    let flood = floodWhileBlocked(c, sid, win * 2)
    check flood.buffered == win * 2
    check streamWindowCredit(flood.ctrl, sid) > 0
    check not c.streamReset(sid)

  test "acking the consumed bytes should credit the peer exactly once and resume it":
    let (c, sid) = openTunnel(sink = true)
    let win = advertisedStreamWindow(c)
    let flood = floodWhileBlocked(c, sid, win)
    check streamWindowCredit(flood.ctrl, sid) == 0
    # The application calls receive(): the driver hands the buffered bytes over and
    # acks exactly what it consumed, in the chunks it consumed them in.
    var acked = ""
    var left = flood.buffered
    while left > 0:
      let n = min(64 * 1024, left)
      acked.add c.ackRecv(sid, n)
      left -= n
    check streamWindowCredit(acked, sid) == win     # every consumed byte, credited once
    # ... and with the window reopened the peer can flood the same amount again.
    let more = floodWhileBlocked(c, sid, win)
    check more.buffered == win
    check streamWindowCredit(more.ctrl, sid) == 0
    check not c.streamReset(sid)
    check c.connError.len == 0

suite "ws-over-h2 tunnel is not a response body (#470)":
  ## The sync WebSocket-over-h2 driver opens its dedicated connection with
  ## `initH2Conn(0)`: `maxResponseBytes` caps a response body, and every DATA frame
  ## on the tunnel stream counts toward the body total, so a capped connection RSTs
  ## the tunnel once it has carried that many frame bytes in all. These drive the
  ## same H2Conn calls the driver makes, sans-io; the driver itself needs TLS + ALPN
  ## and is exercised end to end (with `maxResponseBytes` set) by tests/interop/ws_h2.

  proc acceptedTunnel(maxBody: int): (H2Conn, uint32) =
    let c = initH2Conn(maxBody)
    discard c.preamble()
    discard c.feed(encodeSettings({settingsEnableConnectProtocol: 1'u32}))
    let sid = c.openTunnelStream()
    discard c.encodeRequestHead(sid,
      h2ConnectHeaderList(parseUrl("https://example.com/chat"), "websocket",
                          initHeaders()))
    let enc = HpackEncoder()
    discard c.feed(encodeHeaders(sid, enc.encode(@[(":status", "200")]),
                                 endStream = false, endHeaders = true))
    (c, sid)

  proc carry(c: H2Conn, sid: uint32, bytes: int): int =
    ## Feed `bytes` of tunnel DATA in 1 KiB frames, consuming and acking each the way
    ## `h2Recv` does, and return how many bytes the tunnel delivered.
    let payload = repeat('w', 1024)
    var sent = 0
    while sent < bytes and not c.streamReset(sid):
      discard c.feed(encodeData(sid, payload, endStream = false))
      let got = c.takeBody(sid)
      result += got.len
      if got.len > 0: discard c.ackRecv(sid, got.len)
      sent += payload.len

  test "a cap-0 connection carries tunnel bytes far past any response cap":
    let (c, sid) = acceptedTunnel(0)
    check c.carry(sid, 8 * 1024 * 1024) == 8 * 1024 * 1024   # 8 MiB, acked as read
    check not c.streamReset(sid)
    check not c.streamTooLarge(sid)
    check c.connError.len == 0

  test "a connection opened with maxResponseBytes resets the tunnel at the cap (control)":
    let (c, sid) = acceptedTunnel(1024)
    check c.carry(sid, 4 * 1024) < 4 * 1024   # cut off part way (the RST lands at
                                              # the first frame past the cap)
    check c.streamReset(sid)
    check c.streamTooLarge(sid)
