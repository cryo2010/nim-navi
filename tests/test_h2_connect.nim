## HTTP/2 Extended CONNECT (RFC 8441) foundation (#190): the peer-capability
## setting and the CONNECT + :protocol pseudo-header block for WebSocket-over-h2.

import unittest
import navi/proto/h2/[conn, frame]
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
