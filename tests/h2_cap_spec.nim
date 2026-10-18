## Shared spec for the h2 mux's PER-STREAM response size cap (#466), instantiated by
## test_h2_cap.nim (asyncdispatch) and test_h2_cap_chronos.nim. Each imports its own
## backend, defines `h2BackendName`, `h2CapBasePort`, `connectCapMux` and `napMs`,
## then `include`s this. A test added here runs under BOTH backends. Not named
## `t*`/`test_*`, so the runner does not compile it alone.
##
## Why at the mux level and over plain TCP: `maxResponseBytes` used to be a property
## of the shared CONNECTION (the `maxBody` the mux was built with), set by whichever
## request happened to open it. It is live configuration a caller may change between
## requests, and an `sse()` stream runs with it off, so the cap belongs to the stream.
## These drive `H2Mux` directly so the two roles ("the uncapped stream" and "the
## capped request") are explicit and no TLS/ALPN is needed.
##
## The read bound is NOT per stream: it is the transport's, one per connection, and
## its expiry retires the whole connection. See h2_readbound_spec.nim.

proc reqHeaders(path: string): seq[(string, string)] =
  @[(":method", "GET"), (":scheme", "http"), (":authority", "127.0.0.1"),
    (":path", path)]

proc tunnelHeaders(): seq[(string, string)] =
  ## An Extended CONNECT (RFC 8441) header block, as `doWebsocketH2` sends it.
  @[(":method", "CONNECT"), (":protocol", "websocket"), (":scheme", "http"),
    (":authority", "127.0.0.1"), (":path", "/chat")]

proc cappedRequest(mux: H2Mux, path: string, cap: int): Future[string] {.async.} =
  ## Run a buffered request with its own cap and report what it failed with ("" when
  ## it completed).
  try:
    discard await mux.request(reqHeaders(path), "", cap = cap)
  except CatchableError as e:
    return $e.name & ": " & e.msg
  return ""

proc collect(mux: H2Mux, sid: uint32): Future[seq[string]] {.async.} =
  ## Drain a sink stream to its end, collecting the decoded chunks.
  while true:
    let c = await mux.readChunk(sid)
    if c.len == 0: break
    result.add c

proc drainTunnel(mux: H2Mux): Future[(string, string)] {.async.} =
  ## Open the Extended CONNECT tunnel and read it to its end: (bytes, failure).
  var got, failed: string
  try:
    let sid = await mux.openConnect(tunnelHeaders())
    while true:
      let c = await mux.tunnelRecv(sid)
      if c.len == 0: break
      got.add c
  except CatchableError as e:
    failed = $e.name & ": " & e.msg
  await mux.close()
  return (got, failed)

const
  # gzip -n of 64 KiB of zeros: 96 bytes on the wire, 65536 decoded (a small bomb).
  gzipZeros = parseHexStr("1f8b0800000000000213edc101010000008090feafee080a000000000000000000000000" &
    "000000000000000000000000000000000000000000000000000000000000000000000000" &
    "0000000000000000000000000000006aeb8e97d700000100")
const
  evOne = "data: one\nid: 1\n\n"
  evTwo = "data: two\nid: 2\n\n"

suite "http/2 per-stream size cap (" & h2BackendName & ", #466)":
  test "an uncapped stream and a capped request share a connection opened with the cap":
    # maxResponseBytes is the REQUESTER's, not the connection's. The connection is
    # built with a cap (what `sharedConnCap` hands a fresh mux), yet a stream that
    # asks for no cap must deliver a body many times that size, while a request on
    # the same connection that asks for the cap still trips it.
    var th: Thread[CapPeerArg]
    let port = h2CapBasePort + 0
    startCapPeer(th, CapPeerArg(port: port, script: "qf", quietMs: 80,
                                body: "okokokokok", events: @[evOne, evTwo]))
    proc run(): Future[(seq[string], string)] {.async.} =
      let mux = await connectCapMux(port, maxBody = 5)
      let sid = await mux.sendAndReadHeaders(reqHeaders("/events"), "", cap = 0)
      let got = await mux.collect(sid)
      let outcome = await mux.cappedRequest("/body", cap = 5)
      await mux.close()
      return (got, outcome)
    let (got, outcome) = waitFor run()
    check got == @[evOne, evTwo]              # 36 bytes through a connection capped at 5
    check outcome.startsWith("ResponseTooLargeError")
    joinThread(th)

  test "a request's cap holds on a connection opened without one":
    # The mirror: the connection's default cap is 0 (what an `sse()` stream's view
    # would open it with). A request must still be capped at its own
    # `maxResponseBytes` -- a connection-wide cap of 0 would let the body through.
    var th: Thread[CapPeerArg]
    let port = h2CapBasePort + 1
    startCapPeer(th, CapPeerArg(port: port, script: "f", quietMs: 0,
                                body: "okokokokok", events: @[]))
    proc run(): Future[string] {.async.} =
      let mux = await connectCapMux(port, maxBody = 0)
      let outcome = await mux.cappedRequest("/body", cap = 5)
      await mux.close()
      return outcome
    let outcome = waitFor run()
    check outcome.startsWith("ResponseTooLargeError")
    joinThread(th)

  test "a WebSocket tunnel on the cap-0 mux carries more than maxResponseBytes (#470)":
    # `doWebsocketH2` dials a dedicated mux for its Extended CONNECT tunnel. It used
    # to build it with `config.maxResponseBytes`, and since the tunnel stream
    # registers no cap of its own it inherited that one, so the stream was RST once
    # its frames passed the cap in total. It now builds the mux with cap 0, which
    # must let a tunnel carry any number of bytes; `maxMessageBytes` is the bound.
    var th: Thread[CapPeerArg]
    let port = h2CapBasePort + 2
    let frames = repeat("ws-frame-bytes|", 300)         # 4500 bytes, > 4x the cap
    startCapPeer(th, CapPeerArg(port: port, script: "t", body: frames))
    proc run(): Future[(string, string)] {.async.} =
      let mux = await connectCapMux(port, tunnel = true)   # doWebsocketH2's mux
      var got, failed: string
      try:
        let sid = await mux.openConnect(tunnelHeaders())
        while true:
          let c = await mux.tunnelRecv(sid)
          if c.len == 0: break
          got.add c
      except CatchableError as e:
        failed = $e.name & ": " & e.msg
      await mux.close()
      return (got, failed)
    let (got, failed) = waitFor run()
    check failed == ""
    check got == frames
    joinThread(th)

  test "the same tunnel on a mux opened with the cap is reset (the #470 failure)":
    # The control: the tunnel stream inherits a mux's connection-wide default, so the
    # pre-fix construction (`newH2Mux(conn, maxResponseBytes)`) RSTs it mid-stream.
    var th: Thread[CapPeerArg]
    let port = h2CapBasePort + 3
    let frames = repeat("ws-frame-bytes|", 300)
    startCapPeer(th, CapPeerArg(port: port, script: "t", body: frames))
    proc run(): Future[(int, string)] {.async.} =
      let mux = await connectCapMux(port, maxBody = 1024)
      var got = 0
      var failed: string
      try:
        let sid = await mux.openConnect(tunnelHeaders())
        while true:
          let c = await mux.tunnelRecv(sid)
          if c.len == 0: break
          got += c.len
      except CatchableError as e:
        failed = $e.name & ": " & e.msg
      await mux.close()
      return (got, failed)
    let (got, failed) = waitFor run()
    check failed.startsWith("ResponseTooLargeError")
    check got <= 1024
    joinThread(th)

  test "a tunnel whose CONNECT 2xx says content-encoding: gzip is not decoded (#470)":
    # A CONNECT 2xx has no content (RFC 9110 9.3.6), so its `content-encoding` must
    # not turn the tunnel into a decoder. On the cap-0 tunnel mux that decoder would
    # also be unbounded: a decompression bomb materialized before the ws framing
    # layer (and `maxMessageBytes`) sees a byte. The tunnel mux never decodes, so
    # the gzip bytes arrive verbatim, as on the sync, h1 and h3 legs.
    var th: Thread[CapPeerArg]
    let port = h2CapBasePort + 4
    startCapPeer(th, CapPeerArg(port: port, script: "t", body: gzipZeros,
                                encoding: "gzip"))
    proc run(): Future[(string, string)] {.async.} =
      let mux = await connectCapMux(port, tunnel = true)   # doWebsocketH2's mux
      return await mux.drainTunnel()
    let (got, failed) = waitFor run()
    check failed == ""
    check got == gzipZeros                     # 96 raw bytes, not 64 KiB of zeros
    joinThread(th)

  test "the same tunnel on a decompressing mux is decoded (the #470 review failure)":
    # The control: the construction #470 first shipped, `newH2Mux(conn, 0,
    # wantsDecompress)`, runs the tunnel bytes through an uncapped decoder.
    var th: Thread[CapPeerArg]
    let port = h2CapBasePort + 5
    startCapPeer(th, CapPeerArg(port: port, script: "t", body: gzipZeros,
                                encoding: "gzip"))
    proc run(): Future[(string, string)] {.async.} =
      let mux = await connectCapMux(port, maxBody = 0, decompress = true)
      return await mux.drainTunnel()
    let (got, failed) = waitFor run()
    check failed == ""
    check got == repeat('\0', 65536)          # 96 bytes in, 64 KiB out
    joinThread(th)
