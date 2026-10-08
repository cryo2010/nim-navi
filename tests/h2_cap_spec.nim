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
