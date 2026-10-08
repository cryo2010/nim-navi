## Shared spec for the h2 mux's CONNECTION-LEVEL read bound (#466), instantiated by
## test_h2_readbound.nim (asyncdispatch) and test_h2_readbound_chronos.nim. Each
## imports its own backend, defines `h2BackendName`, `h2BoundBasePort`,
## `connectBoundMux` and `napMs`, then `include`s this. Not named `t*`/`test_*`, so
## the runner does not compile it alone.
##
## `timeouts.read` is the TRANSPORT's per-read bound on a shared h2 connection, taken
## from the config that opened it: its expiry means the peer has gone dark, so the
## reader exits, `failAll` fails every in-flight stream with a REPLAYABLE class
## (keep-alive race / unprocessed) and the mux stops being reusable, so the router
## retires it and the next request reconnects and replays.
##
## This is the regression guard for an earlier attempt at #466 that made the bound
## per STREAM and left the socket read unbounded: a black-holed peer's mux then
## stayed `canReuse` forever and every later request failed with a NON-replayable
## `TimeoutError`. A per-stream bound also cannot see the difference between a peer
## that is slow on one stream and a peer that is gone, which is the distinction the
## replay classification is built on. SSE streams, which must run with no read bound
## at all, get their own connections instead (`sharedView` in impl_common).

proc reqHeaders(path: string): seq[(string, string)] =
  @[(":method", "GET"), (":scheme", "http"), (":authority", "127.0.0.1"),
    (":path", path)]

proc failureOf(mux: H2Mux, path: string): Future[string] {.async.} =
  ## The exception name a request on `mux` fails with ("" if it somehow completed).
  try:
    discard await mux.request(reqHeaders(path), "")
  except CatchableError as e:
    return $e.name
  return ""

suite "http/2 connection-level read bound (" & h2BackendName & ", #466)":
  test "a peer that goes dark fails the request replayably and retires the mux":
    # The peer completes the h2 preface and then never speaks again -- a black-holed
    # path, a hard-rebooted server: the socket stays open, so only a read bound can
    # notice. The keepalive is OFF here, so the read bound is the sole detector.
    var th: Thread[PeerArg]
    let port = h2BoundBasePort + 0
    startPeer(th, port, answerPings = false)
    proc run(): Future[(string, int, bool, string)] {.async.} =
      let mux = await connectBoundMux(port, readMs = 800)
      let t0 = epochTime()
      let outcome = await mux.failureOf("/slow")
      let tookMs = int((epochTime() - t0) * 1000.0)
      let reusable = mux.canReuse
      # A request dispatched after the death is rejected without being sent, which is
      # what makes the router open a fresh connection and replay instead of hanging.
      let second = await mux.failureOf("/slow")
      await mux.close()
      return (outcome, tookMs, reusable, second)
    let (outcome, tookMs, reusable, second) = waitFor run()
    # Replayable, NOT a TimeoutError: the whole connection died, and the request was
    # never answered, so the router may re-issue it on a fresh one.
    check outcome in ["KeepAliveRaceError", "UnprocessedError"]
    check tookMs >= 700                  # it waited out its bound
    check tookMs < 3000                  # ...and did not hang
    check not reusable                   # the mux was retired, not left in the table
    check second == "UnprocessedError"   # provably unsent on a dead connection
    joinThread(th)
