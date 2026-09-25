## Shared SSE reconnect-delay spec for the async backends, instantiated by
## test_sse_retry_async.nim and test_sse_retry_chronos.nim: each imports its
## backend, defines `sseBackendName`, then `include`s this. A test added here runs
## under BOTH backends (in particular chronos's gcsafe / strict-raises checks).
## Not named `t*`/`test_*`, so the runner does not compile it standalone.
##
## Each test's logic lives in a local `run(): Future[...] {.async.}` proc so its
## accumulators are proc locals (captured into the async env, gcsafe) rather than
## module globals, mirroring sink_spec.nim.

proc elapsedMs(t0: MonoTime): int = (getMonoTime() - t0).inMilliseconds.int

suite "SSE reconnect floor (" & sseBackendName & ", #291)":
  test "a server's retry: 0 should be floored, not reconnected instantly":
    # Every connection carries `retry: 0` plus one event, then closes. The stream
    # must still wait out the floor before each reconnect: three events means two
    # reconnects, so at least 2 x 120 ms of sleeping.
    var th: Thread[SseFlapSrv]
    var port, conns: int
    var cfg = SseFlapSrv(conns: addr conns, emptyConns: 0, total: 3,
                         preamble: "retry: 0\ndata: tick\n\n")
    startSseFlap(th, port, cfg)
    proc run(): Future[(seq[string], int)] {.async.} =
      let api = newNavi()
      let s = await api.sse("http://127.0.0.1:" & $port & "/events",
                            retryMs = 20, maxRetryMs = 5000, minRetryMs = 120)
      let t0 = getMonoTime()
      var got: seq[string]
      for _ in 0 ..< 3:
        let ev = await s.next()
        if ev.isSome: got.add ev.get.data
      let took = elapsedMs(t0)
      await s.close()
      return (got, took)
    let (got, took) = waitFor run()
    check got == @["tick", "tick", "tick"]
    check conns == 3                       # one connection per event
    check took >= 200                      # two floored reconnects, not sleep(0)
    drainSseFlap(port, 3, addr conns)
    joinThread(th)

  test "repeated zero-event closes should back off, and an event should reset it":
    # The first four connections answer 200 and close with nothing in them, so the
    # delay doubles per attempt: 40, 80, 160, 320 ms before the fifth connection
    # finally delivers. Delivery then resets the delay to the 20 ms base, so the
    # next event arrives promptly instead of after another 640 ms.
    var th: Thread[SseFlapSrv]
    var port, conns: int
    var cfg = SseFlapSrv(conns: addr conns, emptyConns: 4, total: 6,
                         event: "data: done\n\n")
    startSseFlap(th, port, cfg)
    proc run(): Future[(string, string, int, int)] {.async.} =
      let api = newNavi()
      let s = await api.sse("http://127.0.0.1:" & $port & "/events",
                            retryMs = 20, maxRetryMs = 1000, minRetryMs = 20)
      let t0 = getMonoTime()
      let first = await s.next()
      let backedOff = elapsedMs(t0)
      let t1 = getMonoTime()
      let second = await s.next()
      let afterReset = elapsedMs(t1)
      await s.close()
      let d1 = if first.isSome: first.get.data else: ""
      let d2 = if second.isSome: second.get.data else: ""
      return (d1, d2, backedOff, afterReset)
    let (first, second, backedOff, afterReset) = waitFor run()
    check first == "done"
    check second == "done"
    check conns == 6
    check backedOff >= 450                 # 40+80+160+320, not 4 x 20 ms
    check afterReset < 400                 # reset to the base, not still backing off
    drainSseFlap(port, 6, addr conns)
    joinThread(th)
