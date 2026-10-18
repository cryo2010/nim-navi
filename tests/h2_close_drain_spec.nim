## A complete h2 response whose body is already queued must survive the server closing
## the connection right after sending it (#461 round 2). The drain budget's yield lets
## the mux reader see the EOF while a puller is still working through the queue; the
## puller must still get every queued byte, and fail only for a stream that did not
## end cleanly, once its queue is empty.
##
## Included by `test_h2_close_drain` (asyncdispatch) and `test_h2_close_drain_chronos`,
## which define `connectMux`, `napMs`, `backendName` and `basePort`. Adapted from the
## #461 review's repro.
import std/[monotimes, times, strutils]

const bodyBytes = 2_000_000

proc spin(us: int) =
  let t = getMonoTime()
  while (getMonoTime() - t).inMicroseconds < us: discard

proc closeHeaders(): seq[(string, string)] =
  @[(":method", "GET"), (":scheme", "http"), (":authority", "127.0.0.1"), (":path", "/x")]

proc runCase(port: int, goaway: bool, spinUs: int, awaitMs = 0,
             endStream = true): (int, string) =
  var th: Thread[ClosePeerArg]
  createThread(th, runClosePeer, (port, bodyBytes, 150, goaway, endStream))
  proc run(): Future[(int, string)] {.async.} =
    let mux = await connectMux(port)
    let sid = await mux.sendAndReadHeaders(closeHeaders(), "")
    await napMs(60)        # whole body queued, peer still open (closes at ~150 ms)
    var got = 0
    var err = ""
    try:
      while true:
        let c = await mux.readChunk(sid)
        if c.len == 0: break
        got += c.len
        spin(spinUs)        # a CPU-bound consumer (parse/hash), no await
        if awaitMs > 0: await napMs(awaitMs)
    except CatchableError as e:
      err = e.msg
    try: await mux.close() except CatchableError: discard
    return (got, err)
  result = waitFor run()
  joinThread(th)

suite "h2 complete response vs server close (" & backendName & ")":
  test "EOF after the full body, CPU-bound consumer":
    let (got, err) = runCase(basePort, false, 2000)
    checkpoint "got=" & $got & " err=" & err
    check got == bodyBytes
    check err == ""
  test "GOAWAY + EOF after the full body, CPU-bound consumer":
    let (got, err) = runCase(basePort + 1, true, 2000)
    checkpoint "got=" & $got & " err=" & err
    check got == bodyBytes
    check err == ""
  test "EOF after the full body, consumer that awaits between chunks":
    let (got, err) = runCase(basePort + 2, false, 0, 3)
    checkpoint "got=" & $got & " err=" & err
    check got == bodyBytes
    check err == ""
  test "EOF with no END_STREAM: every queued byte, then the connection error":
    # A tunnel peer's last frames (a WebSocket Close) must reach the consumer before
    # the connection death does.
    let (got, err) = runCase(basePort + 3, false, 0, 3, endStream = false)
    checkpoint "got=" & $got & " err=" & err
    check got == bodyBytes
    check err.startsWith("navi: http/2 connection closed")  # asyncdispatch appends
                                                            # the async traceback
