## Shared spec for the h2 mux's drain budget (#461), instantiated by
## test_h2_fairness.nim (asyncdispatch) and test_h2_fairness_chronos.nim. Each imports
## its own backend, defines `h2BackendName`, `h2FairBasePort`, `connectFairMux` and
## `napMs`, then `include`s this. Not named `t*`/`test_*`, so the runner does not
## compile it alone.
##
## An `await` on an already-finished future does not return to the event loop on
## either runtime, so a body puller whose stream queue is full used to drain all of
## it in one go: up to a whole 8 MiB receive window with nothing else on the loop
## getting a turn. In the mixed soak that starved a streamed upload next to a
## download to 1/40 of its solo rate, because the upload can only move one peer
## window per WINDOW_UPDATE its connection's reader gets to read. The mux now yields
## a turn every read buffer's worth of body it hands out without parking.
##
## The peer (support_h2flood) sends the download's whole body up front, so by the time
## the test pulls it, every chunk is already queued and `readChunk` never has to wait:
## the pre-fix drain ran without a single turn. Plain TCP keeps it deterministic and
## TLS-free; the reader's own budget (a TLS read that keeps finding bytes buffered)
## is exercised by the mixed stress cell, not here.

const
  fairDownloadBytes = 8_000_000   # inside the client's 8 MiB stream window
  fairUploadBytes = 1_048_576
  fairUploadChunk = 16_384
  fairPeerWindow = 65_535         # the peer's default stream/connection window

type FairProgress = ref object
  pulled: int                     # upload bytes the producer has handed out
  draining: bool
  ticks: int                      # event-loop turns the ticker saw

proc fairHeaders(meth, path: string): seq[(string, string)] =
  @[(":method", meth), (":scheme", "http"), (":authority", "127.0.0.1"),
    (":path", path)]

proc fairTicker(p: FairProgress) {.async.} =
  ## Counts event-loop turns while the download drains: each pass parks on a zero
  ## timer, so it only advances when something gives the loop a turn.
  while p.draining:
    await napMs(0)
    inc p.ticks

suite "http/2 mux drain budget (" & h2BackendName & ", #461)":
  test "draining a queued download yields the loop, so an upload on the mux progresses":
    var th: Thread[FloodArg]
    startFlood(th, h2FairBasePort, fairDownloadBytes)
    proc run(): Future[(int, int, int, int, int)] {.async.} =
      let mux = await connectFairMux(h2FairBasePort)
      let sid = await mux.sendAndReadHeaders(fairHeaders("GET", "/download"), "")
      # The body fits the stream window, so the peer sends all of it at once; give
      # the reader time to queue it, so the drain below never waits on the reader.
      await napMs(400)
      let p = FairProgress(draining: true)
      let producer = BodyProducer(proc(): string =
        if p.pulled >= fairUploadBytes: return ""
        p.pulled += fairUploadChunk
        newString(fairUploadChunk))
      let upload = mux.request(fairHeaders("POST", "/upload"), "", bodyStream = producer)
      let ticker = fairTicker(p)
      var got = 0
      while true:
        let c = await mux.readChunk(sid)
        if c.len == 0: break
        got += c.len
      p.draining = false
      let ticksDuringDrain = p.ticks
      let pulledDuringDrain = p.pulled
      let resp = await upload
      await ticker
      await mux.close()
      return (got, ticksDuringDrain, pulledDuringDrain, resp.status, p.pulled)
    let (got, ticks, pulledDuringDrain, status, pulled) = waitFor run()
    check got == fairDownloadBytes
    # ~120 read buffers' worth were queued; before the budget the drain took none
    # (or one or two, when a window ack happened to park on the socket). Leave wide
    # slack for a slow machine; the pre-fix count is far below it.
    check ticks >= 16
    # Those turns let the upload's connection reader see the peer's WINDOW_UPDATEs:
    # the upload got past its first window while the download was still draining.
    # Before the budget it was stuck at the first window until the drain finished.
    check pulledDuringDrain > fairPeerWindow + fairUploadChunk
    check status == 200
    check pulled >= fairUploadBytes
    joinThread(th)
