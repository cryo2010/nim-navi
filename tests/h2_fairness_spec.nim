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
##
## Nothing here depends on how fast the peer thread answers. The upload is parked on
## its first 64 KiB window before the drain starts, and the peer's grant for the rest
## is already sitting unread in the client's socket (the test blocks its own thread
## while the peer writes it). So the upload moves on exactly when the event loop
## gives the mux reader a turn, and the test asserts the two things the budget
## guarantees: the number of turns the drain gives, and that the upload moved within
## the first few of them, long before the drain's first window ack (at 4 MiB, the
## only place the pre-fix drain could turn the loop).

import std/[os, atomics, monotimes, times]

const
  fairDownloadBytes = 8_000_000   # inside the client's 8 MiB stream window
  fairUploadBytes = 1_048_576
  fairUploadChunk = 16_384
  fairTurnBytes = 65_536          # the mux's turn budget (one read buffer)
  fairEarlyBytes = 1_048_576      # "early in the drain": 16 turns, a quarter of
                                  # the way to the first window ack at 4 MiB

type FairProgress = ref object
  pulled: int                     # upload bytes the producer has handed out
  draining: bool
  drained: int                    # download bytes the drain has pulled so far
  movedAt: int                    # `drained` when the upload first moved during the
                                  # drain (-1: it did not)
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

proc blockUntil(flag: var Atomic[bool]): bool =
  ## Wait for the peer thread WITHOUT giving the event loop a turn, so nothing on
  ## the client reads what the peer writes meanwhile.
  let giveUp = getMonoTime() + initDuration(seconds = 10)
  while not flag.load:
    if getMonoTime() > giveUp: return false
    os.sleep(1)
  true

suite "http/2 mux drain budget (" & h2BackendName & ", #461)":
  test "draining a queued download yields the loop, so an upload on the mux progresses":
    var th: Thread[FloodArg]
    startFlood(th, h2FairBasePort, fairDownloadBytes)
    proc run(): Future[(int, int, int, bool, int, int)] {.async.} =
      let mux = await connectFairMux(h2FairBasePort)
      let sid = await mux.sendAndReadHeaders(fairHeaders("GET", "/download"), "")
      let p = FairProgress(movedAt: -1)
      let producer = BodyProducer(proc(): string =
        if p.pulled >= fairUploadBytes: return ""
        if p.draining and p.movedAt < 0: p.movedAt = p.drained
        p.pulled += fairUploadChunk
        newString(fairUploadChunk))
      let upload = mux.request(fairHeaders("POST", "/upload"), "", bodyStream = producer)
      # The upload sends its first window and parks on the peer's; the body fits the
      # stream window, so the peer has sent all of it. Give the reader time to queue
      # it, so the drain below never waits on the reader.
      for _ in 0 ..< 1000:
        if floodUploadBlocked.load: break
        await napMs(10)
      await napMs(400)
      # The peer has the first window and holds its ack. Release the grant and wait
      # for it to be written with this thread blocked: the reader has not seen it.
      floodRelease.store(true)
      let granted = blockUntil(floodGranted)
      os.sleep(50)                     # loopback delivery into our receive buffer
      p.draining = true
      let ticker = fairTicker(p)
      while true:
        let c = await mux.readChunk(sid)
        if c.len == 0: break
        p.drained += c.len
      p.draining = false
      let ticksDuringDrain = p.ticks
      let resp = await upload
      await ticker
      await mux.close()
      return (p.drained, ticksDuringDrain, p.movedAt, granted, resp.status, p.pulled)
    let (got, ticks, movedAt, granted, status, pulled) = waitFor run()
    check granted
    check got == fairDownloadBytes
    # One turn per read buffer of queued body: ~122 for 8 MB. Before the budget the
    # drain took one or two (its 4 MiB window acks). The slack covers chunks that
    # straddle a budget boundary, never the pre-fix count.
    check ticks >= fairDownloadBytes div fairTurnBytes * 3 div 4
    # The first of those turns let the reader parse the grant and the upload pull
    # its next chunk while most of the download was still queued. Before the budget
    # it could not move until the drain's first window ack, 4 MiB in.
    check movedAt >= 0
    check movedAt < fairEarlyBytes
    check status == 200
    check pulled >= fairUploadBytes
    joinThread(th)
