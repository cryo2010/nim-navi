## Regression test for issue #468: on the asyncdispatch backend a FINISHED exchange
## must not stay reachable from the timer that bounded it.
##
## `guard` bounds a request with a `sleepAsync(totalMs)` timer, and std/asyncdispatch
## cannot take a timer off the dispatcher's heap again, so everything that timer still
## reaches is live memory (reachable, not cyclic garbage: no `GC_fullCollect` can
## reclaim it) until it fires. Before the fix that was, per request, the `or`
## combinator's future and closures plus the timer itself, kept for the whole
## `timeouts.total` -- a heap that grew with request rate x `timeouts.total` (610 MB
## at 12k req/s with a 60 s total in the issue's soak), and on the streaming path one
## such set per 64 KiB chunk read, since every `readChunk` is guarded by what is left
## of the deadline.
##
## Two things are checked after each loop has been given a moment to settle, both
## under a 10-minute total timeout that nothing in the test could make expire:
##
##   * the dispatcher's own timer heap (`getGlobalDispatcher().timers`) must have
##     drained to nothing (`timerFloor` is slack, not an expectation: every build
##     measured here lands on exactly 0). A parked entry per finished exchange IS the
##     defect, and it is what each retained `or` race hangs off. Before the fix this
##     heap held 802 entries after the 400 requests and 1205 after the chunk reads on
##     top of them, in every build; after it, 0. The count is exact, so unlike bytes
##     it needs no allocator accounting and no per-build scaling, and it is checked
##     under `-d:useMalloc` too.
##   * on the request path, the settled heap growth must be within a few hundred
##     bytes per request of the SAME loop run against a client with no total timeout
##     at all (the control, which arms no `guard` timer). The control runs first in
##     the same process, so every one-off allocation the request path makes is
##     already paid for when the bounded run is measured against it. An absolute
##     bound could not do this job: the per-request figure moves ~3x between builds,
##     because a release build captures no `newFuture` stack traces, and the broken
##     figures of one build sit inside the healthy figures of another. Measured here,
##     per request over 400 requests (arc within a few per cent of orc):
##
##       | build   | no total timeout | before #468 | after #468 |
##       | ---     | ---              | ---         | ---        |
##       | debug   | 177 B            | 2148 B      | 226 B      |
##       | release | 176 B            | 707 B       | 226 B      |
##
##     Part of every figure is a fixed one-off (400 unbounded requests retain ~70 KB
##     in total whether 400 or 1600 of them run), which is what `absSlack` is for;
##     the rest is per request, and `reqSlack` sits ~3x above the fixed delta and
##     ~3x below the broken one in the tightest build.
##
## The streaming loop reports its bytes but does not bound them: at ~400 guarded
## reads a single 64 KiB socket buffer landing on one side of a snapshot moves the
## figure by ~170 B per read, which is as large as the whole per-read signal in a
## release build. Its timer-heap count is exact, so that is what it asserts.

import unittest
import std/[asyncdispatch, heapqueue]
import navi/asyncdispatch
import ./support

const
  chunkBytes = 64 * 1024
  reqCount = 400
  longTotalMs = 600_000     # 10 minutes: no bound can expire during the test
  reqSlack = 140            # bytes a bounded request may retain over the control
  absSlack = 8 * 1024       # one-off noise floor for a whole measured window
  timerFloor = 8            # slack on the parked-timer count; a fixed tree hits 0
  settleMs = 1500           # time for the last spent timer slice to expire

type Residue = object
  bytes: int                ## settled heap growth over the measured window
  timers: int               ## entries still parked in the dispatcher's timer heap

proc memAccounted(): bool =
  ## Whether `getOccupiedMem()` can see anything at all. It only reports Nim's OWN
  ## allocator: under `-d:useMalloc` (the ASan/UBSan CI job) every allocation goes to
  ## libc malloc and the counter never leaves 0, so a byte bound would hold just as
  ## well on a BROKEN tree. The timer-heap check below is unaffected and still runs
  ## there; only the byte bound is reported as skipped rather than banked as a pass.
  when defined(useMalloc): false
  else: getOccupiedMem() > 0

proc settledMem(): int =
  ## Occupied heap once the dead future/closure chains are gone. Collect twice: the
  ## first pass frees the acyclic part, and only then can the cycle collector see
  ## the rest (a future and its continuation's env reference each other until
  ## completion nils the callback list).
  GC_fullCollect()
  GC_fullCollect()
  getOccupiedMem()

proc settledResidue(before: int): Future[Residue] {.async.} =
  ## What the loop left behind, after giving every spent timer slice time to fire.
  ## The wait is unconditional rather than "poll until it looks good", so the control
  ## run and the bounded run are measured with exactly the same yardstick. It is
  ## needed at all because the fix does not (and on asyncdispatch cannot) remove a
  ## spent timer from the dispatcher's heap: it caps how long one lives at
  ## `naviTimerSliceMs`, re-arming longer bounds in slices. So a fixed build settles
  ## about a second after the last request, while a broken one holds on for the full
  ## `timeouts.total` and this wait changes nothing for it.
  var waited = 0
  while waited < settleMs:
    await sleepAsync(250)
    waited += 250
  result = Residue(bytes: settledMem() - before,
                   timers: getGlobalDispatcher().timers.len)

proc clientWithTotal(totalMs: int): Navi =
  var cfg = initNaviConfig()
  cfg.timeouts.total = totalMs
  newNavi(cfg)

suite "asyncdispatch guard retention":
  test "400 completed requests must not stay reachable from a 10-minute total timer (#468)":
    var portFree, portBound = 0
    var thFree, thBound: Thread[FixedBodyCtx]
    startFixedBody(thFree, portFree, responses = reqCount + 1, bodyBytes = chunkBytes)
    startFixedBody(thBound, portBound, responses = reqCount + 1, bodyBytes = chunkBytes)

    var free, bound: Residue
    var allFull = true
    proc drive(api: Navi, port: int): Future[Residue] {.async.} =
      let url = "http://127.0.0.1:" & $port & "/"
      if (await api.get(url)).body.len != chunkBytes:   # warm up: conn, buffers, pool
        allFull = false
      let before = settledMem()
      for _ in 0 ..< reqCount:
        let res = await api.get(url)
        if res.body.len != chunkBytes: allFull = false
      result = await settledResidue(before)

    proc run(): Future[void] {.async.} =
      free = await drive(clientWithTotal(0), portFree)
      bound = await drive(clientWithTotal(longTotalMs), portBound)
    waitFor run()
    joinThread(thFree)
    joinThread(thBound)

    check allFull                               # every response was read in full
    checkpoint "left " & $bound.timers & " timer entries parked after " & $reqCount &
               " requests under a " & $(longTotalMs div 1000) & " s total (" &
               $free.timers & " with no total timeout)"
    check bound.timers <= timerFloor
    checkpoint "retained " & $(bound.bytes div reqCount) & " B per request under the " &
               $(longTotalMs div 1000) & " s total, vs " & $(free.bytes div reqCount) &
               " B with no total timeout (allowed " & $reqSlack & " B more each)"
    if memAccounted():
      check bound.bytes < free.bytes + reqCount * reqSlack + absSlack
    else:
      echo "  note: -d:useMalloc leaves getOccupiedMem() at 0, so the byte bound " &
           "cannot be measured here; the timer-heap check above still ran"
      skip()

  test "a long-total stream must not retain every chunk read's guard timer (#468)":
    # The streaming path guards EVERY `readChunk` by what is left of the total
    # deadline (impl_stream), so a long `timeouts.total` used to leave one live timer
    # per chunk read: memory proportional to everything downloaded in the window. The
    # control has `total = 0`, which makes `readChunk` skip the guard entirely, so it
    # should park no timer at all.
    var portFree, portBound = 0
    var thFree, thBound: Thread[FixedBodyCtx]
    startBigBody(thFree, portFree, pieces = reqCount, bodyBytes = chunkBytes)
    startBigBody(thBound, portBound, pieces = reqCount, bodyBytes = chunkBytes)

    var free, bound: Residue
    var freeReads, boundReads: int
    var allWhole = true
    proc drive(api: Navi, port: int, reads: ptr int): Future[Residue] {.async.} =
      var before = 0
      proc pump(): Future[void] {.async.} =
        # The handle lives only in here, so it is released before the measurement
        # below and cannot colour one run and not the other. What is under test sits
        # in the dispatcher's timer heap, which dropping the handle does not touch.
        let res = await api.stream.get("http://127.0.0.1:" & $port & "/")
        var got = (await res.readChunk()).len   # first chunk: warm the read buffers
        before = settledMem()
        while true:
          let c = await res.readChunk()
          if c.len == 0: break
          got += c.len
          inc reads[]
        if got != reqCount * chunkBytes: allWhole = false
      await pump()
      result = await settledResidue(before)

    proc run(): Future[void] {.async.} =
      free = await drive(clientWithTotal(0), portFree, addr freeReads)
      bound = await drive(clientWithTotal(longTotalMs), portBound, addr boundReads)
    waitFor run()
    joinThread(thFree)
    joinThread(thBound)

    check allWhole                              # both bodies were streamed in full
    check boundReads > reqCount div 2           # ...in many reads, each one guarded
    check freeReads > reqCount div 2
    # Reported, not bounded: see the note in this file's header.
    checkpoint "retained " & $(bound.bytes div max(boundReads, 1)) & " B per chunk " &
               "read over " & $boundReads & " bounded reads, vs " &
               $(free.bytes div max(freeReads, 1)) & " B over " & $freeReads &
               " unbounded reads"
    checkpoint "left " & $bound.timers & " timer entries parked after " & $boundReads &
               " guarded chunk reads (" & $free.timers & " over " & $freeReads &
               " unguarded ones)"
    check bound.timers <= timerFloor
