## Shared event-loop fairness spec for the async backends (#462), instantiated by
## test_fairness_async.nim and test_fairness_chronos.nim: each imports its backend
## and std/monotimes + std/times, defines `fairBackendName`, then `include`s this.
## Not named `t*`/`test_*`, so the runner does not compile it standalone.
##
## The bug: an await on an already-finished future resumes in place on both
## runtimes, so a TLS transfer whose socket stays readable (or writable) completed
## read after read without ever handing the loop back, and the buffered requests
## sharing that loop collapsed to ~1/50 of their solo rate in the mixed h1 stress
## cell. The connection layer now yields once per `naviFairSliceUs` of unbroken
## I/O. The first tests drive that check (`fairYield`) directly with a CPU-bound
## loop that never parks. The two transport tests go through the real `recvSome` and
## `sendAll` over plain TCP with a consumer/producer slower than loopback, so the
## socket stays readable/writable: on chronos those calls then complete in place
## (`fastRead`/`fastWrite`), and both tests fail with the yield removed from the
## entry point they use (or with `-d:naviFairSliceUs=2000000000`). asyncdispatch's
## plaintext `recv`/`send` always park on the selector, and its in-place TLS path
## needs an origin the unit suite does not have (no libcrypto on macOS), so there
## they pass with or without the yield: the asyncdispatch transport wiring is
## covered only by the mixed stress cell. The last test is the end-to-end contract
## over plain h1 (buffered requests on one `Navi` complete while a streamed download
## holds a pooled connection); its download sleeps per chunk, so it does not depend
## on the yield either.

import navi/backend/fairness   # sliceRestart (the write-order test)

const ioRunMs = 300       # how long each transport-level loop runs
const ioWorkUs = 500      # CPU per chunk: the consumer/producer is slower than
                          # loopback, so the socket stays readable / writable

proc spinUs(us: int) =
  ## Synchronous per-chunk work (a stand-in for TLS + SHA-1 on a real transfer).
  let t0 = getMonoTime()
  while (getMonoTime() - t0).inMicroseconds < us: discard

proc ioLoopLag(port: int, upload: bool): Future[(int, int64, int)] {.async.} =
  ## Drive one raw connection through `recvSome` (download) or `sendAll` (upload)
  ## for `ioRunMs` with no await other than the I/O call, next to a 1 ms ticker.
  ## Returns the ticks, the ticker's worst gap and the bytes moved.
  let c = await be.connect("127.0.0.1", port, false, TlsConfig(), ProxyTarget())
  var ticks = 0
  var worstGapMs = 0'i64
  var done = false
  proc ticker() {.async.} =
    var last = getMonoTime()
    while not done:
      await sleep(1)
      let now = getMonoTime()
      worstGapMs = max(worstGapMs, (now - last).inMilliseconds)
      last = now
      inc ticks
  var moved = 0
  if upload:
    await be.sendAll(c, "POST /sink HTTP/1.1\r\nHost: x\r\n\r\n")
  else:
    await be.sendAll(c, "GET /big HTTP/1.1\r\nHost: x\r\n\r\n")
    moved = (await be.recvSome(c)).len         # the flood has started
  let t = ticker()
  let blk = newString(64 * 1024)
  let stopAt = getMonoTime() + initDuration(milliseconds = ioRunMs)
  # The download stops short of the flood's end: past it the read would wait.
  while getMonoTime() < stopAt and (upload or moved < fairBigBytes):
    if upload:
      await be.sendAll(c, blk)
      moved += blk.len
    else:
      let d = await be.recvSome(c)
      if d.len == 0: break
      moved += d.len
    spinUs(ioWorkUs)
  done = true
  await t
  await be.close(c)
  return (ticks, worstGapMs, moved)

suite "event-loop fairness (" & fairBackendName & ")":
  test "a loop that never parks still lets a concurrent task run (#462)":
    proc run(): Future[(int, int64)] {.async.} =
      var ticks = 0
      var worstGapMs = 0'i64
      var done = false
      proc ticker() {.async.} =
        var last = getMonoTime()
        while not done:
          await sleep(1)
          let now = getMonoTime()
          worstGapMs = max(worstGapMs, (now - last).inMilliseconds)
          last = now
          inc ticks
      let t = ticker()
      # 300 ms of work that completes synchronously, checking in after every unit
      # like recvSome/sendAll do. Without the yield the ticker would not run once.
      let stopAt = getMonoTime() + initDuration(milliseconds = 300)
      var spin = 0'u64
      while getMonoTime() < stopAt:
        for i in 0 ..< 1000: spin = spin * 31 + uint64(i)
        await fairYield()
      done = true
      await t
      doAssert spin != 1     # keep the work observable
      return (ticks, worstGapMs)
    let (ticks, worstGapMs) = waitFor run()
    check ticks >= 20          # ~150 with a 2 ms slice; 0 without the yield
    check worstGapMs < 100     # one slice plus scheduling slop, never the whole run

  test "a lone loop that never parks still finishes (the yield cannot block)":
    # Nothing else is queued, so the yield's wake-up is the only pending work: a
    # yield primitive the poll timeout ignores (chronos `stepsAsync`) would block
    # in the selector forever here.
    proc run(): Future[int] {.async.} =
      var n = 0
      let stopAt = getMonoTime() + initDuration(milliseconds = 50)
      while getMonoTime() < stopAt:
        await fairYield()
        inc n
      return n
    check (waitFor run()) > 0

  test "a download whose socket stays readable lets a concurrent task run (#462)":
    # Through the real `recvSome`. On chronos the plaintext read completes in place
    # while the socket has bytes (4.4's `fastRead`), the shape a TLS download has
    # on both runtimes; asyncdispatch's plaintext `recv` always parks on the
    # selector, so there this only shows the yield costs nothing (TLS, where its
    # `SSL_read` completes in place, is not reachable from the unit suite).
    var srv: FairServer
    startFair(srv)
    let (ticks, worstGapMs, moved) = waitFor ioLoopLag(srv.port, upload = false)
    stopFair(srv)
    check moved > 0
    check ticks >= 20          # 0-2 on chronos without the yield
    check worstGapMs < 100     # the whole run without the yield

  test "an upload whose socket keeps draining lets a concurrent task run (#462)":
    # Through the real `sendAll`: chronos's `fastWrite` completes in place while the
    # send buffer has room, as SSL_write does on asyncdispatch over TLS.
    var srv: FairServer
    startFair(srv)
    let (ticks, worstGapMs, moved) = waitFor ioLoopLag(srv.port, upload = true)
    stopFair(srv)
    check moved > 0
    check ticks >= 20
    check worstGapMs < 100

  test "concurrent un-awaited sendAll calls on one connection write in call order":
    # A WebSocket over h1 sends its automatic pong / Close reply while a user send
    # may be in flight, neither awaiting the other. A write that parked on the
    # fairness yield BEFORE writing let a later call that found a fresh slice (any
    # coroutine resuming from a real park restarts it) reach the wire first. The
    # write is now issued first and the yield follows it.
    var srv: FairServer
    startFair(srv)
    proc run(port: int): Future[string] {.async.} =
      let c = await be.connect("127.0.0.1", port, false, TlsConfig(), ProxyTarget())
      await be.sendAll(c, "POST /mirror HTTP/1.1\r\nHost: x\r\n\r\n")
      spinUs(naviFairSliceUs + 1000)     # the slice is spent
      let a = be.sendAll(c, "A")         # first in call order
      sliceRestart()                     # another coroutine resumed from a real park
      let b = be.sendAll(c, "B")
      await a
      await b
      var got = ""
      while got.len < 2:
        let d = await be.recvSome(c)
        if d.len == 0: break
        got.add d
      await be.close(c)
      return got
    let got = waitFor run(srv.port)
    stopFair(srv)
    check got == "AB"

  test "h1 requests on one Navi do not queue behind a streamed download (#462)":
    var srv: FairServer
    startFair(srv)
    proc run(base: string): Future[(int, int, bool)] {.async.} =
      let api = newNavi()
      var got = 0
      var downloadDone = false
      proc download() {.async.} =
        let res = await api.stream.get(base & "/big")
        res.each(chunk):
          got += chunk.len
          await sleep(1)       # a slow consumer keeps the connection checked out
        downloadDone = true
      let dl = download()
      while got == 0: await sleep(1)       # the download holds its connection now
      var reqs: seq[Future[Response]]
      for _ in 0 ..< 20: reqs.add api.get(base & "/small")
      var ok = 0
      for f in reqs:
        let r = await f
        if r.status == 200 and r.body == "ok": inc ok
      let finishedFirst = not downloadDone
      await dl
      await api.close()
      return (ok, got, finishedFirst)
    let (ok, got, finishedFirst) = waitFor run("http://127.0.0.1:" & $srv.port)
    stopFair(srv)
    check ok == 20
    check got == fairBigBytes
    check finishedFirst        # all 20 answered while the download was in flight
