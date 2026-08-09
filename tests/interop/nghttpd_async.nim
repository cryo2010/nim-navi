## HTTP/2 multiplexing interop against nghttpd (asyncdispatch backend).
##
## Fires concurrent requests that share one connection and asserts they all
## complete over h2 -- exercising navi's transparent stream multiplexing against
## the reference server. Driven by tests/interop/run.sh.

import unittest
import std/[os, strutils]
import navi/asyncdispatch

let base = getEnv("NAVI_INTEROP_URL")
let padded = getEnv("NAVI_INTEROP_PADDED_URL")   # nghttpd started with -b (frame padding)
let cert = getEnv("NAVI_INTEROP_CERT")

suite "nghttpd interop (asyncdispatch, http/2 mux)":
  test "reads a padded response without corruption over the mux":
    # nghttpd -b pads frames; navi must strip PADDED padding, or HPACK / the body
    # break. Runs concurrently to exercise the mux's h2 connection path.
    proc run(): Future[seq[Response]] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      result = await all(@[
        api.get(padded & "/small.txt"),
        api.get(padded & "/large.bin")])

    let res = waitFor run()
    check res[0].status == 200
    check res[0].body == "hello from nghttpd\n"    # exact: HEADERS + DATA padding stripped
    check res[1].status == 200
    check res[1].body.len == 262144                # multi-frame padded body intact

  test "round-trips a streamed upload (bodyStream) over the mux":
    # A pull-based body over the async h2 mux: HEADERS then DATA frames from the
    # producer, larger than the send window so the reader releases the tail on
    # WINDOW_UPDATE and wakes the producer. nghttpd echoes it back verbatim.
    proc run(): Future[Response] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      var left = 5                        # 5 x 50k = 250 KB > the 64 KiB send window
      result = await api.request(POST, base & "/echo", bodyStream = proc(): string =
        if left == 0: return ""
        dec left
        repeat("z", 50_000))
    let res = waitFor run()
    check res.status == 200
    check res.httpVersion == "HTTP/2"
    check res.body == repeat("z", 250_000)

  test "streams a response body to a sink incrementally over the mux":
    # The mux delivers DATA to the sink as it arrives rather than buffering the
    # whole body; a 256 KiB body must therefore arrive in more than one sink call.
    proc run(): Future[(int, int, string)] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      var calls, total = 0
      let res = await api.stream(GET, base & "/large.bin",
        sink = proc(data: string) {.async.} =
          inc calls
          total += data.len)
      return (calls, total, res.httpVersion)
    let (calls, total, ver) = waitFor run()
    check ver == "HTTP/2"
    check total == 262144                 # 256 KiB body delivered in full
    check calls > 1                       # ...incrementally, not buffered into one call

  test "concurrent GETs multiplex over a single connection":
    proc run(): Future[seq[Response]] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      result = await all(@[
        api.get(base & "/small.txt"),
        api.get(base & "/small.txt"),
        api.get(base & "/small.txt"),
        api.get(base & "/small.txt")])

    let res = waitFor run()
    check res.len == 4
    for r in res:
      check r.status == 200
      check r.httpVersion == "HTTP/2"

  test "the mux queues a burst larger than MAX_CONCURRENT_STREAMS":
    # Server caps concurrency at 2 (run.sh -m 2). Firing 8 at once must be
    # admitted in waves by the mux rather than opening streams that get reset.
    proc run(): Future[seq[Response]] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      var futs: seq[Future[Response]]
      for _ in 0 ..< 8: futs.add api.get(base & "/small.txt")
      result = await all(futs)

    let res = waitFor run()
    check res.len == 8
    for r in res:
      check r.status == 200
      check r.body == "hello from nghttpd\n"

  test "the mux stays flat over many concurrent requests":
    # Batches of 10 (past the server's cap of 2) churn the mux's waiter table and
    # slot queue; assert the Nim heap does not grow across 5000 requests.
    proc run(): Future[int] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      proc batch(): Future[void] {.async.} =
        var futs: seq[Future[Response]]
        for _ in 0 ..< 10: futs.add api.get(base & "/small.txt")
        discard await all(futs)
      for _ in 0 ..< 20: await batch()          # reach steady state
      GC_fullCollect()
      let baseline = getOccupiedMem()
      for _ in 0 ..< 500: await batch()          # 5000 requests
      GC_fullCollect()
      result = getOccupiedMem() - baseline

    let growth = waitFor run()
    echo "h2 mux growth over 5000 requests: ", growth, " bytes"
    check growth < 8 * 1024 * 1024
