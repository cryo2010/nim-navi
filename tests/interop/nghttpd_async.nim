## HTTP/2 multiplexing interop against nghttpd (asyncdispatch backend).
##
## Fires concurrent requests that share one connection and asserts they all
## complete over h2 -- exercising navi's transparent stream multiplexing against
## the reference server. Driven by tests/interop/run.sh.

import unittest
import std/os
import navi/asyncdispatch

let base = getEnv("NAVI_INTEROP_URL")
let cert = getEnv("NAVI_INTEROP_CERT")

suite "nghttpd interop (asyncdispatch, http/2 mux)":
  test "concurrent GETs multiplex over a single connection":
    proc run(): Future[seq[Response]] {.async.} =
      let api = newNavi(NaviConfig(tls: TlsConfig(verify: true, caFile: cert)))
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
      let api = newNavi(NaviConfig(tls: TlsConfig(verify: true, caFile: cert)))
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
      let api = newNavi(NaviConfig(tls: TlsConfig(verify: true, caFile: cert)))
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
