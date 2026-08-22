## HTTP/2 multiplexing interop against nghttpd (chronos backend).
##
## Mirror of nghttpd_async.nim for the chronos backend, now that chronos runs
## OpenSSL over its transport and negotiates h2 over ALPN. Fires concurrent
## requests that share one connection and asserts they all complete over h2 --
## exercising the chronos h2 multiplexer against the reference server. Driven by
## tests/interop/run.sh.
##
## The request bodies are written as `run(...)` procs taking the URLs/cert as
## parameters: chronos's async transform is gcsafe-strict and forbids the nested
## coroutine from reading GC'd module globals, so we pass them in as locals.

import unittest
import std/[os, strutils]
import navi/chronos

let base = getEnv("NAVI_INTEROP_URL")
let padded = getEnv("NAVI_INTEROP_PADDED_URL")   # nghttpd started with -b (frame padding)
let cert = getEnv("NAVI_INTEROP_CERT")

proc gather(futs: seq[Future[Response]]): Future[seq[Response]] {.async.} =
  ## chronos has no `all` returning results; wait for all, then read each.
  await allFutures(futs)
  for f in futs: result.add f.read()

suite "nghttpd interop (chronos, http/2 mux)":
  test "reads a padded response without corruption over the mux":
    # nghttpd -b pads frames; navi must strip PADDED padding, or HPACK / the body
    # break. Runs concurrently to exercise the mux's h2 connection path.
    proc run(padded, cert: string): Future[seq[Response]] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      result = await gather(@[
        api.get(padded & "/small.txt"),
        api.get(padded & "/large.bin")])
    let res = waitFor run(padded, cert)
    check res[0].status == 200
    check res[0].body == "hello from nghttpd\n"   # HEADERS + DATA padding stripped
    check res[1].status == 200
    check res[1].body.len == 262144               # multi-frame padded body intact

  test "round-trips a streamed upload (bodyStream) over the mux":
    proc run(base, cert: string): Future[Response] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      var left = 5                        # 5 x 50k = 250 KB > the 64 KiB send window
      result = await api.request(POST, base & "/echo", bodyStream = proc(): string =
        if left == 0: return ""
        dec left
        repeat("z", 50_000))
    let res = waitFor run(base, cert)
    check res.status == 200
    check res.httpVersion == "HTTP/2"
    check res.body == repeat("z", 250_000)

  test "streams a response body to each incrementally over the mux":
    proc run(base, cert: string): Future[(int, int, int, string)] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      var calls, total = 0
      let res = await api.stream(GET, base & "/large.bin")   # returns after headers
      res.each(chunk):
        inc calls
        total += chunk.len
      return (calls, total, res.status, res.httpVersion)
    let (calls, total, status, ver) = waitFor run(base, cert)
    check ver == "HTTP/2"
    check status == 200
    check total == 262144                 # 256 KiB body delivered in full
    check calls > 1                       # ...incrementally, not buffered into one call

  test "concurrent GETs multiplex over a single connection":
    proc run(base, cert: string): Future[seq[Response]] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      result = await gather(@[
        api.get(base & "/small.txt"),
        api.get(base & "/small.txt"),
        api.get(base & "/small.txt"),
        api.get(base & "/small.txt")])
    let res = waitFor run(base, cert)
    check res.len == 4
    for r in res:
      check r.status == 200
      check r.httpVersion == "HTTP/2"

  test "the mux queues a burst larger than MAX_CONCURRENT_STREAMS":
    # Server caps concurrency at 2 (run.sh -m 2). Firing 8 at once must be admitted
    # in waves by the mux rather than opening streams that get reset.
    proc run(base, cert: string): Future[seq[Response]] {.async.} =
      var cfg = initNaviConfig()
      cfg.tls.caFile = cert
      let api = newNavi(cfg)
      var futs: seq[Future[Response]]
      for _ in 0 ..< 8: futs.add api.get(base & "/small.txt")
      result = await gather(futs)
    let res = waitFor run(base, cert)
    check res.len == 8
    for r in res:
      check r.status == 200
      check r.body == "hello from nghttpd\n"
