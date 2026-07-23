## HTTP/2 interop against nghttpd, the nghttp2 reference server.
##
## Driven by tests/interop/run.sh, which starts nghttpd (TLS + h2, echo-upload)
## and exports NAVI_INTEROP_URL / NAVI_INTEROP_CERT. Validates the half of navi
## unit tests can't: our HPACK encoder, the real h2 wire exchange, ALPN, and
## receive-side flow control, all against a strict reference peer.

import unittest
import std/[os, strutils]
import navi

let base = getEnv("NAVI_INTEROP_URL")
let cert = getEnv("NAVI_INTEROP_CERT")

proc client(): Navi =
  newNavi(NaviConfig(tls: TlsConfig(verify: true, caFile: cert)))

suite "nghttpd interop (sync, http/2)":
  test "negotiates h2 over ALPN and GETs a file":
    let res = client().get(base & "/small.txt")
    check res.status == 200
    check res.httpVersion == "HTTP/2"
    check res.body == "hello from nghttpd\n"

  test "the reference server accepts our HPACK-encoded headers":
    var h = initHeaders()
    for i in 0 ..< 20:
      h.add("x-navi-" & $i, "v-" & repeat("a", 100))
    check client().get(base & "/small.txt", headers = h).status == 200

  test "receives a body larger than the initial flow-control window":
    let res = client().get(base & "/large.bin")
    check res.status == 200
    check res.body.len == 262144      # 256 KiB > the 64 KiB initial window

  test "round-trips an uploaded body via echo-upload":
    let payload = repeat("x", 250_000)  # > the 64 KiB window: exercises send-side flow control
    let res = client().post(base & "/echo", body = payload)
    check res.status == 200
    check res.body == payload

  test "parallel multiplexes several requests over one connection":
    let res = client().parallel(
      @[base & "/small.txt", base & "/small.txt", base & "/small.txt"])
    check res.len == 3
    for r in res:
      check r.status == 200
      check r.httpVersion == "HTTP/2"

  test "parallel queues a batch larger than MAX_CONCURRENT_STREAMS":
    # The server caps concurrency at 2 (see run.sh -m 2); a batch of 8 must be
    # queued and drained in waves, not opened all at once (which would be reset).
    var targets: seq[string]
    for _ in 0 ..< 8: targets.add base & "/small.txt"
    let res = client().parallel(targets)
    check res.len == 8
    for r in res:
      check r.status == 200
      check r.body == "hello from nghttpd\n"

  test "many h2 requests over TLS do not grow the heap":
    # Exercises the h2 connection, HPACK encoder/decoder tables, and pool reuse
    # over TLS for growth the plain-http leak.nim loop can't reach.
    let api = client()
    for _ in 0 ..< 200: discard api.get(base & "/small.txt")   # reach steady state
    GC_fullCollect()
    let baseline = getOccupiedMem()
    for _ in 0 ..< 5000: discard api.get(base & "/small.txt")
    GC_fullCollect()
    let growth = getOccupiedMem() - baseline
    echo "h2 sync growth over 5000 requests: ", growth, " bytes"
    check growth < 8 * 1024 * 1024
