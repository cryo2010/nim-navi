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
      let api = newNavi(NaviOptions(tls: TlsConfig(verify: true, caFile: cert)))
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
