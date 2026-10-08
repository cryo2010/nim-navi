## The h2 mux's per-stream response size cap, asyncdispatch backend (#466). The
## scenarios and their rationale live in `h2_cap_spec.nim`, which this instantiates;
## the chronos mirror is `test_h2_cap_chronos.nim`.
##
## The peer runs on its own thread with blocking sockets (see support_h2cap), so its
## deliberate silences never block navi's event loop.
import unittest
import std/[asyncdispatch, times, strutils]
import navi/backend/asyncdispatch as be
import navi/backend/h2mux
import navi/backend/api            # TlsConfig / ProxyTarget
import navi/core/response          # ResponseTooLargeError
import ./support_h2cap

const
  h2BackendName = "asyncdispatch"
  h2CapBasePort = 9460

template napMs(ms: int): untyped = sleepAsync(ms)

proc connectCapMux(port: int, maxBody = 0): Future[H2Mux] {.async.} =
  ## Connect and take over the h2 transport, retrying until the peer thread has
  ## bound its listener. `maxBody` is the DEFAULT cap the connection is opened with
  ## (what `sharedConnCap` hands a fresh mux); the tests open connections both with
  ## and without one, because what decides a stream's cap is the REQUEST's.
  var lastErr: ref CatchableError
  for _ in 0 ..< 100:
    try:
      let conn = await be.connect("127.0.0.1", port, false, TlsConfig(),
                                  ProxyTarget(), @[], 0, 0)
      return await newH2Mux(conn, maxBody)
    except CatchableError as e:
      lastErr = e
      await napMs(20)
  raise lastErr

include ./h2_cap_spec
