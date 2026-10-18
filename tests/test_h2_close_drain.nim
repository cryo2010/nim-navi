## A complete h2 response survives the server closing right after it, asyncdispatch
## backend (#461 round 2). The scenario lives in `h2_close_drain_spec.nim`; the
## chronos mirror is `test_h2_close_drain_chronos.nim`.
import unittest
import std/asyncdispatch
import navi/backend/asyncdispatch as be
import navi/backend/h2mux
import navi/backend/api            # TlsConfig / ProxyTarget
import ./support_h2close

const
  backendName = "asyncdispatch"
  basePort = 9620

template napMs(ms: int): untyped = sleepAsync(ms)

proc connectMux(port: int): Future[H2Mux] {.async.} =
  ## Connect and take over the h2 transport, retrying until the peer thread has
  ## bound its listener.
  var lastErr: ref CatchableError
  for _ in 0 ..< 100:
    try:
      let conn = await be.connect("127.0.0.1", port, false, TlsConfig(),
                                  ProxyTarget(), @[], 0, 0)
      return await newH2Mux(conn)
    except CatchableError as e:
      lastErr = e
      await napMs(20)
  raise lastErr

include ./h2_close_drain_spec
