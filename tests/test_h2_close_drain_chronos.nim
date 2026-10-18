## A complete h2 response survives the server closing right after it, chronos backend
## (#461 round 2). The mirror of `test_h2_close_drain`: the same spec
## (`h2_close_drain_spec.nim`) over the chronos mux.
import unittest
import pkg/chronos
import navi/backend/chronos as be
import navi/backend/h2mux_chronos
import navi/backend/api            # TlsConfig / ProxyTarget
import ./support_h2close

const
  backendName = "chronos"
  basePort = 9630

template napMs(ms: int): untyped = sleepAsync(chronos.milliseconds(ms))

proc connectMux(port: int): Future[H2Mux] {.async.} =
  ## Connect and take over the h2 transport, retrying until the peer thread has
  ## bound its listener. See the asyncdispatch twin.
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
