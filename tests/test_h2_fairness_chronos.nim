## The h2 mux's drain budget, chronos backend (#461). The mirror of
## `test_h2_fairness`: the same spec (`h2_fairness_spec.nim`) over the chronos mux.
import unittest
import pkg/chronos
import navi/backend/chronos as be
import navi/backend/h2mux_chronos
import navi/backend/api            # TlsConfig / ProxyTarget
import navi/core/request           # BodyProducer
import ./support_h2flood

const
  h2BackendName = "chronos"
  h2FairBasePort = 9510

template napMs(ms: int): untyped = sleepAsync(chronos.milliseconds(ms))

proc connectFairMux(port: int): Future[H2Mux] {.async.} =
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

include ./h2_fairness_spec
