## The h2 mux's drain budget, asyncdispatch backend (#461). The scenario and its
## rationale live in `h2_fairness_spec.nim`, which this instantiates; the chronos
## mirror is `test_h2_fairness_chronos.nim`.
##
## The peer (support_h2flood) runs on its own thread with blocking sockets, so it
## never waits on navi's event loop.
import unittest
import std/asyncdispatch
import navi/backend/asyncdispatch as be
import navi/backend/h2mux
import navi/backend/api            # TlsConfig / ProxyTarget
import navi/core/request           # BodyProducer
import ./support_h2flood

const
  h2BackendName = "asyncdispatch"
  h2FairBasePort = 9500

template napMs(ms: int): untyped = sleepAsync(ms)

proc connectFairMux(port: int): Future[H2Mux] {.async.} =
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

include ./h2_fairness_spec
