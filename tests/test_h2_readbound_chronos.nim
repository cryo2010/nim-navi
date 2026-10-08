## The h2 mux's connection-level read bound, chronos backend (#466). The mirror of
## `test_h2_readbound`: the same spec (`h2_readbound_spec.nim`) over the chronos mux,
## whose transport arms the bound with its own timed reads.
import unittest
import pkg/chronos
import std/times
import navi/backend/chronos as be
import navi/backend/h2mux_chronos
import navi/backend/api            # TlsConfig / ProxyTarget
import navi/core/response          # KeepAliveRaceError / UnprocessedError
import ./support_h2peer

const
  h2BackendName = "chronos"
  h2BoundBasePort = 9490

template napMs(ms: int): untyped = sleepAsync(chronos.milliseconds(ms))

proc connectBoundMux(port: int, readMs: int): Future[H2Mux] {.async.} =
  ## Connect and take over the h2 transport with `readMs` as the connection's read
  ## bound. See the asyncdispatch twin.
  var lastErr: ref CatchableError
  for _ in 0 ..< 100:
    try:
      let conn = await be.connect("127.0.0.1", port, false, TlsConfig(),
                                  ProxyTarget(), @[], 0, readMs)
      return await newH2Mux(conn)
    except CatchableError as e:
      lastErr = e
      await napMs(20)
  raise lastErr

include ./h2_readbound_spec
