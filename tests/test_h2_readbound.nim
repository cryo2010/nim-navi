## The h2 mux's connection-level read bound, asyncdispatch backend (#466). The
## scenario and its rationale live in `h2_readbound_spec.nim`, which this
## instantiates; the chronos mirror is `test_h2_readbound_chronos.nim`.
##
## The peer (support_h2peer's blackhole) runs on its own thread with blocking
## sockets, so its silence never blocks navi's event loop.
import unittest
import std/[asyncdispatch, times]
import navi/backend/asyncdispatch as be
import navi/backend/h2mux
import navi/backend/api            # TlsConfig / ProxyTarget
import navi/core/response          # KeepAliveRaceError / UnprocessedError
import ./support_h2peer

const
  h2BackendName = "asyncdispatch"
  h2BoundBasePort = 9480

template napMs(ms: int): untyped = sleepAsync(ms)

proc connectBoundMux(port: int, readMs: int): Future[H2Mux] {.async.} =
  ## Connect and take over the h2 transport with `readMs` as the connection's read
  ## bound, retrying until the peer thread has bound its listener. The keepalive is
  ## left off, so the read bound is the only liveness detector under test.
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
