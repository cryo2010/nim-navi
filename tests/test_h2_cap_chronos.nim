## The h2 mux's per-stream response size cap, chronos backend (#466). The mirror of
## `test_h2_cap`: the same spec (`h2_cap_spec.nim`) over the chronos mux, whose
## strict gcsafe/raises checking the shared mux body has to satisfy.
import unittest
import pkg/chronos
import std/[times, strutils]
import navi/backend/chronos as be
import navi/backend/h2mux_chronos
import navi/backend/api            # TlsConfig / ProxyTarget
import navi/core/response          # ResponseTooLargeError
import ./support_h2cap

const
  h2BackendName = "chronos"
  h2CapBasePort = 9470

template napMs(ms: int): untyped = sleepAsync(chronos.milliseconds(ms))

proc connectCapMux(port: int, maxBody = 0): Future[H2Mux] {.async.} =
  ## Connect and take over the h2 transport, retrying until the peer thread has
  ## bound its listener. See the asyncdispatch twin for what `maxBody` is doing here.
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
