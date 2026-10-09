## SSE-over-HTTP/2 connection sharing, chronos backend (#466). The mirror of
## client.nim: the same spec (`share_spec.nim`) over the chronos client.
import pkg/chronos
import std/[os, strutils, times]
import navi/chronos

const backendName = "chronos"

template napMs(ms: int): untyped = sleepAsync(int64(ms).milliseconds)
  ## int64, so this is chronos's `milliseconds` and not std/times' TimeInterval one
  ## (`navi/chronos` shadows the module name, so it cannot be qualified here).

include ./share_spec
