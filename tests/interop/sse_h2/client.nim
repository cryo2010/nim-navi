## SSE-over-HTTP/2 connection sharing, asyncdispatch backend (#466). The scenario
## lives in `share_spec.nim`; `client_chronos.nim` is the mirror. Driven by run.sh.
import std/[asyncdispatch, os, strutils, times]
import navi/asyncdispatch

const backendName = "asyncdispatch"

template napMs(ms: int): untyped = sleepAsync(ms)

include ./share_spec
