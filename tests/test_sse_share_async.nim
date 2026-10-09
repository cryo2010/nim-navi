## SSE connection sharing on the asyncdispatch backend (#466). The spec body is
## shared with test_sse_share_chronos.nim via sse_share_spec.nim, so a test added
## there runs under both.
import unittest
import std/options
import navi/asyncdispatch
import ./support_sse

const sseBackendName = "asyncdispatch"

include ./sse_share_spec
