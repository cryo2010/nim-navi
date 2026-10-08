## SSE connection sharing on the chronos backend (#466). The spec body is shared
## with test_sse_share_async.nim via sse_share_spec.nim, so a test added there runs
## under both -- in particular chronos's gcsafe / strict-raises checks.
import unittest
import std/options
import chronos
import navi/chronos
import ./support_sse

const sseBackendName = "chronos"

include ./sse_share_spec
