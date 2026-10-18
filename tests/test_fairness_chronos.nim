## Event-loop fairness on the chronos backend (#462). The spec body is shared with
## test_fairness_async.nim via fairness_spec.nim, so a test added there runs under
## both -- in particular chronos's gcsafe / strict-raises checks.

import unittest
import std/[monotimes, times]
import chronos
import navi/chronos
import navi/backend/chronos as be   # raw Conn I/O: connect / recvSome / sendAll
import navi/backend/api            # TlsConfig / ProxyTarget
import ./support_fair

const fairBackendName = "chronos"

include ./fairness_spec
