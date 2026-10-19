## Event-loop fairness on the asyncdispatch backend (#462). The spec body is shared
## with test_fairness_chronos.nim via fairness_spec.nim, so a test added there runs
## under both.

import unittest
import std/[asyncdispatch, monotimes, times]
import navi/asyncdispatch
import navi/backend/asyncdispatch as be   # raw Conn I/O: connect / recvSome / sendAll
import navi/backend/api            # TlsConfig / ProxyTarget
import ./support_fair

const fairBackendName = "asyncdispatch"

include ./fairness_spec
