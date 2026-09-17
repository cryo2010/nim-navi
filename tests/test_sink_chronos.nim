## Response-body sink on the chronos backend. The spec body is shared with
## test_sink_async.nim via sink_spec.nim, so a test added there runs under both --
## in particular chronos's gcsafe / strict-raises checks.

import unittest
import std/strutils
import chronos
import navi/chronos
import navi/core/response as naviresp   # navi's HttpError/ResponseTooLargeError
import ./support

const
  sinkBackendName = "chronos"
  sinkBasePort = 9760

include ./sink_spec
