## Response-body sink on the asyncdispatch backend. The spec body is shared with
## test_sink_chronos.nim via sink_spec.nim, so a test added there runs under both.

import unittest
import std/[asyncdispatch, strutils]
import navi/asyncdispatch
import navi/core/response as naviresp   # navi's HttpError/ResponseTooLargeError
import ./support

const
  sinkBackendName = "asyncdispatch"
  sinkBasePort = 9730

include ./sink_spec
