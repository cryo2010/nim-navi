## SSE reconnect-delay floor and empty-connect backoff on the asyncdispatch
## backend (#291). The spec body is shared with test_sse_retry_chronos.nim via
## sse_retry_spec.nim, so a test added there runs under both.
import unittest
import std/[options, monotimes, times]
import navi/asyncdispatch
import ./support_sse

const sseBackendName = "asyncdispatch"

include ./sse_retry_spec
