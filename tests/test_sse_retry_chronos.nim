## SSE reconnect-delay floor and empty-connect backoff on the chronos backend
## (#291). The spec body is shared with test_sse_retry_async.nim via
## sse_retry_spec.nim, so a test added there runs under both -- in particular
## chronos's gcsafe / strict-raises checks.
import unittest
import std/[options, monotimes, times]
import chronos
import navi/chronos
import ./support_sse

const sseBackendName = "chronos"

include ./sse_retry_spec
