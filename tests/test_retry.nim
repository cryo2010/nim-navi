## Retry backoff unit tests, focused on Retry-After parsing (RFC 9110).
import unittest
import navi/core/[headers, response, retry, request]

proc respWith(retryAfter: string): Response =
  var h = initHeaders()
  if retryAfter.len > 0: h.add("retry-after", retryAfter)
  initResponse(503, "Service Unavailable", "HTTP/1.1", h, "")

let policy = RetryPolicy(limit: 3, maxDelay: 5000)

suite "retry backoff (Retry-After)":
  test "an overflowing Retry-After does not crash and is clamped (#236)":
    # `parseInt(raw) * 1000` would overflow int64 for a value above ~9.2e15 and
    # raise OverflowDefect -- not a CatchableError, so it escapes every handler and
    # crashes the process (a remotely triggerable crash from a single header). It
    # must be clamped before the multiply; the result is then bounded by maxDelay.
    check backoffMs(1, respWith("10000000000000000"), policy) == 5000

  test "a normal delta-seconds Retry-After is honored, bounded by maxDelay":
    check backoffMs(1, respWith("2"), policy) == 2000
    check backoffMs(1, respWith("999999"), policy) == 5000   # clamped to maxDelay

  test "a past HTTP-date Retry-After clamps to 0":
    check backoffMs(1, respWith("Wed, 09 Jun 2021 10:18:14 GMT"), policy) == 0

  test "a negative Retry-After falls back to exponential backoff":
    check backoffMs(1, respWith("-5"), policy) == 100        # attempt 1: 100 * 2^0

  test "no Retry-After uses exponential backoff":
    check backoffMs(1, respWith(""), policy) == 100
    check backoffMs(2, respWith(""), policy) == 200
