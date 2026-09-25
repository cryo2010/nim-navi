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

suite "per-attempt budget (#375)":
  test "the smaller of the per-attempt cap and the remaining total wins":
    check effectiveAttemptMs(1000, 200) == 200   # attempt cap is tighter
    check effectiveAttemptMs(200, 1000) == 200   # remaining total is tighter
    check effectiveAttemptMs(300, 300) == 300    # equal

  test "an unset per-attempt cap falls back to the remaining total":
    check effectiveAttemptMs(500, 0) == 500
    check effectiveAttemptMs(0, 0) == 0          # both unbounded stays unbounded

  test "an unbounded total lets the per-attempt cap bound each try alone":
    check effectiveAttemptMs(0, 250) == 250

suite "idempotency key (keep-alive-race replay opt-in)":
  proc reqWith(headerName: string): Request =
    var h = initHeaders()
    if headerName.len > 0: h[headerName] = "abc-123"
    Request(verb: POST, headers: h)

  test "no idempotency-key header -> not vouched":
    check not hasIdempotencyKey(reqWith(""))
    check not hasIdempotencyKey(reqWith("content-type"))

  test "an Idempotency-Key header vouches (case-insensitive)":
    check hasIdempotencyKey(reqWith("Idempotency-Key"))
    check hasIdempotencyKey(reqWith("idempotency-key"))

  test "the X-Idempotency-Key variant is also accepted (matches Go)":
    check hasIdempotencyKey(reqWith("X-Idempotency-Key"))

suite "HTTP/3 fall-back discipline (#378)":
  proc plain(verb: HttpVerb): Request =
    Request(verb: verb, headers: initHeaders())

  test "a pre-submit QUIC failure falls back for any method":
    # Nothing reached the server (never connected / stream never opened), so even a
    # POST may be sent again over h2/h1.
    check mayFallBackFromH3(plain(POST), submitted = false)
    check mayFallBackFromH3(plain(PATCH), submitted = false)
    check mayFallBackFromH3(plain(GET), submitted = false)

  test "a submitted-then-failed request only falls back when idempotent":
    check mayFallBackFromH3(plain(GET), submitted = true)
    check mayFallBackFromH3(plain(PUT), submitted = true)
    check mayFallBackFromH3(plain(DELETE), submitted = true)
    check not mayFallBackFromH3(plain(POST), submitted = true)
    check not mayFallBackFromH3(plain(PATCH), submitted = true)

  test "the streaming-response leg is gated by the verb, not by a request body":
    # `stream`/SSE over h3 submits without a request body, so `isReplayable` is
    # always true there; the gate that matters is the verb. A POST whose h3 stream
    # was reset after `awaitHeaders` must NOT be re-sent over h2/h1, while a GET may.
    var bodyless = plain(POST)
    bodyless.body = ""
    check not mayFallBackFromH3(bodyless, submitted = true)
    check mayFallBackFromH3(bodyless, submitted = false)
    check mayFallBackFromH3(plain(GET), submitted = true)

suite "HTTP/3 fall-back with a streamed upload (#293)":
  proc streamed(verb: HttpVerb): Request =
    var r = Request(verb: verb, headers: initHeaders())
    r.bodyStream = proc(): string = ""
    r.hasStreamedBody = true
    r

  test "a pre-submit failure may still fall back: the producer was never pulled":
    check mayFallBackFromH3(streamed(PUT), submitted = false)
    check mayFallBackFromH3(streamed(POST), submitted = false)

  test "a submitted streamed upload never falls back, even when idempotent":
    # The h3 data reader pulls `bodyStream` as soon as the stream is submitted, so
    # the producer may be spent; replaying it over h2/h1 would upload a truncated
    # body. Matches the retry loop / digest / redirect guards.
    check not mayFallBackFromH3(streamed(PUT), submitted = true)
    check not mayFallBackFromH3(streamed(GET), submitted = true)

  test "an async producer (hasStreamedBody, no bodyStream) is guarded too":
    var r = Request(verb: PUT, headers: initHeaders())
    r.hasStreamedBody = true
    check not mayFallBackFromH3(r, submitted = true)
    check mayFallBackFromH3(r, submitted = false)
