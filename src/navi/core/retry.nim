## Retry policy: which requests may be retried, and how long to wait.

import std/[strutils, times]
import ./headers, ./request, ./response

proc isRetryableVerb*(verb: HttpVerb, policy: RetryPolicy): bool =
  ## Whether `verb` is eligible for retry under `policy` (idempotent by default).
  verb in policy.methods

proc isIdempotent*(verb: HttpVerb): bool =
  ## HTTP idempotent methods (RFC 9110 9.2.2): intrinsically safe to auto-replay
  ## on a stale reused connection, independent of the user's retry policy. POST and
  ## PATCH are excluded, so they are never silently replayed.
  verb in {GET, HEAD, PUT, DELETE, OPTIONS}

proc isReplayable*(req: Request): bool =
  ## Whether a request may be re-sent on a fresh connection (stale-connection
  ## retry), a redirect hop, or a digest one-shot. A pull-based body producer
  ## (`bodyStream`) cannot rewind: it may already have been partially drained on
  ## the first attempt, so replaying it would send a truncated body. Everything
  ## else (a buffered `body`, or no body) is safe to replay. This is orthogonal to
  ## `isIdempotent`, which governs *whether* a processed request should be retried;
  ## a non-replayable body is never retried regardless of method.
  req.bodyStream == nil

proc isRetryableStatus*(status: int, policy: RetryPolicy): bool =
  ## Whether `status` should trigger a retry under `policy`.
  status in policy.statuses

proc shouldRetryAfterError*(attempt: int; bodyReplayable, unprocessed: bool;
                            verb: HttpVerb; policy: RetryPolicy): bool =
  ## Whether a raised transport error should be retried: attempts remain, the
  ## body can be replayed, and either the verb is retryable or the peer proved
  ## the request was not processed (h2 REFUSED_STREAM / above GOAWAY -- safe to
  ## replay even when non-idempotent).
  attempt < policy.limit and bodyReplayable and
    (isRetryableVerb(verb, policy) or unprocessed)

proc shouldRetryAfterResponse*(attempt, status: int; bodyReplayable: bool;
                               verb: HttpVerb; policy: RetryPolicy): bool =
  ## Whether a completed response should be retried: attempts remain, the body
  ## can be replayed, and both the verb and the status are retryable.
  attempt < policy.limit and bodyReplayable and
    isRetryableVerb(verb, policy) and isRetryableStatus(status, policy)

proc retryAfterMs(resp: Response): int =
  ## `Retry-After` as milliseconds, or -1 when absent/unparseable. Accepts both
  ## the delta-seconds form ("120") and the HTTP-date form ("Wed, 21 Oct 2015
  ## 07:28:00 GMT"), per RFC 9110; a past date clamps to 0.
  let raw = resp.headers.get("retry-after").strip
  if raw.len == 0: return -1
  try:
    let secs = parseInt(raw)
    if secs < 0: return -1            # a negative delta-seconds is malformed
    # Clamp before multiplying: a value that parses as int64 but overflows the
    # `* 1000` (anything above ~9.2e15) would raise OverflowDefect -- not a
    # CatchableError, so it would escape every handler and crash the process
    # (a remotely triggerable crash from one header). The min(cap, ...) bound in
    # backoffMs then clamps the (still large) result to the policy's maxDelay.
    return min(secs, high(int) div 1000) * 1000
  except ValueError: discard
  try:
    let at = parse(raw, "ddd, dd MMM yyyy HH:mm:ss 'GMT'", utc())
    return max(0, int((at.toTime - getTime()).inMilliseconds))
  except TimeParseError, ValueError:
    return -1

proc backoffMs*(attempt: int, resp: Response, policy: RetryPolicy): int =
  ## Wait before retry `attempt`: a `Retry-After` value takes precedence,
  ## otherwise capped exponential backoff. Either way it is bounded by
  ## `policy.maxDelay` so a hostile `Retry-After` cannot stall the client.
  let ra = retryAfterMs(resp)
  let cap = if policy.maxDelay > 0: policy.maxDelay else: high(int)
  if ra >= 0: return min(cap, ra)
  min(cap, 100 * (1 shl min(attempt - 1, 6)))
