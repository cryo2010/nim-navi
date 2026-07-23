## Retry policy: which requests may be retried, and how long to wait.

import std/[strutils, times]
import ./headers, ./request, ./response

proc isRetryableVerb*(verb: HttpVerb, policy: RetryPolicy): bool =
  ## Whether `verb` is eligible for retry under `policy` (idempotent by default).
  verb in policy.methods

proc isRetryableStatus*(status: int, policy: RetryPolicy): bool =
  ## Whether `status` should trigger a retry under `policy`.
  status in policy.statuses

proc retryAfterMs(resp: Response): int =
  ## `Retry-After` as milliseconds, or -1 when absent/unparseable. Accepts both
  ## the delta-seconds form ("120") and the HTTP-date form ("Wed, 21 Oct 2015
  ## 07:28:00 GMT"), per RFC 9110; a past date clamps to 0.
  let raw = resp.headers.get("retry-after").strip
  if raw.len == 0: return -1
  try: return parseInt(raw) * 1000
  except ValueError: discard
  try:
    let at = parse(raw, "ddd, dd MMM yyyy HH:mm:ss 'GMT'", utc())
    return max(0, int((at.toTime - getTime()).inMilliseconds))
  except TimeParseError, ValueError:
    return -1

proc backoffMs*(attempt: int, resp: Response, policy: RetryPolicy): int =
  ## Wait before retry `attempt`: a `Retry-After` value takes precedence,
  ## otherwise capped exponential backoff. Either way it is bounded by
  ## `policy.backoffCap` so a hostile `Retry-After` cannot stall the client.
  let ra = retryAfterMs(resp)
  let cap = if policy.backoffCap > 0: policy.backoffCap else: high(int)
  if ra >= 0: return min(cap, ra)
  min(cap, 100 * (1 shl min(attempt - 1, 6)))
