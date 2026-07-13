## Retry policy: which requests may be retried, and how long to wait.

import std/strutils
import ./headers, ./request, ./response

proc isRetryableVerb*(verb: HttpVerb): bool =
  ## Only idempotent methods are retried by default.
  verb in {GET, HEAD, PUT, DELETE, OPTIONS}

proc isRetryableStatus*(status: int): bool =
  status in [408, 413, 429, 500, 502, 503, 504]

proc backoffMs*(attempt: int, resp: Response): int =
  ## A `Retry-After` value (integer seconds) takes precedence; otherwise a
  ## capped exponential backoff based on the attempt number.
  let retryAfter = resp.headers.get("retry-after").strip
  if retryAfter.len > 0:
    try: return parseInt(retryAfter) * 1000
    except ValueError: discard
  min(10000, 100 * (1 shl min(attempt - 1, 6)))
