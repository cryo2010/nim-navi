## Backend-agnostic rate-limiting core: a token bucket. Pure state + arithmetic,
## no async -- the sync and async middleware factories both drive it and then wait
## with their own `sleep`. (The concurrency limiter needs a per-backend future
## queue, so it lives in the async factory, not here.)

import std/[times, math]

type
  TokenBucket* = ref object
    ratePerSec: float   ## sustained rate (tokens added per second)
    burst: float        ## bucket capacity (max tokens available at once)
    tokens: float       ## current tokens; may go negative while requests queue
    last: float         ## epoch seconds of the last refill

proc newTokenBucket*(perSec: float, burst = 0): TokenBucket =
  ## `perSec` sustained requests/second; `burst` (default = ceil(perSec), min 1)
  ## is how many may go at once after an idle period.
  let cap = if burst > 0: burst.float else: max(1.0, ceil(perSec))
  TokenBucket(ratePerSec: perSec, burst: cap, tokens: cap, last: epochTime())

proc take*(b: TokenBucket, now = epochTime()): int =
  ## Refill for elapsed time, then reserve one token for this request. Returns the
  ## milliseconds the caller must wait before proceeding (0 if a token was free).
  ## Reserving drives `tokens` negative, so concurrent callers are handed
  ## increasing delays and thus proceed in order at the configured rate.
  if b.ratePerSec <= 0: return 0     # unlimited
  b.tokens = min(b.burst, b.tokens + (now - b.last) * b.ratePerSec)
  b.last = now
  b.tokens -= 1.0
  if b.tokens >= 0: return 0
  int(ceil(-b.tokens / b.ratePerSec * 1000.0))   # wait for the deficit to accrue
