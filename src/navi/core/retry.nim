## Retry policy: which requests may be retried, and how long to wait.

import std/[strutils, times, monotimes]
import ./headers, ./request, ./response

proc isRetryableVerb*(verb: HttpVerb, policy: RetryPolicy): bool =
  ## Whether `verb` is eligible for retry under `policy` (idempotent by default).
  verb in policy.methods

proc isIdempotent*(verb: HttpVerb): bool =
  ## HTTP idempotent methods (RFC 9110 9.2.2): intrinsically safe to auto-replay
  ## on a stale reused connection, independent of the user's retry policy. POST and
  ## PATCH are excluded, so they are never silently replayed.
  verb in {GET, HEAD, PUT, DELETE, OPTIONS}

proc hasIdempotencyKey*(req: Request): bool =
  ## Whether the request carries a caller-supplied idempotency guarantee, letting the
  ## client safely auto-replay it even when the method is non-idempotent and the
  ## request may already have been transmitted. Mirrors Go net/http, which treats an
  ## `Idempotency-Key` (or `X-Idempotency-Key`) header as making any method replayable.
  req.headers.contains("idempotency-key") or req.headers.contains("x-idempotency-key")

proc isReplayable*(req: Request): bool =
  ## Whether a request may be re-sent on a fresh connection (stale-connection
  ## retry), a redirect hop, or a digest one-shot. A pull-based body producer
  ## (`bodyStream`, or an async producer threaded outside the request -- both flag
  ## `hasStreamedBody`) cannot rewind: it may already have been partially drained on
  ## the first attempt, so replaying it would send a truncated body. Everything
  ## else (a buffered `body`, or no body) is safe to replay. This is orthogonal to
  ## `isIdempotent`, which governs *whether* a processed request should be retried;
  ## a non-replayable body is never retried regardless of method.
  req.bodyStream == nil and not req.hasStreamedBody

proc isRetryableStatus*(status: int, policy: RetryPolicy): bool =
  ## Whether `status` should trigger a retry under `policy`.
  status in policy.statuses

proc replayableAnyMethod*(req: Request, e: ref Exception): bool =
  ## Whether transport error `e` makes `req` safe to replay REGARDLESS of method
  ## idempotency: the peer proved it was not processed (`UnprocessedError` -- h2
  ## REFUSED_STREAM / above GOAWAY / a connection found dead before the request was
  ## written), or the connection dropped before any response began
  ## (`KeepAliveRaceError`) AND the caller vouched safety with an Idempotency-Key.
  ## This is NOT proof of non-processing for the keyed-race case; it is caller-vouched.
  ## Mirrors Go net/http's post-write replay rule (RFC 9110 9.2.2).
  (e of UnprocessedError) or ((e of KeepAliveRaceError) and hasIdempotencyKey(req))

proc isReplayClassError*(e: ref Exception): bool =
  ## Whether `e` is one of the two transport error classes a reused/pooled
  ## fall-through may replay on a fresh connection: `KeepAliveRaceError` (the request
  ## was written, then the connection dropped before any response began -- ambiguous)
  ## or `UnprocessedError` (the peer proved it was not processed). Any other error
  ## (a post-response truncation, a cancellation, a protocol error) is terminal and
  ## must propagate. The single place the replayable-error TYPE set lives, shared by
  ## every fall-through so the set cannot diverge (see `replayableAnyMethod`).
  e of KeepAliveRaceError or e of UnprocessedError

proc replayableAfterError*(req: Request, e: ref Exception): bool =
  ## The single transport-layer replay predicate, shared by every reused/pooled
  ## connection fall-through (h1 + h2, sync + async, streaming + buffered): re-send `req`
  ## on a fresh connection after `e` when the method is idempotent, or the error makes it
  ## replayable regardless of method (`replayableAnyMethod`). Orthogonal to `isReplayable`
  ## (body rewindability), which the caller must also check.
  isIdempotent(req.verb) or replayableAnyMethod(req, e)

proc mayFallBackFromH3*(req: Request, submitted: bool): bool =
  ## Whether an HTTP/3 attempt that failed with a `QuicError` may be re-sent over
  ## h2/h1. `submitted` is false when the failure is provably pre-submit (the QUIC
  ## connection was never established or was found closed, or the stream could not be
  ## opened): nothing reached the server, so any method may fall back. Once the stream
  ## has been submitted (`QuicSubmittedError`) the outcome is indeterminate -- the
  ## server may have processed the request before the stream or connection died -- so
  ## only an idempotent method may be re-sent, the same discipline the h1/h2
  ## fall-through applies via `replayableAfterError` (RFC 9110 9.2.2, issue #378).
  ##
  ## A streamed body is bound to the same split (issue #293): the h3 leg pulls
  ## `bodyStream` from the C data reader as soon as the stream is submitted, so a
  ## submitted-then-failed attempt may have spent the producer and replaying it over
  ## h2/h1 would upload a truncated body. `isReplayable` therefore gates the submitted
  ## case, exactly as it does in the retry loop, `maybeDigest` and `followRedirects`;
  ## a pre-submit failure never invoked the producer, so it still falls back freely.
  if not submitted: return true
  isReplayable(req) and isIdempotent(req.verb)

proc shouldRetryAfterError*(attempt: int; bodyReplayable, replayableAnyMethod: bool;
                            verb: HttpVerb; policy: RetryPolicy): bool =
  ## Whether a raised transport error should be retried by the policy loop: attempts
  ## remain, the body can be replayed, and either the verb is in the retry policy or the
  ## error is replayable regardless of method (`replayableAnyMethod` -- a proven-
  ## unprocessed error, or an Idempotency-Key-vouched keep-alive race; see that proc).
  attempt < policy.limit and bodyReplayable and
    (isRetryableVerb(verb, policy) or replayableAnyMethod)

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

type RetryDeadline* = object
  ## An absolute deadline for the *whole* request (all attempts, redirects, and
  ## the backoff sleeps between them). `config.timeouts.total` documents this
  ## contract. On the async backends the outer `guard` enforces it by aborting
  ## in-flight IO; the sync and batch paths have no such guard, so they carry a
  ## `RetryDeadline` and honor it cooperatively: they stop retrying once the
  ## budget is spent and cap each backoff sleep to what remains. An inactive
  ## deadline (`totalMs <= 0`) is unbounded.
  active: bool
  at: MonoTime

proc initRetryDeadline*(totalMs: int): RetryDeadline =
  ## Arm a deadline `totalMs` from now, or an inactive (unbounded) one when
  ## `totalMs <= 0`. Called once at request start, before the first attempt.
  if totalMs > 0:
    RetryDeadline(active: true, at: getMonoTime() + initDuration(milliseconds = totalMs))
  else:
    RetryDeadline(active: false)

proc remainingMs*(d: RetryDeadline): int =
  ## Milliseconds left in the budget: `int.high` when unbounded, and never below
  ## 0 (a lapsed deadline reports 0, which callers treat as "no time for another
  ## attempt").
  if not d.active: return high(int)
  let left = (d.at - getMonoTime()).inMilliseconds
  if left <= 0: 0 else: int(left)

proc attemptBudgetMs*(d: RetryDeadline): int =
  ## The per-attempt connect/total budget to stamp on the next attempt's request
  ## (`Request.deadlineMs`), so each attempt gets the REMAINING time rather than a
  ## fresh `totalMs`. 0 when unbounded -- meaning "fall back to config.total" (which
  ## is itself unbounded then), so an inactive deadline never fabricates a huge
  ## `deadlineMs` that would overflow the monotonic-clock arithmetic at connect.
  if not d.active: 0 else: d.remainingMs

proc effectiveAttemptMs*(remainingTotalMs, attemptMs: int): int =
  ## The wall-clock budget for one attempt: the smaller of the per-attempt cap
  ## (`config.timeouts.attempt`, 0 = none) and the remaining whole-request budget
  ## (`remainingTotalMs`, 0 = unbounded). Returns 0 (unbounded) only when neither
  ## is set. Composes with the retry deadline: pass `deadline.attemptBudgetMs` as
  ## `remainingTotalMs` so an attempt never outlives the `total` budget. Because it
  ## is a plain `min`, a lapse of the per-attempt slice (total still has room) and a
  ## lapse of `total` itself are distinguished by the retry loop, not here: the
  ## former is retried like any transport error, the latter stops the loop.
  if attemptMs <= 0: remainingTotalMs
  elif remainingTotalMs <= 0: attemptMs
  else: min(remainingTotalMs, attemptMs)

proc backoffWithinDeadline*(d: RetryDeadline, backoff: int): int =
  ## The backoff sleep to actually perform before the next attempt, or -1 to stop
  ## retrying because the budget is (or would be) exhausted. When unbounded the
  ## backoff is used as is. Otherwise: no time left, or not enough left to sleep
  ## the backoff AND still make a meaningful attempt, means give up now (surface
  ## the last error/response, matching the async guard's timeout semantics);
  ## a positive remainder caps the sleep so it never overruns the deadline.
  if not d.active: return backoff
  let left = d.remainingMs
  if left <= 0: return -1               # budget spent: no further attempt
  if backoff >= left: return -1         # sleeping the backoff would exhaust it
  backoff

proc backoffMs*(attempt: int, resp: Response, policy: RetryPolicy): int =
  ## Wait before retry `attempt`: a `Retry-After` value takes precedence,
  ## otherwise capped exponential backoff. Either way it is bounded by
  ## `policy.maxDelay` so a hostile `Retry-After` cannot stall the client.
  let ra = retryAfterMs(resp)
  let cap = if policy.maxDelay > 0: policy.maxDelay else: high(int)
  if ra >= 0: return min(cap, ra)
  min(cap, 100 * (1 shl min(attempt - 1, 6)))
