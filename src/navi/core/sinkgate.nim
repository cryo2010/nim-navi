## The response-sink delivery gate.
##
## A gated response sink (`request(..., sink = ...)`) must receive only the body of
## the FINAL surfaced response: never a redirect hop, a digest 401 challenge, a
## retryable status, or a body that will be thrown as an `HttpError`. The engine
## decides whether to stream a body to the sink at the drain site (after the
## headers, before the body), where it cannot yet see the policy layer's later
## decisions (follow this redirect? retry this status? throw this non-2xx?).
##
## `SinkGate` carries those decisions down to the drain site. The policy loops
## (`followRedirects`, `performRequest`, `maybeDigest`) fill its fields per hop /
## per attempt before each `run`, and the drain site calls `wantsDelivery` to ask
## "is this response certain to be surfaced normally?" -- delivering to the sink
## only when the answer is yes.
##
## It is a `ref object`, not a closure, deliberately: the policy loops are templates
## expanded into BOTH a sync proc and `{.async.}` procs, so a captured closure would
## have a backend-specific type. A plain ref shared by reference needs zero capture
## and threads through every backend identically.
##
## The gate is a streaming OPTIMIZATION and may be conservative: a false negative
## (it declines to stream a body that turns out to be the final one) is corrected by
## the one-shot fallback in `performRequest`, which delivers a buffered final body to
## the sink. A false positive (it streams a body that later turns out NOT to be
## final) would be a correctness bug, so `wantsDelivery` errs toward `false`.

import ./headers, ./request, ./redirect, ./retry
when not defined(js):
  import ./digest

type
  SinkGate* = ref object
    ## Per-request delivery state, filled by the policy loops and read at the drain
    ## site via `wantsDelivery`. A single instance lives for the whole request; the
    ## loops overwrite the hop/attempt-varying fields before each `run`.
    attempt*: int              ## retry attempt index (performRequest, per attempt)
    retryReplayable*: bool     ## the request body may be replayed (retry mirror)
    retryVerb*: HttpVerb       ## the verb the retry policy tests
    policy*: RetryPolicy       ## the retry policy in force
    hops*: int                 ## redirect hops taken so far (followRedirects)
    redirectLimit*: int        ## the configured redirect limit
    hopReplayable*: bool       ## the current hop's request body may be replayed
                               ## (so a non-replayable 307/308 IS surfaced)
    digestReady*: bool         ## a digest 401 challenge could still be answered on
                               ## this hop (so its 401 body must not be delivered)
    wantsThrow*: bool          ## throwHttpErrors is on (a non-2xx will be thrown)
    http*: set[HttpVersion]    ## the requested protocol set (for protocolAllowed)
    fed*: bool                 ## the sink has been fed at least once; set by the
                               ## wrapped sink before its first delivery. Read by the
                               ## replay guards: a half-delivered body must never be
                               ## re-issued, so once fed, every retry/replay point
                               ## raises instead of retrying.

proc newSinkGate*(): SinkGate = SinkGate()

proc wantsDelivery*(g: SinkGate, httpVersion: string, status: int,
                    headers: Headers): bool =
  ## Whether the response with this `httpVersion`/`status`/`headers` is certain to be
  ## surfaced to the caller normally, so its body may stream to the sink. Returns
  ## `false` (do not deliver) whenever the policy layer will instead follow, retry,
  ## digest-replay, or throw it:
  ##   * the protocol is not allowed (enforceProtocol will throw);
  ##   * it is a redirect that will be followed (a NON-replayable 307/308 is the
  ##     exception: it is surfaced, so it IS delivered);
  ##   * a digest challenge that will be answered (401 with a usable challenge while
  ##     digest is still armed for this hop);
  ##   * a retryable status with attempts remaining;
  ##   * a non-2xx that throwHttpErrors will raise.
  if not protocolAllowed(g.http, httpVersion):
    return false
  let location = headers.get("location")
  if shouldFollowRedirect(status, g.hops, g.redirectLimit, location):
    # A redirect that would be followed. The one carve-out is a non-replayable
    # 307/308: followRedirects breaks rather than replay it, so it is surfaced and
    # must be delivered. Everything else is not the final response.
    if not (not g.hopReplayable and (status == 307 or status == 308)):
      return false
  when not defined(js):
    if g.digestReady and status == 401:
      let chal = bestChallenge(headers.getAll("www-authenticate"))
      if chal.isSome:
        return false
  if shouldRetryAfterResponse(g.attempt, status, g.retryReplayable, g.retryVerb,
                              g.policy):
    return false
  if g.wantsThrow and not (status >= 200 and status < 300):
    return false
  true
