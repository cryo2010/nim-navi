## Response model and body accessors.

import std/json
import ./headers, ./charset
export json

type
  StreamPhase* = enum
    ## Lifecycle of a pull-based streaming-download handle (`StreamResponse`), one
    ## field in place of the former correlated `drained`/`closed` bool pair. The two
    ## terminal states are mutually exclusive, so a single enum makes that invariant
    ## explicit and collapses every `if drained or closed` guard to `phase != spOpen`.
    spOpen                     ## body not yet fully read and not disposed
    spDrained                  ## body fully read; connection returned/finished
    spClosed                   ## disposed without draining (early close)

  Response* = object
    status*: int
    reason*: string
    httpVersion*: string
    headers*: Headers
    trailers*: Headers         ## trailing fields after the body (chunked/h2); empty
                               ## when the response carried none
    body*: string
    bodyTruncated*: bool        ## set only when a gated response sink returned `false`
                               ## to stop the download early: the request still
                               ## completed normally (no exception), but the body was
                               ## not fully transferred, so `body` is "" and any
                               ## trailers are absent. On h1 the connection is closed
                               ## (not pooled); on h2 the stream is reset while the
                               ## connection is kept; on js the fetch body is aborted.
                               ## Always false on a full drain or a buffered request.
    dataCache: ref JsonNode    ## lazily-parsed, cached JSON (see `data`)

  HttpError* = object of CatchableError
    ## Raised for non-2xx responses when `throwHttpErrors` is on (the default).
    ## The full response is attached for inspection.
    response*: Response

  SinkStopSignal* = object of CatchableError
    ## Internal control signal, raised by the wrapped gated sink when the caller's
    ## sink returns `false` (stop the download early). It unwinds the body-drain
    ## loop and is caught at the drain site, which turns it into a normal return with
    ## `bodyTruncated = true`. It must NEVER escape `request()`; catching it anywhere
    ## it might leak to a user is a bug.

  TimeoutError* = object of CatchableError
    ## Raised when a request exceeds the configured `timeout`.

  ResponseTooLargeError* = object of CatchableError
    ## Raised when a response body exceeds the configured `maxResponseBytes`.

  UnprocessedError* = object of CatchableError
    ## Raised when the peer signalled (HTTP/2 REFUSED_STREAM or a GOAWAY above the
    ## stream id) that the request was not processed. Safe to retry regardless of
    ## method idempotency; the retry layer does so automatically.

  KeepAliveRaceError* = object of IOError
    ## Raised when a connection is torn down after the request was WRITTEN but before
    ## any response HEADERS arrived (an idle recycle, a GOAWAY-less close, or a mid-flight
    ## drop). The request's fate is genuinely ambiguous: it may have been received and
    ## processed by the peer, or not. Unlike `UnprocessedError` (a peer PROOF of
    ## non-processing -- REFUSED_STREAM, a GOAWAY above the stream id, or a connection
    ## found dead BEFORE the request was sent), this is only "no response began," which
    ## does not imply "not processed" once the bytes are on the wire.
    ##
    ## Retry policy (mirrors Go net/http's post-write behavior; see RFC 9110 9.2.2):
    ## an idempotent method is retried, and any method is retried if the request carries
    ## an `Idempotency-Key`; a non-idempotent method WITHOUT such a key is NOT
    ## auto-retried (retrying could double-apply a side effect the peer already
    ## committed). A drop that happens AFTER response headers begin is a plain `IOError`
    ## (truncation), never this. An `IOError` subtype, so existing `except IOError`
    ## handlers still catch it and the surfaced message is unchanged.

  ProtocolError* = object of CatchableError
    ## Raised when the HTTP version actually used is not one the request allowed
    ## via `config.http` (strict protocol selection). For example, requesting
    ## `{H2}` against an origin that only offers HTTP/1.1 raises this instead of
    ## silently downgrading. Narrow or widen `config.http` to control it.

proc initResponse*(status: int; reason, httpVersion: string; headers: Headers;
                   body: string): Response =
  ## Build a Response with an allocated JSON cache slot. Used by the protocol
  ## layers so `data` caches instead of re-parsing.
  result = Response(status: status, reason: reason, httpVersion: httpVersion,
                    headers: headers, body: body)
  when not defined(js):
    # The JS backend can't allocate this ref-to-a-ref cell; leaving it nil makes
    # `data` reparse each call instead of caching (see the nil branch there).
    new(result.dataCache)

proc enforceMaxResponse*(r: Response, limit: int) =
  ## Raise `ResponseTooLargeError` if a buffered body exceeds `limit` (0 = off).
  ## The streaming path enforces the same cap incrementally as chunks are decoded
  ## in the engine (and via the h2 receive-window RST on the async mux).
  if limit > 0 and r.body.len > limit:
    raise newException(ResponseTooLargeError,
      "navi: response body of " & $r.body.len & " bytes exceeds " &
      "maxResponseBytes (" & $limit & ")")

proc ok*(r: Response): bool {.inline.} =
  ## True for 2xx status codes.
  r.status >= 200 and r.status < 300

proc text*(r: Response): string =
  ## The body decoded to UTF-8 from its charset. Uses a leading BOM if present,
  ## else the `Content-Type` charset parameter, else UTF-8; an unrecognized
  ## charset returns the raw bytes. Unlike `body` (raw bytes) this yields correct
  ## text for non-UTF-8 responses (ISO-8859-1, Windows-1252, UTF-16).
  decodeText(r.body, r.headers.get("content-type"))

proc data*(r: Response): JsonNode =
  ## The body parsed as JSON, regardless of Content-Type, parsed once and cached
  ## (so `res.data["a"]` and `res.data["b"]` reuse one parse). Raises
  ## JsonParsingError on invalid JSON.
  if r.dataCache == nil:            # response built without a cache slot
    return parseJson(r.body)
  if r.dataCache[] == nil:
    r.dataCache[] = parseJson(r.body)
  r.dataCache[]
