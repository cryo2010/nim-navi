## Response model and body accessors.

import std/json
import ./headers
export json

type
  Response* = object
    status*: int
    reason*: string
    httpVersion*: string
    headers*: Headers
    body*: string
    dataCache: ref JsonNode    ## lazily-parsed, cached JSON (see `data`)

  HttpError* = object of CatchableError
    ## Raised for non-2xx responses when `throwHttpErrors` is on (the default).
    ## The full response is attached for inspection.
    response*: Response

  TimeoutError* = object of CatchableError
    ## Raised when a request exceeds the configured `timeout`.

  ResponseTooLargeError* = object of CatchableError
    ## Raised when a response body exceeds the configured `maxResponseBytes`.

  UnprocessedError* = object of CatchableError
    ## Raised when the peer signalled (HTTP/2 REFUSED_STREAM or a GOAWAY above the
    ## stream id) that the request was not processed. Safe to retry regardless of
    ## method idempotency; the retry layer does so automatically.

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
  ## The streaming path enforces the same cap incrementally via `limitedSink`.
  if limit > 0 and r.body.len > limit:
    raise newException(ResponseTooLargeError,
      "navi: response body of " & $r.body.len & " bytes exceeds " &
      "maxResponseBytes (" & $limit & ")")

proc ok*(r: Response): bool {.inline.} =
  ## True for 2xx status codes.
  r.status >= 200 and r.status < 300

proc data*(r: Response): JsonNode =
  ## The body parsed as JSON, regardless of Content-Type, parsed once and cached
  ## (so `res.data["a"]` and `res.data["b"]` reuse one parse). Raises
  ## JsonParsingError on invalid JSON.
  if r.dataCache == nil:            # response built without a cache slot
    return parseJson(r.body)
  if r.dataCache[] == nil:
    r.dataCache[] = parseJson(r.body)
  r.dataCache[]
