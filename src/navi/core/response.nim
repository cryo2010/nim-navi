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

  HttpError* = object of CatchableError
    ## Raised for non-2xx responses when `throwHttpErrors` is on (the default).
    ## The full response is attached for inspection.
    response*: Response

proc ok*(r: Response): bool {.inline.} =
  ## True for 2xx status codes.
  r.status >= 200 and r.status < 300

proc text*(r: Response): string {.inline.} = r.body

proc bytes*(r: Response): seq[byte] =
  result = newSeq[byte](r.body.len)
  for i, c in r.body:
    result[i] = byte(c)

proc json*(r: Response): JsonNode =
  ## Parse the body as JSON. Raises JsonParsingError on malformed input.
  parseJson(r.body)
