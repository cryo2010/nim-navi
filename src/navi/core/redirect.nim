## Redirect handling: deciding whether to follow and rewriting the request.

import ./headers, ./url, ./request

proc isRedirect*(status: int): bool =
  status in [301, 302, 303, 307, 308]

proc redirectRequest*(req: Request, status: int, location: string): Request =
  ## Build the follow-up request for a redirect response, applying the usual
  ## method rewrites and stripping Authorization when the origin changes.
  result = req
  let previousOrigin = req.url.originKey
  result.url = resolve(req.url, location)
  result.headers.del("cookie") # recomputed from the jar for the new target
  if result.url.originKey != previousOrigin:
    result.headers.del("authorization")
  case status
  of 303:
    # 303 See Other always continues with GET and no body.
    result.verb = GET
    result.body = ""
    result.bodyStream = nil
  of 301, 302:
    # A non-idempotent method degrades to GET (matching fetch/browsers).
    if req.verb notin {GET, HEAD}:
      result.verb = GET
      result.body = ""
      result.bodyStream = nil
  else:
    discard # 307/308 preserve method and body
