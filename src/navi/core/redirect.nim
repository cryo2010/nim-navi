## Redirect handling: deciding whether to follow and rewriting the request.

import ./headers, ./url, ./request

proc isRedirect*(status: int): bool =
  status in [301, 302, 303, 307, 308]

proc shouldFollowRedirect*(status, hops, limit: int; location: string): bool =
  ## Whether a completed response should be auto-followed as a redirect: the
  ## redirect limit is enabled and unspent, the status is a redirect, and a
  ## Location was supplied. This is the gate shared by every request path (the
  ## buffered engine, the streaming pull paths, and the parallel batch loop).
  ## A non-replayable streamed body (307/308) is a separate concern the caller
  ## guards before rewriting; a batch is GET-only and never carries one.
  limit > 0 and hops < limit and isRedirect(status) and location.len > 0

proc dropBody(r: var Request) =
  ## Clear the request body when a redirect rewrites the method to GET; the
  ## trailers belong to the dropped body and go with it.
  r.body = ""
  r.bodyStream = nil
  r.trailers = initHeaders()

proc redirectRequest*(req: Request, status: int, location: string): Request =
  ## Build the follow-up request for a redirect response, applying the usual
  ## method rewrites and stripping Authorization when the origin changes.
  result = req
  let previousOrigin = req.url.originKey
  result.url = resolve(req.url, location)
  result.headers.del("cookie") # recomputed from the jar for the new target
  if result.url.originKey != previousOrigin:
    # Credentials are origin-scoped: never carry them to a different origin.
    # Proxy-Authorization is stripped too -- a redirect can point at a host the
    # proxy reaches directly, so its credentials must not leak downstream.
    result.headers.del("authorization")
    result.headers.del("proxy-authorization")
  case status
  of 303:
    # 303 See Other always continues with GET and no body.
    result.verb = GET
    result.dropBody()
  of 301, 302:
    # A non-idempotent method degrades to GET (matching fetch/browsers).
    if req.verb notin {GET, HEAD}:
      result.verb = GET
      result.dropBody()
  else:
    discard # 307/308 preserve method and body
