## Conversions between navi's Request/Response and the sans-io h2 connection.

import std/strutils
import ./headers, ./url, ./request, ./response
import ../proto/h2/[conn, hpack]

proc h2HeaderList*(req: Request): seq[HeaderPair] =
  ## Pseudo-headers first, then regular headers (lowercased, connection-specific
  ## fields dropped, Host replaced by :authority).
  result.add((":method", $req.verb))
  result.add((":scheme", if req.url.isTls: "https" else: "http"))
  result.add((":path", req.url.requestTarget))
  var authority = req.url.host
  let p = req.url.port
  if not ((req.url.isTls and p == 443) or (not req.url.isTls and p == 80)):
    authority.add(":" & $p)
  result.add((":authority", authority))
  for (name, value) in req.headers.pairs:
    let lower = name.toLowerAscii
    if lower in ["host", "connection", "keep-alive", "proxy-connection",
                 "transfer-encoding", "upgrade"]:
      continue
    result.add((lower, value))

proc toResponse*(r: H2Response): Response =
  result.status = r.status
  result.httpVersion = "HTTP/2"
  result.body = r.body
  for (name, value) in r.headers:
    result.headers.add(name, value)
