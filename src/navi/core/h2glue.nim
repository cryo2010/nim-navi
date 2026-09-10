## Conversions between navi's Request/Response and the sans-io h2 connection.

import std/strutils
import ./headers, ./url, ./request, ./response
import ../proto/h2/[conn, hpack]

proc appendMappedHeaders(result: var seq[HeaderPair], headers: Headers) =
  ## Append user headers lowercased, dropping everything that must not cross to h2
  ## (RFC 9113 8.2.2): the connection-specific fields, any field a `Connection` header
  ## nominates, a user pseudo-header (name starting ':', which would land after the
  ## regular fields and be malformed), and a `TE` that is not exactly "trailers".
  var nominated: seq[string]
  for (name, value) in headers.pairs:
    if name.toLowerAscii == "connection":
      for tok in value.split(','):
        let t = tok.strip.toLowerAscii
        if t.len > 0: nominated.add t
  for (name, value) in headers.pairs:
    let lower = name.toLowerAscii
    if lower.len == 0 or lower[0] == ':': continue
    if lower in ["host", "connection", "keep-alive", "proxy-connection",
                 "transfer-encoding", "upgrade"]:
      continue
    if lower in nominated: continue
    if lower == "te" and value.strip.toLowerAscii != "trailers": continue
    result.add((lower, value))

proc h2HeaderList*(req: Request): seq[HeaderPair] =
  ## Pseudo-headers first, then regular headers (lowercased, connection-specific and
  ## connection-nominated fields dropped, Host replaced by :authority).
  result.add((":method", $req.verb))
  result.add((":scheme", if req.url.isTls: "https" else: "http"))
  result.add((":path", req.url.requestTarget))
  var authority = req.url.host
  let p = req.url.port
  if not ((req.url.isTls and p == 443) or (not req.url.isTls and p == 80)):
    authority.add(":" & $p)
  result.add((":authority", authority))
  result.appendMappedHeaders(req.headers)

proc h2ConnectHeaderList*(url: Url, protocol: string, extra: Headers): seq[HeaderPair] =
  ## Pseudo-headers for an Extended CONNECT (RFC 8441): `:method` is CONNECT with a
  ## `:protocol` (e.g. "websocket"). Unlike a plain CONNECT, `:scheme` and `:path`
  ## are kept (RFC 8441 4) so the request addresses a resource. This is the
  ## WebSocket-over-h2 handshake; `extra` carries sec-websocket-version and any
  ## subprotocol/extension fields (Sec-WebSocket-Key/Accept are NOT used over h2).
  result.add((":method", "CONNECT"))
  result.add((":protocol", protocol))
  result.add((":scheme", if url.isTls: "https" else: "http"))
  result.add((":path", url.requestTarget))
  var authority = url.host
  let p = url.port
  if not ((url.isTls and p == 443) or (not url.isTls and p == 80)):
    authority.add(":" & $p)
  result.add((":authority", authority))
  result.appendMappedHeaders(extra)

proc h2TrailerList*(req: Request): seq[HeaderPair] =
  ## Request trailer fields as HPACK pairs (lowercased names). Pseudo-headers and
  ## fields that are meaningless or forbidden in a trailer section (framing,
  ## routing, and `Trailer` itself, RFC 9110 6.5.1) are dropped.
  for (name, value) in req.trailers.pairs:
    let lower = name.toLowerAscii
    if lower.len == 0 or lower[0] == ':': continue
    if lower in ["host", "connection", "keep-alive", "proxy-connection",
                 "transfer-encoding", "upgrade", "content-length", "te", "trailer"]:
      continue
    result.add((lower, value))

proc toResponse*(r: H2Response): Response =
  var headers: Headers
  for (name, value) in r.headers:
    headers.add(name, value)
  result = initResponse(r.status, "", "HTTP/2", headers, r.body)
  for (name, value) in r.trailers:
    result.trailers.add(name, value)

