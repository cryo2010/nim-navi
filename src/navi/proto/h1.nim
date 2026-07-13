## Sans-io HTTP/1.1: pure serialization and an incremental response parser.
##
## No sockets here. Callers serialize a request to bytes, then feed received
## bytes into `H1Parser` until `finished`. This keeps the wire logic identical
## across the sync, asyncdispatch, and chronos backends and unit-testable in
## isolation.

import std/strutils
import ../core/[headers, url, request, response]

proc serializeRequest*(req: Request): string =
  ## Build an origin-form HTTP/1.1 request. Adds Host and Content-Length when
  ## the caller did not supply them; keeps the connection close for phase 1.
  result = $req.verb & " " & req.url.requestTarget & " HTTP/1.1\r\n"
  if not req.headers.contains("host"):
    var hostLine = req.url.host
    let p = req.url.port
    if not ((req.url.isTls and p == 443) or (not req.url.isTls and p == 80)):
      hostLine.add(":" & $p)
    result.add("Host: " & hostLine & "\r\n")
  for (k, v) in req.headers.pairs:
    result.add(k & ": " & v & "\r\n")
  if req.body.len > 0 and not req.headers.contains("content-length"):
    result.add("Content-Length: " & $req.body.len & "\r\n")
  if not req.headers.contains("connection"):
    result.add("Connection: close\r\n")
  result.add("\r\n")
  result.add(req.body)

type
  H1BodyMode = enum
    bmUntilClose, bmLength, bmChunked

  H1State = enum
    stStatusLine, stHeaders, stBody, stChunkSize, stChunkData, stTrailers, stDone

  H1Parser* = object
    state: H1State
    buf: string
    bodyMode: H1BodyMode
    remaining: int          ## bytes left in current length-delimited span
    status: int
    reason: string
    version: string
    headers: Headers
    body: string

proc initH1Parser*(): H1Parser =
  result.state = stStatusLine
  result.bodyMode = bmUntilClose

proc finished*(p: H1Parser): bool {.inline.} = p.state == stDone

proc takeLine(p: var H1Parser, line: var string): bool =
  ## Pop one CRLF-terminated line from the buffer, if a full line is present.
  let idx = p.buf.find("\r\n")
  if idx < 0: return false
  line = p.buf[0 ..< idx]
  p.buf.delete(0 .. idx + 1)
  true

proc parseStatusLine(p: var H1Parser, line: string) =
  # e.g. "HTTP/1.1 200 OK"
  let sp1 = line.find(' ')
  if sp1 < 0: raise newException(ValueError, "malformed status line: " & line)
  p.version = line[0 ..< sp1]
  let rest = line[sp1 + 1 .. ^1]
  let sp2 = rest.find(' ')
  if sp2 < 0:
    p.status = parseInt(rest.strip())
    p.reason = ""
  else:
    p.status = parseInt(rest[0 ..< sp2])
    p.reason = rest[sp2 + 1 .. ^1]
  p.state = stHeaders

proc finishHeaders(p: var H1Parser) =
  let te = p.headers.get("transfer-encoding")
  if te.len > 0 and "chunked" in te.toLowerAscii:
    p.bodyMode = bmChunked
    p.state = stChunkSize
  elif p.headers.contains("content-length"):
    p.bodyMode = bmLength
    p.remaining = parseInt(p.headers.get("content-length").strip())
    p.state = if p.remaining == 0: stDone else: stBody
  else:
    p.bodyMode = bmUntilClose
    p.state = stBody

proc step(p: var H1Parser): bool =
  ## Advance one unit of work; returns false when it needs more bytes.
  case p.state
  of stStatusLine:
    var line: string
    if not p.takeLine(line): return false
    p.parseStatusLine(line)
    true
  of stHeaders:
    var line: string
    if not p.takeLine(line): return false
    if line.len == 0:
      p.finishHeaders()
    else:
      let colon = line.find(':')
      if colon > 0:
        p.headers.add(line[0 ..< colon].strip(), line[colon + 1 .. ^1].strip())
    true
  of stBody:
    case p.bodyMode
    of bmLength:
      let take = min(p.remaining, p.buf.len)
      if take == 0: return false
      p.body.add(p.buf[0 ..< take])
      p.buf.delete(0 ..< take)
      dec p.remaining, take
      if p.remaining == 0: p.state = stDone
      true
    of bmUntilClose:
      if p.buf.len == 0: return false
      p.body.add(p.buf)
      p.buf.setLen(0)
      false # need EOF to terminate; drained for now
    else: false
  of stChunkSize:
    var line: string
    if not p.takeLine(line): return false
    let semi = line.find(';')
    let hex = (if semi < 0: line else: line[0 ..< semi]).strip()
    p.remaining = parseHexInt(hex)
    p.state = if p.remaining == 0: stTrailers else: stChunkData
    true
  of stChunkData:
    if p.buf.len < p.remaining + 2: return false # need data + trailing CRLF
    p.body.add(p.buf[0 ..< p.remaining])
    p.buf.delete(0 ..< p.remaining + 2)
    p.state = stChunkSize
    true
  of stTrailers:
    var line: string
    if not p.takeLine(line): return false
    if line.len == 0: p.state = stDone
    true
  of stDone:
    false

proc feed*(p: var H1Parser, data: openArray[char]) =
  ## Supply received bytes and drive the state machine as far as it can go.
  if data.len > 0:
    let start = p.buf.len
    p.buf.setLen(start + data.len)
    for i in 0 ..< data.len:
      p.buf[start + i] = data[i]
  while p.step(): discard

proc eof*(p: var H1Parser) =
  ## Signal connection close. Completes a body that runs until close.
  if p.state == stBody and p.bodyMode == bmUntilClose:
    p.state = stDone

proc toResponse*(p: H1Parser): Response =
  result.status = p.status
  result.reason = p.reason
  result.httpVersion = p.version
  result.headers = p.headers
  result.body = p.body
