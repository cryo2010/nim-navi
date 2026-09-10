## Sans-io HTTP/1.1: pure serialization and an incremental response parser.
##
## No sockets here. Callers serialize a request to bytes, then feed received
## bytes into `H1Parser` until `finished`. This keeps the wire logic identical
## across the sync, asyncdispatch, and chronos backends and unit-testable in
## isolation.

import std/[strutils, strformat]
import ../core/[headers, url, request, response]

proc serializeHead*(req: Request, chunked = false): string =
  ## Request line, headers, and the terminating blank line (no body). Adds Host
  ## when missing, and either Transfer-Encoding: chunked (streaming upload) or
  ## Content-Length. HTTP/1.1 keeps connections alive by default, which pooling
  ## relies on.
  let target = if req.absoluteForm: req.url.absoluteTarget else: req.url.requestTarget
  result = $req.verb & " " & target & " HTTP/1.1\r\n"
  if not req.headers.contains("host"):
    var hostLine = req.url.hostLiteral   # IPv6 literals stay bracketed
    let p = req.url.port
    if not ((req.url.isTls and p == 443) or (not req.url.isTls and p == 80)):
      hostLine.add(":" & $p)
    result.add("Host: " & hostLine & "\r\n")
  for (k, v) in req.headers.pairs:
    result.add(k & ": " & v & "\r\n")
  if chunked:
    if not req.headers.contains("transfer-encoding"):
      result.add("Transfer-Encoding: chunked\r\n")
    # Advertise which fields arrive as trailers (RFC 9110 6.6.2). Recommended so an
    # intermediary keeps them; only added when the caller did not set it themselves.
    if req.trailers.len > 0 and not req.headers.contains("trailer"):
      var names: seq[string]
      for (k, _) in req.trailers.pairs: names.add(k)
      result.add("Trailer: " & names.join(", ") & "\r\n")
  elif req.body.len > 0 and not req.headers.contains("content-length"):
    result.add("Content-Length: " & $req.body.len & "\r\n")
  result.add("\r\n")

proc serializeRequest*(req: Request): string =
  ## Full request with a buffered body.
  serializeHead(req) & req.body

const chunkTerminator* = "0\r\n\r\n"

proc encodeChunk*(data: string): string =
  ## One HTTP/1.1 chunked-transfer frame: `<hex-size>\r\n<data>\r\n`. `data` must be
  ## non-empty. Built into a single preallocated buffer (the payload is copied once)
  ## rather than chained `&` temporaries, since this runs per chunk of a streamed
  ## upload.
  let hex = fmt"{data.len:X}"
  result = newStringOfCap(hex.len + data.len + 4)
  result.add hex
  result.add "\r\n"
  result.add data
  result.add "\r\n"

proc finalChunk*(req: Request): string =
  ## The terminating zero-length chunk plus any request trailer fields (RFC 9110
  ## 7.1.2). With no trailers this is exactly `chunkTerminator` ("0\r\n\r\n").
  result = "0\r\n"
  for (k, v) in req.trailers.pairs:
    result.add(k & ": " & v & "\r\n")
  result.add("\r\n")

type
  H1BodyMode = enum
    bmUntilClose, bmLength, bmChunked

  H1State = enum
    stStatusLine, stHeaders, stBody, stChunkSize, stChunkData, stTrailers, stDone

  H1Parser* = object
    state: H1State
    buf: string
    pos: int                ## read cursor into `buf`: bytes consumed but not yet
                            ## dropped. Consuming advances `pos`; `feed` compacts the
                            ## consumed prefix in one shift (like h2's FrameDecoder),
                            ## avoiding an O(lines x bodyBytes) front-`delete` memmove.
    bodyMode: H1BodyMode
    remaining: int          ## bytes left in current length-delimited span
    status: int
    reason: string
    version: string
    headers: Headers
    trailers: Headers       ## trailing fields after a chunked body (RFC 9110 7.1.2)
    body: string
    streaming: bool         ## when set, body bytes accumulate in `pending` for
                            ## the engine to drain and hand to a sink, not `body`
    pending: string         ## streaming body received since the last `takeBody`
    headRequest: bool       ## response is to a HEAD request -> never has a body

proc initH1Parser*(streaming = false, headRequest = false): H1Parser =
  result.state = stStatusLine
  result.bodyMode = bmUntilClose
  result.streaming = streaming
  result.headRequest = headRequest

proc emitBody(p: var H1Parser, chunk: string) =
  if p.streaming:
    p.pending.add(chunk)
  else:
    p.body.add(chunk)

proc takeBody*(p: var H1Parser): string =
  ## Move the streaming body received so far out of the parser, leaving it empty.
  ## The engine drains this per feed, decodes it, and hands it to the sink, so raw
  ## body bytes never accumulate whole in the parser.
  result = move(p.pending)

proc contentEncoding*(p: H1Parser): string =
  ## The response's Content-Encoding (once headers are parsed); "" if absent. The
  ## engine uses it to build a streaming decoder for the body it drains.
  p.headers.get("content-encoding")

proc finished*(p: H1Parser): bool {.inline.} = p.state == stDone

proc headersReady*(p: H1Parser): bool {.inline.} =
  ## True once the status line and all headers are parsed (the body may still be
  ## pending). Lets a streaming caller inspect status/headers, via `toResponse`,
  ## before it starts draining the body.
  p.state notin {stStatusLine, stHeaders}

proc takeLine(p: var H1Parser, line: var string): bool =
  ## Pop one CRLF-terminated line from the buffer, if a full line is present.
  ## Scans from the read cursor; consuming only advances `pos` (no memmove).
  let idx = p.buf.find("\r\n", start = p.pos)
  if idx < 0: return false
  line = p.buf[p.pos ..< idx]
  p.pos = idx + 2
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
  if p.status in 100 .. 199:
    # Interim response (100 Continue, 103 Early Hints, ...): it has no body, and
    # its status/headers are not the final response (RFC 9110 15.2). Drop them and
    # read the final response that follows on the same connection.
    p.status = 0
    p.reason = ""
    p.headers = initHeaders()
    p.state = stStatusLine
    return
  if p.headRequest or p.status == 204 or p.status == 304:
    # No message body regardless of Content-Length / Transfer-Encoding: a
    # response to HEAD (RFC 9110 9.3.2), or a 204/304 (RFC 9110 15.3.5/15.4.5).
    # Without this the parser would block waiting for a body the server, being
    # correct, never sends -- which for HEAD on a keep-alive connection hangs.
    p.bodyMode = bmLength     # self-delimited (zero length): stays keep-alive reusable
    p.state = stDone
    return
  let te = p.headers.get("transfer-encoding")
  let hasCl = p.headers.contains("content-length")
  if te.len > 0:
    # RFC 9112 6.1: chunked must be the *final* transfer coding. A value that merely
    # contains "chunked" as a substring, or where chunked is not last, is not chunk-
    # framed -- treating it as chunked (the old substring test) would misframe the
    # body. Tokenize and only trust chunked when it is the final coding.
    var codings: seq[string]
    for tok in te.toLowerAscii.split(','): codings.add tok.strip()
    if codings.len > 0 and codings[^1] == "chunked":
      # RFC 9112 6.1/6.3: chunked framing together with a Content-Length is the
      # classic request-smuggling ambiguity. Reject rather than silently prefer one
      # and pool a possibly-poisoned connection.
      if hasCl:
        raise newException(ValueError, "h1: both Transfer-Encoding: chunked and Content-Length")
      p.bodyMode = bmChunked
      p.state = stChunkSize
    else:
      # Transfer-Encoding present but chunked is not final: RFC 9112 6.3 says the body
      # runs until the connection closes (Transfer-Encoding overrides Content-Length),
      # and such a connection is not reusable (keepAliveAfter rejects bmUntilClose).
      p.bodyMode = bmUntilClose
      p.state = stBody
  elif hasCl:
    # RFC 9112 6.3: multiple Content-Length values must agree (a conflict is a framing
    # error / smuggling vector); collapse duplicates, reject a conflict.
    var clVal = ""
    for v in p.headers.getAll("content-length"):
      let s = v.strip()
      if clVal.len == 0: clVal = s
      elif s != clVal:
        raise newException(ValueError, "h1: conflicting Content-Length values")
    # RFC 9112 6.3: Content-Length is 1*DIGIT. parseInt would accept a leading '+' (a
    # smuggling differential) and the old negative guard only caught '-'. Require pure
    # digits; parseInt still raises (caught upstream) on an overflowing value.
    if clVal.len == 0 or not clVal.allCharsInSet({'0' .. '9'}):
      raise newException(ValueError, "h1: invalid Content-Length")
    p.remaining = parseInt(clVal)
    p.bodyMode = bmLength
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
    let avail = p.buf.len - p.pos
    case p.bodyMode
    of bmLength:
      let take = min(p.remaining, avail)
      if take == 0: return false
      p.emitBody(p.buf[p.pos ..< p.pos + take])
      p.pos += take
      dec p.remaining, take
      if p.remaining == 0: p.state = stDone
      true
    of bmUntilClose:
      if avail == 0: return false
      p.emitBody(p.buf[p.pos ..< p.buf.len])
      p.pos = p.buf.len
      false # need EOF to terminate; drained for now
    else: false
  of stChunkSize:
    var line: string
    if not p.takeLine(line): return false
    let semi = line.find(';')
    let hex = (if semi < 0: line else: line[0 ..< semi]).strip()
    p.remaining = parseHexInt(hex)
    # parseHexInt wraps on overflow; a negative or absurd size would slice out
    # of bounds (RangeDefect) or overflow `remaining + 2`. Reject it as a
    # catchable error (fuzz-found). 1 shl 40 is far above any real chunk.
    if p.remaining < 0 or p.remaining > (1 shl 40):
      raise newException(ValueError, "h1: invalid chunk size")
    p.state = if p.remaining == 0: stTrailers else: stChunkData
    true
  of stChunkData:
    if p.buf.len - p.pos < p.remaining + 2: return false # need data + trailing CRLF
    # RFC 9112 7.1: chunk-data is terminated by CRLF. Verify it instead of blindly
    # consuming two bytes -- a missing CRLF is a framing desync that would otherwise
    # deliver a corrupted body and could leave the pooled connection poisoned.
    if p.buf[p.pos + p.remaining] != '\r' or p.buf[p.pos + p.remaining + 1] != '\n':
      raise newException(ValueError, "h1: chunk data not terminated by CRLF")
    p.emitBody(p.buf[p.pos ..< p.pos + p.remaining])
    p.pos += p.remaining + 2
    p.state = stChunkSize
    true
  of stTrailers:
    var line: string
    if not p.takeLine(line): return false
    if line.len == 0:
      p.state = stDone
    else:
      let colon = line.find(':')
      if colon > 0:
        p.trailers.add(line[0 ..< colon].strip(), line[colon + 1 .. ^1].strip())
    true
  of stDone:
    false

proc feed*(p: var H1Parser, data: openArray[char]) =
  ## Supply received bytes and drive the state machine as far as it can go.
  if p.pos > 0:                          # drop the consumed prefix in one shift
    if p.pos >= p.buf.len: p.buf.setLen(0)
    else: p.buf = p.buf[p.pos .. ^1]
    p.pos = 0
  if data.len > 0:
    let start = p.buf.len
    p.buf.setLen(start + data.len)
    copyMem(addr p.buf[start], unsafeAddr data[0], data.len)
  while p.step(): discard

proc eof*(p: var H1Parser) =
  ## Signal connection close. Completes a body that runs until close.
  if p.state == stBody and p.bodyMode == bmUntilClose:
    p.state = stDone

proc keepAliveAfter*(p: H1Parser): bool =
  ## Whether the connection can be reused once this response is fully read.
  ## Requires the response to be fully consumed (`stDone`) -- reusing a connection
  ## whose body was not completely read leaves those bytes on the wire, so the next
  ## request on it parses stale body as its status line (a short read on a pooled
  ## keep-alive connection is exactly this case). Also requires a self-delimited body
  ## (one that ends only at connection close cannot be pooled) and an HTTP/1.1 peer
  ## that did not ask to close.
  if p.state != stDone: return false
  if p.bodyMode == bmUntilClose: return false
  if p.version != "HTTP/1.1": return false
  # RFC 9110 5.3: a field may be split across multiple lines with the same semantics
  # as one comma-joined value. `get` returns only the first, so a peer that sends
  # `Connection: keep-alive` then `Connection: close` would look reusable. Inspect
  # every value.
  for v in p.headers.getAll("connection"):
    if "close" in v.toLowerAscii: return false
  true

proc trailers*(p: H1Parser): Headers =
  ## Trailing header fields received after a chunked body (empty if none).
  p.trailers

proc toResponse*(p: H1Parser): Response =
  result = initResponse(p.status, p.reason, p.version, p.headers, p.body)
  result.trailers = p.trailers
