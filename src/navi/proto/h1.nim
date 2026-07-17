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
    var hostLine = req.url.host
    let p = req.url.port
    if not ((req.url.isTls and p == 443) or (not req.url.isTls and p == 80)):
      hostLine.add(":" & $p)
    result.add("Host: " & hostLine & "\r\n")
  for (k, v) in req.headers.pairs:
    result.add(k & ": " & v & "\r\n")
  if chunked:
    if not req.headers.contains("transfer-encoding"):
      result.add("Transfer-Encoding: chunked\r\n")
  elif req.body.len > 0 and not req.headers.contains("content-length"):
    result.add("Content-Length: " & $req.body.len & "\r\n")
  result.add("\r\n")

proc serializeRequest*(req: Request): string =
  ## Full request with a buffered body.
  serializeHead(req) & req.body

const chunkTerminator* = "0\r\n\r\n"

proc encodeChunk*(data: string): string =
  ## One HTTP/1.1 chunked-transfer frame. `data` must be non-empty.
  fmt"{data.len:X}" & "\r\n" & data & "\r\n"

type
  H1BodyMode = enum
    bmUntilClose, bmLength, bmChunked

  H1State = enum
    stStatusLine, stHeaders, stBody, stChunkSize, stChunkData, stTrailers, stDone

  SinkFactory* = proc(headers: Headers): BodySink {.closure, raises: [CatchableError].}
    ## Chooses the body sink once response headers are known (e.g. to wrap it in
    ## a content-encoding decoder). Keeps the parser ignorant of decompression.

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
    sink: BodySink          ## when set, body bytes go here instead of `body`
    sinkFactory: SinkFactory ## when set, builds `sink` from the parsed headers

proc initH1Parser*(sink: BodySink = nil, sinkFactory: SinkFactory = nil): H1Parser =
  result.state = stStatusLine
  result.bodyMode = bmUntilClose
  result.sink = sink
  result.sinkFactory = sinkFactory

proc emitBody(p: var H1Parser, chunk: string) =
  if p.sink != nil:
    # navi runs on a single thread (one event loop, or blocking sync), so the
    # user's sink need not be marked gcsafe for chronos's gcsafe async procs.
    {.cast(gcsafe).}:
      p.sink(chunk.toOpenArrayByte(0, chunk.high))
  else:
    p.body.add(chunk)

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
  if p.sinkFactory != nil:      # now that headers are known, choose the sink
    # single-threaded client; the factory need not be gcsafe (see emitBody).
    {.cast(gcsafe).}:
      p.sink = p.sinkFactory(p.headers)
  let te = p.headers.get("transfer-encoding")
  if te.len > 0 and "chunked" in te.toLowerAscii:
    p.bodyMode = bmChunked
    p.state = stChunkSize
  elif p.headers.contains("content-length"):
    p.bodyMode = bmLength
    p.remaining = parseInt(p.headers.get("content-length").strip())
    # A peer controls this; a negative length would slice out of bounds (a
    # RangeDefect crash). Reject it as a catchable error instead (found by
    # tests/fuzz). parseInt already rejects non-numeric and overflowing values.
    if p.remaining < 0:
      raise newException(ValueError, "h1: negative Content-Length")
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
      p.emitBody(p.buf[0 ..< take])
      p.buf.delete(0 ..< take)
      dec p.remaining, take
      if p.remaining == 0: p.state = stDone
      true
    of bmUntilClose:
      if p.buf.len == 0: return false
      p.emitBody(p.buf)
      p.buf.setLen(0)
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
    if p.buf.len < p.remaining + 2: return false # need data + trailing CRLF
    p.emitBody(p.buf[0 ..< p.remaining])
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

proc keepAliveAfter*(p: H1Parser): bool =
  ## Whether the connection can be reused once this response is fully read.
  ## Requires a self-delimited body (a body that ends only at connection close
  ## cannot be pooled) and an HTTP/1.1 peer that did not ask to close.
  if p.bodyMode == bmUntilClose: return false
  if p.version != "HTTP/1.1": return false
  "close" notin p.headers.get("connection").toLowerAscii

proc toResponse*(p: H1Parser): Response =
  initResponse(p.status, p.reason, p.version, p.headers, p.body)
