## Sans-io HTTP/1.1: pure serialization and an incremental response parser.
##
## No sockets here. Callers serialize a request to bytes, then feed received
## bytes into `H1Parser` until `finished`. This keeps the wire logic identical
## across the sync, asyncdispatch, and chronos backends and unit-testable in
## isolation.

import std/strutils
import ../core/[headers, url, request, response]

proc serializeHead*(req: Request, chunked = false): string =
  ## Request line, headers, and the terminating blank line (no body). Adds Host
  ## when missing, and either Transfer-Encoding: chunked (streaming upload) or
  ## Content-Length. HTTP/1.1 keeps connections alive by default, which pooling
  ## relies on.
  # navi owns transfer framing: a streamed body (`bodyStream`) or trailers select the
  # chunked path (`chunked = true`, which frames the body and adds the header). A caller
  # must not set Transfer-Encoding by hand -- on the buffered path (`chunked = false`)
  # the header loop below would advertise it while the body is written unframed, a
  # connection-desyncing / request-smuggling footgun (#273). Reject rather than emit it.
  if not chunked and req.headers.contains("transfer-encoding"):
    raise newException(ValueError,
      "navi: use a streaming body (bodyStream) for chunked transfer; " &
      "do not set a Transfer-Encoding request header manually")
  # navi owns the length too: on the chunked path a caller-supplied Content-Length is
  # dropped rather than emitted next to Transfer-Encoding: chunked. Both framing
  # headers on one request is the CL.TE smuggling ambiguity (RFC 9112 6.1 tells a
  # recipient to ignore the length, but intermediaries disagree in practice), and the
  # length is wrong anyway: the real body is the producer's, whose size is unknown
  # here. Stripping matches h3, which already drops it via h3SkipHeaders (#294).
  let target = if req.absoluteForm: req.url.absoluteTarget else: req.url.requestTarget
  result = $req.verb & " " & target & " HTTP/1.1\r\n"
  if not req.headers.contains("host"):
    var hostLine = req.url.hostLiteral   # IPv6 literals stay bracketed
    let p = req.url.port
    if not ((req.url.isTls and p == 443) or (not req.url.isTls and p == 80)):
      hostLine.add(":" & $p)
    result.add("Host: " & hostLine & "\r\n")
  for (k, v) in req.headers.pairs:
    if chunked and cmpIgnoreCase(k, "content-length") == 0: continue
    result.add(k & ": " & v & "\r\n")
  if chunked:
    if not req.headers.contains("transfer-encoding"):
      result.add("Transfer-Encoding: chunked\r\n")
    # Advertise which fields arrive as trailers (RFC 9110 6.6.2). Recommended so an
    # intermediary keeps them; only added when the caller did not set it themselves.
    # Only the names `finalChunk` will actually emit are listed: a forbidden trailer
    # name is filtered there (#296), so advertising it would promise a field that
    # never arrives.
    if req.trailers.len > 0 and not req.headers.contains("trailer"):
      var names: seq[string]
      for (k, _) in req.trailers.pairs:
        if not isForbiddenTrailer(k): names.add(k)
      if names.len > 0:
        result.add("Trailer: " & names.join(", ") & "\r\n")
  elif not req.headers.contains("content-length"):
    if req.body.len > 0:
      result.add("Content-Length: " & $req.body.len & "\r\n")
    elif req.verb in {POST, PUT, PATCH}:
      # An empty body for a method that normally carries one: send an explicit
      # Content-Length: 0 so servers/WAFs that require a length don't stall or 411 on
      # a bodyless POST/PUT/PATCH (#274). GET/HEAD/etc. carry no length by default.
      result.add("Content-Length: 0\r\n")
  result.add("\r\n")

proc serializeRequest*(req: Request): string =
  ## Full request with a buffered body.
  serializeHead(req) & req.body

const chunkTerminator* = "0\r\n\r\n"

const h1CoalesceSize* = 16 * 1024
  ## Cap on the streamed-upload write buffer (#299). A producer that yields many tiny
  ## chunks would otherwise cost one socket write -- and, under TLS, one record with
  ## its own header and MAC -- per chunk. The send loop buffers raw body bytes and
  ## flushes what it holds as a single chunk BEFORE an append would take the buffer
  ## past this size, so a framed buffer never exceeds it; a producer chunk already
  ## this large is framed and written on its own. 16 KiB is the maximum TLS record
  ## payload, so a full buffer always fits one record.

const hexDigits = "0123456789ABCDEF"

proc addChunkSize(buf: var string, n: Positive) =
  ## Append `n` as an uppercase hex chunk-size, written digit by digit straight into
  ## `buf`. `fmt"{n:X}"` would allocate a throwaway string per chunk of a streamed
  ## upload, which is the one allocation a chunk header does not need.
  var digits {.noinit.}: array[16, char]        # 16 nibbles spans the whole int range
  var i = 0
  var v = int(n)
  while v > 0:
    digits[i] = hexDigits[v and 0xf]
    inc i
    v = v shr 4
  while i > 0:
    dec i
    buf.add digits[i]

proc addChunk*(buf: var string, data: string) =
  ## Append one HTTP/1.1 chunked-transfer frame for `data` to `buf`. Lets the send
  ## loop pack the last chunk and the terminator into a single write without an
  ## intermediate per-chunk string. An empty `data` appends nothing: an empty chunk
  ## would encode as "0\r\n\r\n", a premature body terminator (#274).
  if data.len == 0: return
  buf.addChunkSize(data.len)
  buf.add "\r\n"
  buf.add data
  buf.add "\r\n"

proc encodeChunk*(data: string): string =
  ## One HTTP/1.1 chunked-transfer frame: `<hex-size>\r\n<data>\r\n`. `data` must be
  ## non-empty. Built into a single preallocated buffer (the payload is copied once,
  ## the size written in place) rather than chained `&` temporaries, since this runs
  ## per chunk of a streamed upload.
  if data.len == 0: return ""
  result = newStringOfCap(data.len + 20)
  result.addChunk(data)

proc finalChunk*(req: Request): string =
  ## The terminating zero-length chunk plus any request trailer fields (RFC 9110
  ## 7.1.2). With no trailers this is exactly `chunkTerminator` ("0\r\n\r\n").
  ##
  ## Fields that must not appear in a trailer section (framing and routing fields,
  ## pseudo-headers, `Trailer` itself) are dropped through the shared
  ## `isForbiddenTrailer`, the same filter h2 and h3 apply. Without it a
  ## `Content-Length` or `Transfer-Encoding` smuggled in as a trailer would be written
  ## straight after the zero chunk, where a lenient intermediary may act on it (#296).
  result = "0\r\n"
  for (k, v) in req.trailers.pairs:
    if isForbiddenTrailer(k): continue
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
                            ## dropped. Consuming advances `pos`; `feed` reclaims the
                            ## consumed prefix periodically (see `compact`), like h2's
                            ## FrameDecoder, avoiding the O(lines x bodyBytes) memmove
                            ## a front `delete` per consumed span costs.
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
    sawInterim: bool        ## a 1xx interim response (100 Continue / 103 Early Hints)
                            ## has arrived, so the peer demonstrably began responding even
                            ## before the final headers -- mirrors h2's `responseBegan`,
                            ## so a later drop is a truncation, not a keep-alive race

proc initH1Parser*(streaming = false, headRequest = false): H1Parser =
  result.state = stStatusLine
  result.bodyMode = bmUntilClose
  result.streaming = streaming
  result.headRequest = headRequest

proc addRange(dst: var string, src: string, first, n: int) {.inline.} =
  ## Append `src[first ..< first + n]` to `dst` without materializing the slice as
  ## its own string first. Body bytes are copied out of the parse buffer once per
  ## read on the hot path, so the slice temporary is pure overhead.
  if n <= 0: return
  let start = dst.len
  dst.setLen(start + n)
  copyMem(addr dst[start], unsafeAddr src[first], n)

proc emitBody(p: var H1Parser, first, n: int) =
  ## Hand `buf[first ..< first + n]` to the body sink for this parser's mode.
  if p.streaming:
    p.pending.addRange(p.buf, first, n)
  else:
    p.body.addRange(p.buf, first, n)

proc setStreaming*(p: var H1Parser, streaming: bool) =
  ## Flip the streaming flag after the headers are in (the gated-drain path decides
  ## whether to stream only once status/headers are known). Turning streaming OFF
  ## migrates any body bytes that arrived alongside the headers from `pending` (where
  ## the streaming `emitBody` routed them) into `body`, so the buffered drain below
  ## and `toResponse` see them; from here `emitBody` appends to `body`. Turning it ON
  ## does the reverse move so a later `takeBody` delivers them.
  if p.streaming == streaming: return
  if streaming:
    p.pending.add(move(p.body))
  else:
    p.body.add(move(p.pending))
  p.streaming = streaming

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

proc responseBegan*(p: H1Parser): bool {.inline.} =
  ## True once the peer has sent ANY response -- a 1xx interim (100/103/...) or the
  ## final headers. Mirrors the h2 `responseBegan`: used to classify a connection drop.
  ## A close after the final headers is truncation (`headersReady` alone catches that),
  ## but a close after ONLY a 1xx interim (which the parser discards) must also count as
  ## "the peer began responding," so it is not misread as a safe keep-alive race.
  p.sawInterim or p.headersReady

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
    # read the final response that follows on the same connection. Record that the peer
    # began responding (`responseBegan`): a subsequent close before the FINAL headers is
    # then a truncation, not a keep-alive race -- the peer demonstrably started replying.
    p.sawInterim = true
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
      let (name, value, ok) = parseHeaderLine(line)
      if ok:
        p.headers.add(name, value)
    true
  of stBody:
    let avail = p.buf.len - p.pos
    case p.bodyMode
    of bmLength:
      let take = min(p.remaining, avail)
      if take == 0: return false
      p.emitBody(p.pos, take)
      p.pos += take
      dec p.remaining, take
      if p.remaining == 0: p.state = stDone
      true
    of bmUntilClose:
      if avail == 0: return false
      p.emitBody(p.pos, avail)
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
    # catchable error (fuzz-found). The bound is far above any real chunk; use an
    # int64 literal so it is valid on a 32-bit `int` build too (`1 shl 40` overflows
    # a 32-bit int) (#274).
    const maxChunkSize = 1'i64 shl 40
    if p.remaining < 0 or p.remaining.int64 > maxChunkSize:
      raise newException(ValueError, "h1: invalid chunk size")
    p.state = if p.remaining == 0: stTrailers else: stChunkData
    true
  of stChunkData:
    # Emit what of the chunk has arrived instead of waiting for all of it: a server
    # is free to declare one multi-megabyte chunk, and buffering it whole would hold
    # that chunk in the parser even on the streaming path, where the size cap
    # (applied to emitted bytes) could then never fire to stop it. Chunk boundaries
    # are framing, not delivery units (RFC 9112 7.1).
    #
    # Incremental delivery stops one byte short, though: the LAST byte of a chunk is
    # held back until its terminating CRLF has been verified, so no chunk is ever
    # fully delivered on the strength of a size line alone. A desynced or smuggled
    # chunk therefore cannot land complete in a sink before the error is raised, and
    # what a streaming consumer did receive is always a strict prefix of the chunk,
    # exactly as a body cut short by a dropped connection is.
    let avail = p.buf.len - p.pos
    if avail >= p.remaining + 2:
      # The rest of the chunk and its terminator are both here. RFC 9112 7.1:
      # chunk-data is terminated by CRLF. Verify it instead of blindly consuming two
      # bytes -- a missing CRLF is a framing desync that would otherwise deliver a
      # corrupted body and could leave the pooled connection poisoned. Raising here
      # leaves the parser short of stDone, so `keepAliveAfter` refuses the connection.
      if p.buf[p.pos + p.remaining] != '\r' or p.buf[p.pos + p.remaining + 1] != '\n':
        raise newException(ValueError, "h1: chunk data not terminated by CRLF")
      p.emitBody(p.pos, p.remaining)
      p.pos += p.remaining + 2
      p.remaining = 0
      p.state = stChunkSize
      true
    else:
      # Still mid-chunk: deliver everything except the final byte. `remaining` never
      # reaches 0 on this path, so the terminator check above is the only way out of
      # a chunk. A 1-byte chunk delivers nothing here, by the same rule.
      let take = min(avail, p.remaining - 1)
      if take <= 0: return false
      p.emitBody(p.pos, take)
      p.pos += take
      dec p.remaining, take
      true
  of stTrailers:
    var line: string
    if not p.takeLine(line): return false
    if line.len == 0:
      p.state = stDone
    else:
      let (name, value, ok) = parseHeaderLine(line)
      if ok:
        p.trailers.add(name, value)
    true
  of stDone:
    false

const h1CompactMin = 8 * 1024
  ## Smallest consumed prefix worth shifting the retained bytes for (see `compact`).

proc compact(p: var H1Parser) =
  ## Reclaim the consumed prefix of the parse buffer. Fully consumed is the common
  ## case (a read that ends on a frame boundary) and costs nothing but a `setLen`.
  ## Otherwise the retained bytes are moved down IN PLACE, and only once the prefix
  ## is both worth reclaiming and at least as large as what the move copies -- so
  ## shifting stays amortized O(1) per byte rather than re-copying a large retained
  ## remainder on every feed to drop a few consumed header bytes.
  if p.pos == 0: return
  if p.pos >= p.buf.len:
    p.buf.setLen(0)
  elif p.pos >= h1CompactMin and p.pos >= p.buf.len - p.pos:
    let keep = p.buf.len - p.pos
    moveMem(addr p.buf[0], addr p.buf[p.pos], keep)
    p.buf.setLen(keep)
  else:
    return                               # leave the prefix; the cursor skips it
  p.pos = 0

proc feed*(p: var H1Parser, data: openArray[char]) =
  ## Supply received bytes and drive the state machine as far as it can go.
  p.compact()
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
  # Only pool an HTTP/1.1 peer. HTTP/1.0 keep-alive (via `Connection: keep-alive`) is
  # spec-permitted (RFC 9112 6.3) but notoriously ambiguous through proxies, so we
  # deliberately decline to reuse a 1.0 connection rather than risk a desync (#274).
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
  # Trailers are surfaced separately (`result.trailers`), never merged into the header
  # set: RFC 9110 6.5.1 forbids blindly merging trailing fields, and keeping them apart
  # is what makes the parser's lack of trailer-name filtering safe -- a trailer cannot
  # override a real header (e.g. a smuggled Content-Length in a trailer is inert) (#274).
  result = initResponse(p.status, p.reason, p.version, p.headers, p.body)
  result.trailers = p.trailers
