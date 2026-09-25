## Sans-io HTTP/1.1 unit tests: serialization and the incremental parser.
## No sockets — bytes in, response out.

import unittest
import std/[strutils, strformat]
import navi/core/[headers, url, request, response]
import navi/proto/h1

suite "h1 serialize":
  test "the request serializer should add Host and keep the connection alive by default":
    var req = Request(verb: GET, url: parseUrl("http://example.com/path?q=1"))
    let wire = serializeRequest(req)
    check wire.startsWith("GET /path?q=1 HTTP/1.1\r\n")
    check "Host: example.com\r\n" in wire
    check "Connection: close" notin wire

  test "the request serializer should set Content-Length when a body is present":
    var req = Request(verb: POST, url: parseUrl("http://h/"), body: "hello")
    let wire = serializeRequest(req)
    check "Content-Length: 5\r\n" in wire
    check wire.endsWith("\r\n\r\nhello")

  test "the request serializer should include the port in Host when it is non-default":
    var req = Request(verb: GET, url: parseUrl("http://h:8080/"))
    check "Host: h:8080\r\n" in serializeRequest(req)

  test "a caller-supplied Content-Length should be dropped on the chunked path (#294)":
    # Emitting it next to Transfer-Encoding: chunked is the CL.TE smuggling ambiguity,
    # and the length is wrong anyway (the real body is the producer's). navi strips it,
    # matching h3 (h3SkipHeaders) and h2.
    var req = Request(verb: POST, url: parseUrl("http://h/"))
    req.headers = initHeaders()
    req.headers["Content-Length"] = "5"
    req.bodyStream = proc(): string = ""
    let head = serializeHead(req, chunked = true)
    check "Transfer-Encoding: chunked\r\n" in head
    check "content-length" notin head.toLowerAscii

  test "a caller-supplied Content-Length should survive on the buffered path":
    var req = Request(verb: POST, url: parseUrl("http://h/"), body: "hello")
    req.headers = initHeaders()
    req.headers["Content-Length"] = "5"
    let wire = serializeRequest(req)
    check "Content-Length: 5\r\n" in wire
    check "Transfer-Encoding" notin wire

  test "the request serializer should reject a manually-set Transfer-Encoding on a buffered body (#273)":
    var req = Request(verb: POST, url: parseUrl("http://h/"), body: "hello")
    req.headers = initHeaders()
    req.headers["transfer-encoding"] = "chunked"
    expect ValueError: discard serializeRequest(req)

  test "the request serializer should send Content-Length: 0 for an empty POST (#274)":
    var req = Request(verb: POST, url: parseUrl("http://h/"))
    check "Content-Length: 0\r\n" in serializeRequest(req)

  test "the request serializer should not add Content-Length: 0 to an empty GET (#274)":
    var req = Request(verb: GET, url: parseUrl("http://h/"))
    check "Content-Length" notin serializeRequest(req)

  test "encodeChunk should return empty for empty data rather than a premature terminator (#274)":
    check encodeChunk("") == ""
    check encodeChunk("ab") == "2\r\nab\r\n"

  test "encodeChunk should write the chunk size as uppercase hex at every width (#244)":
    # The size is written digit by digit into the output buffer rather than through a
    # formatted temporary, so check the boundaries a hand-rolled hex writer can get
    # wrong: single digit, nibble rollover, and a multi-byte size.
    for n in [1, 9, 10, 15, 16, 17, 255, 256, 4095, 4096, 1048576]:
      let data = repeat('x', n)
      check encodeChunk(data) == fmt"{n:X}" & "\r\n" & data & "\r\n"

  test "addChunk should append frames to an existing buffer and skip empty data (#244)":
    var buf = "head:"
    buf.addChunk("")                  # an empty chunk would be a premature terminator
    buf.addChunk("abc")
    buf.addChunk(repeat('y', 26))
    check buf == "head:3\r\nabc\r\n1A\r\n" & repeat('y', 26) & "\r\n"

  test "validateRequest should reject CR/LF in the request path (#274)":
    var req = Request(verb: GET, url: parseUrl("http://h/a"))
    req.url = parseUrl("http://h/a")
    req.url.raw.path = "/a\r\nX-Injected: 1"
    expect ValueError: validateRequest(req)

  test "the request serializer should bracket an IPv6 host literal in the Host header (#270)":
    var req = Request(verb: GET, url: parseUrl("http://[2001:db8::1]:8080/x"))
    check "Host: [2001:db8::1]:8080\r\n" in serializeRequest(req)
    var reqDef = Request(verb: GET, url: parseUrl("http://[2001:db8::1]/x"))
    check "Host: [2001:db8::1]\r\n" in serializeRequest(reqDef)   # default port, still bracketed

proc parseAll(chunks: varargs[string]): Response =
  var p = initH1Parser()
  for c in chunks:
    p.feed(c)
  if not p.finished: p.eof()
  check p.finished
  p.toResponse()

proc parseKA(chunks: varargs[string]): bool =
  ## Parse to completion and report whether the connection may be pooled.
  var p = initH1Parser()
  for c in chunks:
    p.feed(c)
  if not p.finished: p.eof()
  p.keepAliveAfter()

suite "h1 parse":
  test "the h1 parser should read a Content-Length body":
    let r = parseAll("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
    check r.status == 200
    check r.reason == "OK"
    check r.httpVersion == "HTTP/1.1"
    check r.body == "hello"
    check r.headers.get("content-length") == "5"

  test "the h1 parser should reassemble a response split across feeds":
    let r = parseAll("HTTP/1.1 20", "0 OK\r\nContent-Len", "gth: 3\r\n\r\nab", "c")
    check r.status == 200
    check r.body == "abc"

  test "the h1 parser should decode a chunked body":
    let r = parseAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" &
                     "3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n")
    check r.body == "abcde"

  test "the h1 parser should decode a multi-chunk body fed one byte at a time":
    # Stresses the read-cursor + feed compaction (the pos-cursor rewrite): every
    # feed carries a single byte, so line/chunk boundaries land mid-buffer and the
    # consumed prefix is compacted repeatedly.
    let raw = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" &
              "5\r\nhello\r\n6\r\n world\r\n1\r\n!\r\n0\r\n\r\n"
    var p = initH1Parser()
    for ch in raw:
      p.feed($ch)
    check p.finished
    check p.toResponse().body == "hello world!"

  test "the h1 parser should read a large chunked body fed in tiny fragments (#244)":
    # The read-cursor rewrite must stay linear: 256 KiB of body handed over in
    # 3-byte feeds is ~90k feeds, each of which used to memmove the whole remaining
    # buffer down. Also covers chunk data, chunk terminators and chunk-size lines
    # straddling feed boundaries.
    var body = newStringOfCap(256 * 1024)
    var i = 0
    while body.len < 256 * 1024:
      body.add($i & ",")
      inc i
    var raw = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
    var off = 0
    while off < body.len:                       # many chunks, uneven sizes
      let n = min(1 + (off mod 7000), body.len - off)
      raw.add(fmt"{n:X}" & "\r\n" & body[off ..< off + n] & "\r\n")
      off += n
    raw.add("0\r\n\r\n")
    var p = initH1Parser()
    off = 0
    while off < raw.len:
      let n = min(3, raw.len - off)
      p.feed(raw.toOpenArray(off, off + n - 1))
      off += n
    check p.finished
    check p.keepAliveAfter()
    check p.toResponse().body == body

  test "the h1 parser should stream a chunk incrementally instead of buffering it whole (#244)":
    # A chunk larger than one read must not be held in the parser until complete:
    # the streaming path drains what has arrived, which is also what makes the
    # response size cap effective mid-chunk.
    var p = initH1Parser(streaming = true)
    p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nA\r\n01234")
    check p.takeBody() == "01234"               # half a 10-byte chunk, already out
    check not p.finished
    p.feed("56789\r\n0\r\n\r\n")
    check p.takeBody() == "56789"
    check p.finished
    check p.keepAliveAfter()

  test "the h1 parser should hold a chunk's last byte back until its CRLF is verified (#244)":
    # Incremental delivery must not hand a streaming consumer a COMPLETE chunk on the
    # strength of its size line alone: the final byte waits for the terminator, so a
    # desynced or smuggled chunk can only ever deliver a strict prefix before the
    # framing error is raised.
    var p = initH1Parser(streaming = true)
    p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello")
    var got = p.takeBody()
    check got.len <= 4                          # all five bytes are here, four may go
    var msg = ""
    try:
      p.feed("XX0\r\n\r\n")                      # terminator is not CRLF
    except ValueError as e: msg = e.msg
    got.add p.takeBody()
    check "CRLF" in msg
    check got.len <= 4
    check "hello" notin got                     # the chunk never landed whole
    check not p.finished
    check not p.keepAliveAfter()

  test "the h1 parser should deliver nothing of a one-byte chunk ended by a bare LF (#244)":
    var p = initH1Parser(streaming = true)
    var msg = ""
    try:
      p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nA\n0\r\n\r\n")
    except ValueError as e: msg = e.msg
    check "CRLF" in msg
    check p.takeBody() == ""
    check not p.keepAliveAfter()

  test "the h1 parser should ignore bytes that arrive after a keep-alive response (#244)":
    # A pooled connection can deliver the tail of the current response and the head
    # of whatever the server sends next in one read. The trailing bytes must not
    # join the body, unfinish the response, or make it unpoolable.
    var p = initH1Parser()
    p.feed("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello" &
           "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi")
    check p.finished
    check p.toResponse().body == "hello"
    check p.keepAliveAfter()
    var q = initH1Parser()                      # same, split across feeds
    q.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel")
    q.feed("lo\r\n0\r\n\r\nHTTP/1.1 204 No")
    check q.finished
    check q.toResponse().body == "hello"
    check q.keepAliveAfter()

  test "the h1 parser should read a length body split across many feeds":
    var p = initH1Parser()
    let head = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n"
    for ch in head: p.feed($ch)
    for ch in "0123456789": p.feed($ch)
    check p.finished
    check p.toResponse().body == "0123456789"

  test "the h1 parser should read a body until connection close when no length is given":
    let r = parseAll("HTTP/1.1 200 OK\r\n\r\nstreamed-to-eof")
    check r.body == "streamed-to-eof"

  test "the h1 parser should produce an empty body for a 204 response":
    let r = parseAll("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
    check r.status == 204
    check r.body == ""

  test "the h1 parser should look up headers case-insensitively":
    let r = parseAll("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 0\r\n\r\n")
    check r.headers.get("CONTENT-TYPE") == "text/html"

  test "the h1 parser should skip a 103 Early Hints interim response before the final one":
    let r = parseAll(
      "HTTP/1.1 103 Early Hints\r\nLink: </s.css>; rel=preload\r\n\r\n" &
      "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
    check r.status == 200
    check r.body == "hello"
    check not r.headers.contains("link")          # interim header did not leak

  test "the h1 parser should skip 100 Continue then read the final response":
    let r = parseAll(
      "HTTP/1.1 100 Continue\r\n\r\n" &
      "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
    check r.status == 204
    check r.body == ""

  test "the h1 parser should skip an interim response split across feeds":
    let r = parseAll("HTTP/1.1 100 Cont", "inue\r\n\r\nHTTP/1.1 200 OK\r\n",
                     "Content-Length: 2\r\n\r\nhi")
    check r.status == 200
    check r.body == "hi"

  test "the h1 parser should produce no body for a HEAD response despite Content-Length":
    # headRequest = true: the parser must complete on the headers alone, not block
    # waiting for the Content-Length bytes a HEAD reply never sends.
    var p = initH1Parser(headRequest = true)
    p.feed("HTTP/1.1 200 OK\r\nContent-Length: 42\r\n\r\n")
    check p.finished
    let r = p.toResponse()
    check r.status == 200
    check r.body == ""
    check r.headers.get("content-length") == "42"   # header preserved

  test "the h1 parser should keep a HEAD response with keep-alive reusable":
    var p = initH1Parser(headRequest = true)
    p.feed("HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\n")
    check p.finished
    check p.keepAliveAfter()

  test "the h1 parser should produce no body for 204 and 304 responses despite Content-Length":
    let a = parseAll("HTTP/1.1 204 No Content\r\nContent-Length: 5\r\n\r\n")
    check a.status == 204 and a.body == ""
    let b = parseAll("HTTP/1.1 304 Not Modified\r\nContent-Length: 99\r\n\r\n")
    check b.status == 304 and b.body == ""

  test "the h1 parser should reject a negative Content-Length without crashing":
    # A peer-controlled negative length must raise (caught upstream), not slice
    # out of bounds into a RangeDefect crash (found by tests/fuzz).
    var p = initH1Parser()
    var msg = ""
    try:
      p.feed("HTTP/1.1 200 OK\r\nContent-Length: -7\r\n\r\n")
    except ValueError as e: msg = e.msg
    check "Content-Length" in msg

  test "the h1 parser should reject an overflowing chunk size without crashing":
    # parseHexInt wraps on overflow; the result must be bounds-checked before it
    # slices the buffer (found by tests/fuzz).
    var p = initH1Parser()
    var msg = ""
    try:
      p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff\r\n")
    except ValueError as e: msg = e.msg
    check "chunk size" in msg

  test "the h1 parser should reject chunk data not terminated by CRLF (#244)":
    # The two bytes after chunk-data must be CRLF (RFC 9112 7.1). A missing CRLF is
    # a framing desync: without the check it is silently consumed, a corrupted body
    # is delivered, and the pooled connection can be left poisoned.
    var p = initH1Parser()
    var msg = ""
    try:
      # "abc" declared as a 3-byte chunk, but followed by "XX" instead of CRLF.
      p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabcXX0\r\n\r\n")
    except ValueError as e: msg = e.msg
    check "CRLF" in msg
    # The desynced response must never look complete or poolable: returning the
    # connection to the pool would leave the unread remainder on the wire, where the
    # next request on it parses stale body bytes as its status line.
    check not p.finished
    check not p.keepAliveAfter()

  test "the h1 parser should reject a bare-LF chunk terminator (#244)":
    # A lone LF after chunk-data is the same framing desync: one byte of the next
    # chunk-size line would be swallowed as the missing CR.
    var p = initH1Parser()
    var msg = ""
    try:
      p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\n0\r\n\r\n")
    except ValueError as e: msg = e.msg
    check "CRLF" in msg
    check not p.keepAliveAfter()

  test "the h1 parser should reject a CR-only chunk terminator across feeds (#244)":
    # The terminator can straddle reads: the parser waits for both bytes, then
    # rejects "\r" followed by anything other than "\n".
    var p = initH1Parser()
    var msg = ""
    try:
      p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r")
      check not p.finished                # still waiting for the second byte
      p.feed("X0\r\n\r\n")
    except ValueError as e: msg = e.msg
    check "CRLF" in msg
    check not p.keepAliveAfter()

  test "a chunked response whose chunks are properly terminated stays poolable (#244)":
    check parseKA("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" &
                  "3\r\nabc\r\n0\r\n\r\n")

  test "the h1 parser should reject Transfer-Encoding: chunked together with Content-Length (#271)":
    var p = initH1Parser()
    var msg = ""
    try:
      p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n")
    except ValueError as e: msg = e.msg
    check "Transfer-Encoding" in msg

  test "the h1 parser should reject conflicting Content-Length values (#271)":
    var p = initH1Parser()
    var msg = ""
    try:
      p.feed("HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Length: 5\r\n\r\n")
    except ValueError as e: msg = e.msg
    check "Content-Length" in msg

  test "the h1 parser should collapse duplicate agreeing Content-Length values (#271)":
    let r = parseAll("HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\nabc")
    check r.body == "abc"

  test "the h1 parser should reject a signed Content-Length (#271)":
    var p = initH1Parser()
    var msg = ""
    try:
      p.feed("HTTP/1.1 200 OK\r\nContent-Length: +5\r\n\r\n")
    except ValueError as e: msg = e.msg
    check "Content-Length" in msg

  test "the h1 parser should not treat a substring 'chunked' as chunked framing (#271)":
    # "not-chunked" is a single (unknown) coding, not the chunked framing: RFC 9112
    # 6.3 says read until close, so the whole remainder is the body, not a chunk size.
    let r = parseAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: not-chunked\r\n\r\n5\r\nhello")
    check r.body == "5\r\nhello"
    check not parseKA("HTTP/1.1 200 OK\r\nTransfer-Encoding: not-chunked\r\n\r\n5\r\nhello")

  test "the h1 parser should read until close when chunked is not the final coding (#271)":
    let r = parseAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, gzip\r\n\r\nrawbytes")
    check r.body == "rawbytes"

  test "keepAliveAfter should see a second Connection: close line (#272)":
    check not parseKA("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n" &
                      "Connection: keep-alive\r\nConnection: close\r\n\r\n")

  test "keepAliveAfter should reuse a plain keep-alive response (#272)":
    check parseKA("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi")

suite "url port parsing":
  test "an explicit port and the scheme defaults parse":
    check parseUrl("http://h:8080/").port == 8080
    check parseUrl("http://h/").port == 80
    check parseUrl("https://h/").port == 443

  test "a non-numeric port raises ValueError, not a cryptic parse error":
    expect ValueError: discard parseUrl("http://h:80x/").port

  test "an out-of-range port raises ValueError":
    expect ValueError: discard parseUrl("http://h:99999/").port
    expect ValueError: discard parseUrl("http://h:99999999999999999999/").port
