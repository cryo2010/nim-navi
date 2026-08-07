## Sans-io HTTP/1.1 unit tests: serialization and the incremental parser.
## No sockets — bytes in, response out.

import unittest
import std/strutils
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

proc parseAll(chunks: varargs[string]): Response =
  var p = initH1Parser()
  for c in chunks:
    p.feed(c)
  if not p.finished: p.eof()
  check p.finished
  p.toResponse()

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
