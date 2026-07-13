## Sans-io HTTP/1.1 unit tests: serialization and the incremental parser.
## No sockets — bytes in, response out.

import unittest
import std/strutils
import navi/core/[headers, url, request, response]
import navi/proto/h1

suite "h1 serialize":
  test "GET adds Host and Connection":
    var req = Request(verb: GET, url: parseUrl("http://example.com/path?q=1"))
    let wire = serializeRequest(req)
    check wire.startsWith("GET /path?q=1 HTTP/1.1\r\n")
    check "Host: example.com\r\n" in wire
    check "Connection: close\r\n" in wire

  test "body sets Content-Length":
    var req = Request(verb: POST, url: parseUrl("http://h/"), body: "hello")
    let wire = serializeRequest(req)
    check "Content-Length: 5\r\n" in wire
    check wire.endsWith("\r\n\r\nhello")

  test "non-default port in Host":
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
  test "content-length body":
    let r = parseAll("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
    check r.status == 200
    check r.reason == "OK"
    check r.httpVersion == "HTTP/1.1"
    check r.body == "hello"
    check r.headers.get("content-length") == "5"

  test "split across feeds":
    let r = parseAll("HTTP/1.1 20", "0 OK\r\nContent-Len", "gth: 3\r\n\r\nab", "c")
    check r.status == 200
    check r.body == "abc"

  test "chunked body":
    let r = parseAll("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" &
                     "3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n")
    check r.body == "abcde"

  test "until-close body":
    let r = parseAll("HTTP/1.1 200 OK\r\n\r\nstreamed-to-eof")
    check r.body == "streamed-to-eof"

  test "empty 204 body":
    let r = parseAll("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
    check r.status == 204
    check r.body == ""

  test "case-insensitive header lookup":
    let r = parseAll("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 0\r\n\r\n")
    check r.headers.get("CONTENT-TYPE") == "text/html"
