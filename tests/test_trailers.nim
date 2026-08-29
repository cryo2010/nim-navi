## Response trailer surfacing for HTTP/1.1 (chunked) and HTTP/2.
import unittest
import navi/core/[headers, response]
import navi/proto/h1
import navi/proto/h2/[conn, frame, hpack]

suite "HTTP/1.1 chunked trailers":
  test "trailing fields after a chunked body should be surfaced":
    var p = initH1Parser()
    p.feed("HTTP/1.1 200 OK\r\n" &
           "Transfer-Encoding: chunked\r\n" &
           "Trailer: X-Checksum\r\n\r\n" &
           "5\r\nhello\r\n" &
           "0\r\n" &
           "X-Checksum: abc123\r\n" &
           "X-Extra: 7\r\n\r\n")
    check p.finished
    let r = p.toResponse()
    check r.body == "hello"
    check r.trailers.get("x-checksum") == "abc123"
    check r.trailers.get("x-extra") == "7"

  test "a chunked body with no trailers should leave trailers empty":
    var p = initH1Parser()
    p.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" &
           "5\r\nhello\r\n0\r\n\r\n")
    check p.finished
    check p.toResponse().trailers.len == 0

proc newServerConn(): H2Conn =
  result = initH2Conn(0)
  discard result.feed(encodeSettings([]))

proc responseWithTrailers(streamId: uint32; status: string;
                          headers, trailers: seq[HeaderPair]; body: string): string =
  ## One encoder for both header blocks so the decoder's dynamic table stays in
  ## sync. The body DATA is not END_STREAM; the trailing HEADERS block is.
  let enc = HpackEncoder()
  result = encodeHeaders(streamId, enc.encode(@[(":status", status)] & headers),
                         endStream = false, endHeaders = true)
  result.add encodeData(streamId, body, endStream = false)
  result.add encodeHeaders(streamId, enc.encode(trailers),
                           endStream = true, endHeaders = true)

suite "HTTP/2 trailers":
  test "a trailing HEADERS block should be surfaced on the response":
    let c = newServerConn()
    let id = c.openStream()
    discard c.feed(responseWithTrailers(id, "200",
      @[("content-type", "text/plain")], @[("grpc-status", "0")], "hello"))
    check c.streamDone(id)
    let resp = c.takeResponse(id)
    check resp.status == 200
    check resp.body == "hello"
    check resp.headers == @[("content-type", "text/plain")]
    check resp.trailers == @[("grpc-status", "0")]

  test "a response without a trailing block should have no trailers":
    let c = newServerConn()
    let id = c.openStream()
    let enc = HpackEncoder()
    var wire = encodeHeaders(id, enc.encode(@[(":status", "200")]),
                             endStream = false, endHeaders = true)
    wire.add encodeData(id, "hi", endStream = true)
    discard c.feed(wire)
    check c.streamDone(id)
    check c.takeResponse(id).trailers.len == 0
