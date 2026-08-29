## Trailer surfacing (response) and emission (request) for HTTP/1.1 and HTTP/2.
import unittest
import std/strutils
import navi/core/[headers, response, request, url]
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

proc trailerRequest(): Request =
  result.verb = POST
  result.url = parseUrl("http://example.com/upload")
  result.headers = initHeaders()
  result.trailers = initHeaders(@[("X-Checksum", "abc123"), ("X-Rows", "7")])

suite "HTTP/1.1 request trailers":
  test "serializeHead should declare the trailer names when chunked":
    let head = serializeHead(trailerRequest(), chunked = true)
    check "Transfer-Encoding: chunked\r\n" in head
    check "Trailer: X-Checksum, X-Rows\r\n" in head

  test "serializeHead should not add a Trailer header when the caller set one":
    var req = trailerRequest()
    req.headers["trailer"] = "X-Checksum"
    let head = serializeHead(req, chunked = true)
    check "trailer: X-Checksum\r\n" in head
    check "Trailer: X-Checksum, X-Rows\r\n" notin head

  test "finalChunk should emit the trailer fields after the zero chunk":
    check finalChunk(trailerRequest()) ==
      "0\r\nX-Checksum: abc123\r\nX-Rows: 7\r\n\r\n"

  test "finalChunk without trailers should equal the plain chunk terminator":
    var req = trailerRequest()
    req.trailers = initHeaders()
    check finalChunk(req) == chunkTerminator

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

type ClientFrames = object
  dataFrames: seq[tuple[body: string, endStream: bool]]
  blocks: seq[tuple[fields: seq[(string, string)], endStream: bool]]

proc decodeClientFrames(wire: string): ClientFrames =
  ## Decode client-to-server frames, keeping the HPACK decoder in sync by decoding
  ## every HEADERS block in order (request headers first, then any trailers).
  var fd: FrameDecoder
  fd.feed(wire)
  var dec = initHpackDecoder()
  var f: Frame
  while fd.next(f):
    if f.typ == uint8(ftData):
      result.dataFrames.add((f.payload, (f.flags and flagEndStream) != 0))
    elif f.typ == uint8(ftHeaders):
      var fields: seq[(string, string)]
      for (n, v) in dec.decode(f.payload): fields.add((n, v))
      result.blocks.add((fields, (f.flags and flagEndStream) != 0))

const reqHeaders = @[(":method", "POST"), (":scheme", "https"),
                     (":path", "/upload"), (":authority", "example.com")]

suite "HTTP/2 request trailers":
  test "a buffered body with trailers should end the stream on a trailing HEADERS block":
    let c = initH2Conn(0)
    let id = c.openStream()
    let wire = c.encodeRequest(id, reqHeaders, "hello",
                               @[("x-checksum", "abc123")])
    let cf = decodeClientFrames(wire)
    # the request HEADERS block does not end the stream
    check cf.blocks[0].endStream == false
    # the single DATA frame carries the body but not END_STREAM (trailers follow)
    check cf.dataFrames.len == 1
    check cf.dataFrames[0].body == "hello"
    check cf.dataFrames[0].endStream == false
    # a trailing HEADERS block carries the trailers and ends the stream
    check cf.blocks.len == 2
    check cf.blocks[1].fields == @[("x-checksum", "abc123")]
    check cf.blocks[1].endStream == true

  test "trailers with no body should ride a trailing HEADERS block after the headers":
    let c = initH2Conn(0)
    let id = c.openStream()
    let wire = c.encodeRequest(id, reqHeaders, "", @[("grpc-status", "0")])
    let cf = decodeClientFrames(wire)
    check cf.dataFrames.len == 0
    check cf.blocks[0].endStream == false          # request HEADERS, stream still open
    check cf.blocks.len == 2
    check cf.blocks[1].fields == @[("grpc-status", "0")]
    check cf.blocks[1].endStream == true

  test "a buffered body without trailers should end the stream on the DATA frame":
    let c = initH2Conn(0)
    let id = c.openStream()
    let wire = c.encodeRequest(id, reqHeaders, "hello")
    let cf = decodeClientFrames(wire)
    check cf.dataFrames.len == 1
    check cf.dataFrames[0].endStream == true       # END_STREAM on the last DATA
    check cf.blocks.len == 1                        # no trailing HEADERS block

  test "a streamed body should end on a trailing HEADERS block via finishSend":
    let c = initH2Conn(0)
    let id = c.openStream()
    var wire = c.encodeRequestHead(id, reqHeaders)
    wire.add c.queueSend(id, "chunk1")
    wire.add c.finishSend(id, @[("x-rows", "7")])
    let cf = decodeClientFrames(wire)
    check cf.dataFrames.len >= 1
    for d in cf.dataFrames: check d.endStream == false
    check cf.blocks[^1].fields == @[("x-rows", "7")]
    check cf.blocks[^1].endStream == true
