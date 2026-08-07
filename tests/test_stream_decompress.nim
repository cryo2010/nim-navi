## Streaming-response decompression: the incremental decoder fed across chunk
## boundaries (deterministic), and the end-to-end `stream` path decoding a body.

import unittest
import std/net
import navi
import navi/core/decompress
import ./support

proc feedSliced(encoding, compressed: string, sliceLen: int): string =
  ## Drive a StreamDecoder with `compressed` split into `sliceLen`-byte pieces,
  ## as if each arrived in its own TCP read. Concatenates the decoded output.
  let dec = newStreamDecoder(encoding)
  var i = 0
  while i < compressed.len:
    let n = min(sliceLen, compressed.len - i)
    let piece = compressed[i ..< i + n]
    result.add dec.update(piece.toOpenArrayByte(0, piece.high))
    i += n

suite "incremental decoder across chunk boundaries":
  # Blobs are the same fixtures the buffered tests use: encodings of {"ok":true}.
  let cases = {
    "gzip": "1f8b0800000000000003ab56cacf56b22a292a4dad0500905fd4a70b000000",
    "br": "0f05807b226f6b223a747275657d03",
    "zstd": "28b52ffd04585900007b226f6b223a747275657d6abe13c7",
  }
  for (encoding, hex) in cases:
    test "the incremental decoder should decode " & encoding & " when fed one byte at a time":
      check feedSliced(encoding, hexToBytes(hex), 1) == """{"ok":true}"""
    test "the incremental decoder should decode " & encoding & " when fed in 3-byte slices":
      check feedSliced(encoding, hexToBytes(hex), 3) == """{"ok":true}"""

  test "the incremental decoder should yield no decoder when the encoding is unknown":
    check newStreamDecoder("identity") == nil
    check newStreamDecoder("") == nil

suite "stream() decompresses the response body":
  test "stream should deliver a gzip body to the sink decoded":
    const port = 8965
    let gz = hexToBytes("1f8b0800000000000003ab56cacf56b22a292a4dad0500905fd4a70b000000")
    let payload = "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n" &
                  "Content-Length: " & $gz.len & "\r\nConnection: close\r\n\r\n" & gz
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi()
    var collected = ""
    let res = api.stream(GET, "http://127.0.0.1:" & $port & "/",
      sink = proc(data: openArray[byte]) =
        for b in data: collected.add char(b))
    check res.status == 200
    check res.body == ""                      # streamed, not buffered
    check collected == """{"ok":true}"""      # decoded on the way to the sink
    joinThread(th)

  test "stream should leave the body compressed when decompress is false":
    const port = 8966
    let gz = hexToBytes("1f8b0800000000000003ab56cacf56b22a292a4dad0500905fd4a70b000000")
    let payload = "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n" &
                  "Content-Length: " & $gz.len & "\r\nConnection: close\r\n\r\n" & gz
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    var cfg = initNaviConfig()
    cfg.decompress = false
    let api = newNavi(cfg)
    var collected = ""
    let res = api.stream(GET, "http://127.0.0.1:" & $port & "/",
      sink = proc(data: openArray[byte]) =
        for b in data: collected.add char(b))
    check res.status == 200
    check collected == gz                      # raw compressed bytes
    joinThread(th)

suite "stacked content-encoding":
  test "a buffered get should decode a doubly-encoded body (gzip, gzip)":
    const port = 8968
    # {"ok":true} gzipped twice; Content-Encoding lists them in applied order.
    let body = hexToBytes(
      "1f8b08000000000002ff93efe6600001a6ffabc34e9d0fdba4a5a9e5bb9" &
      "6956142fc95e5dc406100aa34a89a1f000000")
    let payload = "HTTP/1.1 200 OK\r\nContent-Encoding: gzip, gzip\r\n" &
                  "Content-Length: " & $body.len & "\r\nConnection: close\r\n\r\n" & body
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let res = newNavi().get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.body == """{"ok":true}"""         # both gzip layers undone, in reverse
    check not res.headers.contains("content-encoding")
    joinThread(th)
