## Streaming-response decompression: the incremental decoder fed across chunk
## boundaries (deterministic), and the end-to-end `stream` path decoding a body.

import unittest
import std/[net, os, strutils]
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
    test encoding & " decodes when fed one byte at a time":
      check feedSliced(encoding, hexToBytes(hex), 1) == """{"ok":true}"""
    test encoding & " decodes when fed in 3-byte slices":
      check feedSliced(encoding, hexToBytes(hex), 3) == """{"ok":true}"""

  test "an unknown encoding yields no decoder (pass-through)":
    check newStreamDecoder("identity") == nil
    check newStreamDecoder("") == nil

suite "stream() decompresses the response body":
  test "a gzip body reaches the sink decoded, not compressed":
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

  test "decompress = false leaves the streamed body compressed":
    const port = 8966
    let gz = hexToBytes("1f8b0800000000000003ab56cacf56b22a292a4dad0500905fd4a70b000000")
    let payload = "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n" &
                  "Content-Length: " & $gz.len & "\r\nConnection: close\r\n\r\n" & gz
    var th: Thread[ServerCtx]
    startRaw(th, port, payload)

    let api = newNavi(NaviOptions(decompress: some(false)))
    var collected = ""
    let res = api.stream(GET, "http://127.0.0.1:" & $port & "/",
      sink = proc(data: openArray[byte]) =
        for b in data: collected.add char(b))
    check res.status == 200
    check collected == gz                      # raw compressed bytes
    joinThread(th)
