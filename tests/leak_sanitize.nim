## C-side leak check for the codec FFI, run under AddressSanitizer +
## LeakSanitizer (`nimble leakSanitize`, ASAN_OPTIONS=detect_leaks=1).
##
## The Nim-heap growth check (leak.nim) uses getOccupiedMem and so cannot see
## allocations made inside zlib / libbrotlidec / libzstd. This loops both the
## buffered (`decodeBody`) and streaming (`newStreamDecoder`/`update`) decoders
## for every encoding, so LeakSanitizer verifies each codec context is released:
## inflateEnd, BrotliDecoderDestroyInstance, ZSTD_freeDStream, and the
## StreamDecoder `=destroy`. A dropped `defer`/`=destroy` shows up as a definite
## leak of thousands of contexts.
##
## No sockets or threads here, so an LSan report is unambiguous. LeakSanitizer
## runs on Linux CI (it is unavailable on macOS).

import navi/core/[decompress, headers, response, request]
import ./support

const
  iters = 20_000
  want = """{"ok":true}"""
  # The same fixtures the decode tests use: gzip/brotli/zstd of `want`.
  fixtures = {
    "gzip": "1f8b0800000000000003ab56cacf56b22a292a4dad0500905fd4a70b000000",
    "br": "0f05807b226f6b223a747275657d03",
    "zstd": "28b52ffd04585900007b226f6b223a747275657d6abe13c7",
  }

let opts = NaviConfigBase(decompress: true)   # exercise the decoders

proc decodeBuffered(encoding, body: string) =
  var r = initResponse(200, "", "HTTP/1.1",
    initHeaders({"content-encoding": encoding}), body)
  decodeBody(r, opts)
  doAssert r.body == want, "buffered " & encoding & " decoded wrong"

proc decodeStreamed(encoding, body: string) =
  let dec = newStreamDecoder(encoding)      # ref; its =destroy frees the codec
  doAssert dec.update(body.toOpenArrayByte(0, body.high)) == want,
    "streamed " & encoding & " decoded wrong"

when isMainModule:
  for i in 0 ..< iters:
    for (encoding, hex) in fixtures:
      let body = hexToBytes(hex)
      decodeBuffered(encoding, body)
      decodeStreamed(encoding, body)
  echo "decoded ", iters, " x (gzip+br+zstd, buffered+streaming); no leak"
