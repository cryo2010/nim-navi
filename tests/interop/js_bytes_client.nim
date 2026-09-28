## navi/js binary body round-trip checks, run under Node against a small HTTP
## server (see js_bytes.sh). Guards the bulk Uint8Array <-> Nim marshalling that
## replaced the per-byte jsffi loops (#412): a bulk conversion that silently
## mangles bytes > 127, or drops a chunk boundary, still compiles and still passes
## `nim check`, so every byte value is asserted end to end under Node.
##
##   1. streaming sink: a 1 MiB body of every byte value, delivered over many
##      chunks, arrives byte-exact (drainToSink).
##   2. pull stream: the same body through `stream()` / `readChunk` (readOne).
##   3. buffered fallback: `bytesOfBody` converts a Nim js string holding all 256
##      byte values to seq[byte] exactly (the gate-miss sink path).
##   4. empty body: a zero-length chunk conversion yields an empty seq, not junk.

import navi/js
import navi/backend/js as jsbackend

const
  base = "http://127.0.0.1:9524"
  bodyLen = 256 * 4096          ## 1 MiB, written by the server in many chunks

proc allBytes(): string =
  ## Every byte value 0..255, once.
  result = newString(256)
  for i in 0 .. 255: result[i] = char(i)

proc checkPattern(got: seq[byte], what: string) =
  doAssert got.len == bodyLen,
    what & ": length mismatch: got " & $got.len & ", want " & $bodyLen
  for i in 0 ..< got.len:
    doAssert int(got[i]) == i mod 256,
      what & ": byte " & $i & " is " & $int(got[i]) & ", want " & $(i mod 256)

proc sinkRoundTrip(): Future[void] {.async.} =
  let api = newNavi()
  var got: seq[byte] = @[]
  var chunks = 0
  let sink = proc(data: seq[byte]): Future[bool] {.async.} =
    inc chunks
    got.add data
    return true
  let res = await api.get(base & "/bytes", sink = sink)
  doAssert res.status == 200, "status " & $res.status
  doAssert chunks > 1, "expected a multi-chunk body, got " & $chunks & " chunk(s)"
  checkPattern(got, "sink")
  echo "OK: streaming sink, ", bodyLen, " bytes over ", chunks, " chunks, byte-exact"

proc pullRoundTrip(): Future[void] {.async.} =
  let api = newNavi()
  let sr = await api.stream(GET, base & "/bytes")
  doAssert sr.status == 200, "status " & $sr.status
  var got: seq[byte] = @[]
  var chunks = 0
  while true:
    let c = await sr.readChunk()
    if c.len == 0: break
    inc chunks
    got.add c
  doAssert chunks > 1, "expected a multi-chunk body, got " & $chunks & " chunk(s)"
  checkPattern(got, "pull stream")
  echo "OK: pull stream, ", bodyLen, " bytes over ", chunks, " chunks, byte-exact"

proc bufferedFallbackConversion() =
  let s = allBytes()
  let b = jsbackend.bytesOfBody(s)
  doAssert b.len == 256, "bytesOfBody length: " & $b.len
  for i in 0 .. 255:
    doAssert int(b[i]) == i, "bytesOfBody byte " & $i & " is " & $int(b[i])
  doAssert jsbackend.bytesOfBody("").len == 0, "bytesOfBody of an empty string"
  echo "OK: buffered-body string -> seq[byte], all 256 values"

proc emptyBody(): Future[void] {.async.} =
  let api = newNavi()
  var calls = 0
  var total = 0
  let sink = proc(data: seq[byte]): Future[bool] {.async.} =
    inc calls
    total += data.len
    return true
  let res = await api.get(base & "/empty", sink = sink)
  doAssert res.status == 200, "status " & $res.status
  doAssert total == 0, "empty body delivered " & $total & " bytes"
  echo "OK: empty body delivers no bytes (", calls, " sink call(s))"

proc main() {.async.} =
  bufferedFallbackConversion()
  await sinkRoundTrip()
  await pullRoundTrip()
  await emptyBody()
  echo "ALL OK: navi/js binary body round trip"

discard main()
