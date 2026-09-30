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
##   5. upload: a request body of every byte value, and a gzip blob, reach the
##      server byte-exact (#417 -- a `cstring` body was UTF-8 transcoded, so every
##      byte >= 0x80 outside a valid sequence went out as U+FFFD and the body grew).

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

proc hexOf(s: string): string =
  ## Lowercase hex of every byte in `s`, to compare against what the server saw.
  const digits = "0123456789abcdef"
  result = newString(s.len * 2)
  for i in 0 ..< s.len:
    let b = int(uint8(s[i]))
    result[i * 2] = digits[b shr 4]
    result[i * 2 + 1] = digits[b and 0x0f]

proc download(path: string): Future[string] {.async.} =
  ## Fetch `path` over the (byte-exact) streaming sink, as a byte string.
  let api = newNavi()
  var got: seq[byte] = @[]
  let sink = proc(data: seq[byte]): Future[bool] {.async.} =
    got.add data
    return true
  let res = await api.get(base & path, sink = sink)
  doAssert res.status == 200, path & ": status " & $res.status
  var bytes = newString(got.len)
  for i in 0 ..< got.len: bytes[i] = char(got[i])
  return bytes

proc uploadExact(payload, what: string): Future[void] {.async.} =
  ## POST `payload` and assert the server received exactly those bytes.
  let api = newNavi()
  let res = await api.post(base & "/echo", body = payload)
  doAssert res.status == 200, what & ": status " & $res.status
  let gotLen = res.headers.get("x-body-len")
  doAssert gotLen == $payload.len,
    what & ": server received " & gotLen & " bytes, sent " & $payload.len
  let gotHex = res.headers.get("x-body-hex")
  let wantHex = hexOf(payload)
  if gotHex != wantHex:
    var at = 0
    while at < gotHex.len and at < wantHex.len and gotHex[at] == wantHex[at]: inc at
    doAssert false, what & ": bytes differ at byte " & $(at div 2) &
      ": got " & gotHex[at div 2 * 2 .. min(gotHex.high, at div 2 * 2 + 1)] &
      ", want " & wantHex[at div 2 * 2 .. min(wantHex.high, at div 2 * 2 + 1)]
  echo "OK: upload byte-exact, ", what, ", ", payload.len, " bytes"

proc uploadAllByteValues(): Future[void] {.async.} =
  await uploadExact(allBytes(), "all 256 byte values")

proc uploadGzipBody(): Future[void] {.async.} =
  let gz = await download("/gzip")
  doAssert gz.len > 0, "gzip blob download was empty"
  doAssert gz[0] == '\x1F' and gz[1] == '\x8B', "downloaded blob is not gzip"
  await uploadExact(gz, "gzip blob")

proc uploadUtf8Body(): Future[void] {.async.} =
  ## A valid UTF-8 body must still go out unchanged (no double encoding).
  await uploadExact("h\xC3\xA9llo \xE4\xB8\x96\xE7\x95\x8C", "valid UTF-8 text")

proc uploadNoImplicitContentType(): Future[void] {.async.} =
  ## A raw string body carries no Content-Type, and fetch must not add its
  ## text/plain default for a byte body (the native clients send none either).
  let api = newNavi()
  let res = await api.post(base & "/echo", body = "plain")
  doAssert res.status == 200, "status " & $res.status
  let ct = res.headers.get("x-req-ctype")
  doAssert ct == "", "raw body sent an implicit Content-Type: " & ct
  echo "OK: raw string body sends no implicit Content-Type"

proc clearTlsSecretsOverload(): Future[void] {.async.} =
  ## The js `clearTlsSecrets(client)` overload (#438) exists so cross-backend code
  ## can call it unconditionally. It can only drop the strings here (fetch owns TLS
  ## and a JS string has no buffer to overwrite), and the client must keep working.
  var cfg = initNaviConfig()
  cfg.tls.password = "s3cret"
  cfg.tls.keyPem = "-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n"
  cfg.tls.certPem = "-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n"
  let api = newNavi(cfg)
  api.clearTlsSecrets()
  doAssert api.config.tls.password.len == 0, "password survived the wipe"
  doAssert api.config.tls.keyPem.len == 0, "keyPem survived the wipe"
  doAssert api.config.tls.certPem.len == 0, "certPem survived the wipe"
  doAssert cfg.tls.keyPem.len > 0, "the caller's own config must be untouched"
  cfg.tls.clearTlsSecrets()                 # the config-level wipe is js-safe too
  doAssert cfg.tls.keyPem.len == 0, "the config-level wipe did nothing"
  let res = await api.get(base & "/echo")
  doAssert res.status == 200, "status " & $res.status
  echo "OK: clearTlsSecrets on a js client empties the TLS fields"

proc main() {.async.} =
  bufferedFallbackConversion()
  await sinkRoundTrip()
  await pullRoundTrip()
  await emptyBody()
  await uploadAllByteValues()
  await uploadGzipBody()
  await uploadUtf8Body()
  await uploadNoImplicitContentType()
  await clearTlsSecretsOverload()
  echo "ALL OK: navi/js binary body round trip"

discard main()
