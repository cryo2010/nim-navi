# One verified streamed download for the navi/js backend, shared by
# clients/stream_download_js.nim and the streamDownload slice of
# clients/mixed_js.nim. `include`d (not imported) AFTER the including file's
# `import navi/js`, like the native parts. Not a standalone module. Needs
# ../common/[harness_js, streamcontent] from the including file.
#
# Constant memory: each chunk is hashed with Node's native SHA-1 and discarded,
# never buffered. A mismatch against the server's x-sha1, or a non-200, is a
# hard fail. (js cannot stream a request body, so there is no upload half.)

# Node's native SHA-1 (C-speed) -- Nim's checksums/sha1 uses copyMem and does not
# compile to js.
type Sha1 = ref object
proc createSha1(): Sha1 {.importjs: "require('crypto').createHash('sha1')".}
proc update(h: Sha1, chunk: seq[byte]) {.importjs: "#.update(Buffer.from(#))".}
proc digestHex(h: Sha1): cstring {.importjs: "#.digest('hex')".}

proc oneDownload(api: Navi, label, url: string): Future[int] {.async.} =
  let h = createSha1()
  var got = 0
  let res = await api.stream.get(url)
  if res.status != 200:
    jsFail(label, "/download -> " & $res.status)
  let expected = res.headers.get("x-sha1")
  res.each(chunk):                       # chunk: seq[byte] under js
    if chunk.len > 0:
      h.update(chunk)
      got += chunk.len
  let clientSha = $h.digestHex()
  if clientSha != expected:
    jsFail(label, "checksum mismatch got " & $got & " bytes client=" &
      clientSha & " server=" & expected)
  return got
