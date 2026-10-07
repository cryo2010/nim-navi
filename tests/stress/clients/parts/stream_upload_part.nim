# One verified streamed upload, shared by clients/stream_upload.nim and the
# streamUpload slice of clients/mixed.nim. `include`d (not imported) AFTER the
# including file's `import navi[/backend]` + `include ../common/httpset`, like
# common/httpset and common/chaos, because BodyProducer/Response/Future come from
# that backend. Not a standalone module. Needs std/json and
# ../common/[config, streamcontent] from the including file. The shared
# StreamProgress lives in common/streamcontent so both stream parts (and a client
# that includes only one of them) see the same type.
#
# Constant memory: one reused 1 MiB block, re-stamped per chunk so a whole-block
# reorder or duplication changes the digest, hashed as it flies and never
# retained. The server hashes what it received; a checksum or size mismatch, a
# non-200 or a protocol downgrade is a hard fail. A transient transport error is
# the caller's to retry (that is soak noise, not a bug).

proc oneUpload(api: Navi, cfg: Config, prog: StreamProgress, url: string) {.async.} =
  var st = newSha1State()
  var remaining = cfg.streamBytes
  var sent = 0
  var idx = 0
  var blk = fillBlock()   # local (gcsafe under chronos); re-stamped per block below
  var h = initHeaders()
  h["content-type"] = "application/octet-stream"

  let res = await api.request(POST, url, headers = h,
    body = BodyProducer(proc(): string =
      if remaining <= 0: return ""
      let n = min(blockSize, remaining)
      remaining -= n
      stampBlock(blk, idx); inc idx   # distinct per block: server catches a reorder/dup
      let chunk = if n == blockSize: blk else: blk[0 ..< n]
      st.update(chunk)
      sent += n
      prog.rate.add n
      chunk))

  if res.status != 200:
    cfg.failHard("/upload -> " & $res.status)
  cfg.checkVersion(res.httpVersion)   # hard-fail if the streamed upload downgraded
  let clientSha = st.hex
  # A 200 whose body is not the verification JSON is a verification MISS, not a
  # transient: unwrapped, parseJson's exception surfaced to the caller, which (in
  # the mixed cell) tallies a retry and carries on, so an upload the server never
  # actually confirmed would pass as soak noise.
  var j: JsonNode
  try:
    j = parseJson(res.body)
  except CatchableError as e:
    cfg.failHard("/upload: non-JSON body: " & e.msg & " (body '" & res.body & "')")
    return
  let serverSha = j{"sha1"}.getStr
  let serverSize = j{"size"}.getInt
  if serverSha != clientSha or serverSize != sent:
    cfg.failHard("checksum mismatch\n" &
      "  sent " & $sent & " bytes, client sha1=" & clientSha & "\n" &
      "  server got " & $serverSize & " bytes, sha1=" & serverSha)
