# One verified streamed download, shared by clients/stream_download.nim and the
# streamDownload slice of clients/mixed.nim. `include`d (not imported) AFTER the
# including file's `import navi[/backend]` + `include ../common/httpset`, like
# common/httpset and common/chaos, because the stream Response/`each`/Future come
# from that backend. Not a standalone module. Needs std/strutils and
# ../common/[config, streamcontent] from the including file. The shared
# StreamProgress lives in common/streamcontent so both stream parts (and a client
# that includes only one of them) see the same type.
#
# Constant memory: each chunk is hashed and discarded, never buffered. A checksum
# mismatch against the server's x-sha1, a non-200 or a protocol downgrade is a
# hard fail. A transient transport error is the caller's to retry.

proc oneDownload(api: Navi, cfg: Config, prog: StreamProgress, url: string) {.async.} =
  var st = newSha1State()
  var got = 0
  let res = await api.stream.get(url)
  if res.status != 200:
    cfg.failHard("/download -> " & $res.status)
  cfg.checkVersion(res.httpVersion)   # hard-fail on a silent protocol downgrade
  let expected = res.headers.get("x-sha1").toLowerAscii
  res.each(chunk):
    if chunk.len > 0:
      st.update(chunk)                 # hash then discard: never buffered
      got += chunk.len
      prog.rate.add chunk.len
  let clientSha = st.hex
  if clientSha != expected:
    cfg.failHard("checksum mismatch\n" &
      "  got " & $got & " bytes, client sha1=" & clientSha & "\n" &
      "  server x-sha1=" & expected)
