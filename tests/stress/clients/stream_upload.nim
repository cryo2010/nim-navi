## stressStreamUpload, async backends (built twice: asyncdispatch, -d:useChronos).
##
## Streams `streamBytes` (default 1 GiB) up to /upload as a pull-based chunked body
## (constant memory: one reused 1 MiB block, hashed as it flies), then compares the
## client's incremental SHA-1 to the one the server computed over what it received.
## A mismatch (or size mismatch) FAILS HARD (exit 1). Repeats transfers while time
## remains; each must verify. Progress + RSS printed every report interval.

import std/[times, json]
import ../common/[config, reporter, servers, streamcontent]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"

let blk = fillBlock()

proc oneUpload(api: Navi, cfg: Config, url: string) {.async.} =
  var st = newSha1State()
  var remaining = cfg.streamBytes
  var sent = 0
  var lastReport = epochTime()
  var h = initHeaders()
  h["content-type"] = "application/octet-stream"

  let res = await api.request(POST, url, headers = h,
    bodyStream = proc(): string =
      if remaining <= 0: return ""
      let n = min(blockSize, remaining)
      remaining -= n
      let chunk = if n == blockSize: blk else: blk[0 ..< n]
      st.update(chunk)
      sent += n
      let now = epochTime()
      if now - lastReport >= cfg.reportSeconds.float:
        lastReport = now
        echo cfg.label, " up ", sent div (1 shl 20), "/",
             cfg.streamBytes div (1 shl 20), "MB | RSS ", rssBytes() div (1 shl 20), "MB"
      chunk)

  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /upload -> " & $res.status
    quit(1)
  let clientSha = st.hex
  let j = parseJson(res.body)
  let serverSha = j{"sha1"}.getStr
  let serverSize = j{"size"}.getInt
  if serverSha != clientSha or serverSize != sent:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch\n" &
      "  sent " & $sent & " bytes, client sha1=" & clientSha & "\n" &
      "  server got " & $serverSize & " bytes, sha1=" & serverSha
    quit(1)

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)

  var c = initNaviConfig()
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  let start = epochTime()
  let deadline = start + cfg.seconds
  var transfers = 0
  while true:                          # at least one full transfer, then while time remains
    await oneUpload(api, cfg, pool.pick() & "/upload")
    inc transfers
    if epochTime() >= deadline: break
  echo "== streamUpload ", backend, " passed (", transfers, " x ",
       cfg.streamBytes, " bytes) =="

waitFor main()
