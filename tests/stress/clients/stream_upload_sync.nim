## stressStreamUpload, sync backend (`import navi`). Streams `streamBytes` up to
## /upload as a pull-based chunked body (constant memory), hashing as it flies, and
## compares to the server's SHA-1. Mismatch FAILS HARD (exit 1). Repeats while time
## remains.

import std/[times, json]
import ../common/[config, reporter, servers, streamcontent]
import navi

let blk = fillBlock()

proc oneUpload(api: Navi, cfg: Config, url: string) =
  var st = newSha1State()
  var remaining = cfg.streamBytes
  var sent = 0
  var lastReport = epochTime()
  var h = initHeaders()
  h["content-type"] = "application/octet-stream"

  let res = api.request(POST, url, headers = h,
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
             cfg.streamBytes div (1 shl 20), "MB | RSS ", fmtBytes(rssBytes()),
             " | heap ", fmtBytes(getOccupiedMem())
      chunk)

  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /upload -> " & $res.status
    quit(1)
  let clientSha = st.hex
  let j = parseJson(res.body)
  if j{"sha1"}.getStr != clientSha or j{"size"}.getInt != sent:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch\n" &
      "  sent " & $sent & " bytes, client sha1=" & clientSha & "\n" &
      "  server got " & $j{"size"}.getInt & " bytes, sha1=" & j{"sha1"}.getStr
    quit(1)

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  var c = initNaviConfig()
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  let deadline = epochTime() + cfg.seconds
  var transfers = 0
  while true:
    oneUpload(api, cfg, pool.pick() & "/upload")
    inc transfers
    if epochTime() >= deadline: break
  echo "== streamUpload sync passed (", transfers, " x ", cfg.streamBytes, " bytes) =="

main()
