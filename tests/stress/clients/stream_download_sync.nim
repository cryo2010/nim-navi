## stressStreamDownload, sync backend (`import navi`). Streams `streamBytes` down
## from /download, hashing each chunk and discarding it (never buffered), then
## compares to the server's x-sha1. Mismatch FAILS HARD (exit 1). Repeats while
## time remains.

import std/[times, strutils]
import ../common/[config, reporter, servers, streamcontent]
import navi

proc oneDownload(api: Navi, cfg: Config, url: string) =
  var st = newSha1State()
  var got = 0
  var lastReport = epochTime()
  let res = api.stream(GET, url)
  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /download -> " & $res.status
    quit(1)
  let expected = res.headers.get("x-sha1").toLowerAscii
  res.each(chunk):
    if chunk.len > 0:
      st.update(chunk)
      got += chunk.len
      let now = epochTime()
      if now - lastReport >= cfg.reportSeconds.float:
        lastReport = now
        echo cfg.label, " down ", got div (1 shl 20), "/",
             cfg.streamBytes div (1 shl 20), "MB | RSS ", fmtBytes(rssBytes()),
             " | heap ", fmtBytes(getOccupiedMem())

  let clientSha = st.hex
  if clientSha != expected:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch\n" &
      "  got " & $got & " bytes, client sha1=" & clientSha & "\n" &
      "  server x-sha1=" & expected
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
    oneDownload(api, cfg, pool.pick() & "/download?size=" & $cfg.streamBytes)
    inc transfers
    if epochTime() >= deadline: break
  echo "== streamDownload sync passed (", transfers, " x ", cfg.streamBytes, " bytes) =="

main()
