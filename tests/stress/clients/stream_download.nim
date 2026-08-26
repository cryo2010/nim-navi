## stressStreamDownload, async backends (built twice: asyncdispatch, -d:useChronos).
##
## Streams `streamBytes` (default 1 GiB) down from /download and hashes each chunk
## incrementally, discarding it (never buffering the file), then compares to the
## server's `x-sha1`. A mismatch FAILS HARD (exit 1). Repeats while time remains.
## Progress + RSS printed every report interval.

import std/[times, strutils]
import ../common/[config, reporter, servers, streamcontent]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"

proc oneDownload(api: Navi, cfg: Config, url: string) {.async.} =
  var st = newSha1State()
  var got = 0
  var lastReport = epochTime()
  let res = await api.stream(GET, url)
  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /download -> " & $res.status
    quit(1)
  let expected = res.headers.get("x-sha1").toLowerAscii
  res.each(chunk):
    if chunk.len > 0:
      st.update(chunk)                 # hash then discard: never buffered
      got += chunk.len
      let now = epochTime()
      if now - lastReport >= cfg.reportSeconds.float:
        lastReport = now
        echo cfg.label, " down ", got div (1 shl 20), "/",
             cfg.streamBytes div (1 shl 20), "MB | RSS ", rssBytes() div (1 shl 20), "MB"

  let clientSha = st.hex
  if clientSha != expected:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch\n" &
      "  got " & $got & " bytes, client sha1=" & clientSha & "\n" &
      "  server x-sha1=" & expected
    quit(1)
  echo cfg.label, " verified: sha1 match (", got, " bytes)"

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
  while true:
    await oneDownload(api, cfg, pool.pick() & "/download?size=" & $cfg.streamBytes)
    inc transfers
    if epochTime() >= deadline: break
  echo "== streamDownload ", backend, " passed (", transfers, " x ",
       cfg.streamBytes, " bytes) =="

waitFor main()
