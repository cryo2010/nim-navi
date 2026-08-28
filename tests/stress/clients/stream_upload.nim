## stressStreamUpload, async backends (built twice: asyncdispatch, -d:useChronos).
##
## Streams `streamBytes` (default 1 GiB) up to /upload as a pull-based chunked body
## (constant memory: one reused 1 MiB block, hashed as it flies), then compares the
## client's incremental SHA-1 to the one the server computed over what it received.
## A checksum/size mismatch FAILS HARD (exit 1). A transient transport error (e.g.
## the server recycled/idle-closed a pooled connection mid-transfer) is retried, not
## fatal. Repeats while time remains; a background reporter prints cumulative
## progress + RSS on the interval regardless of per-transfer duration.

import std/[times, json]
import ../common/[config, reporter, servers, streamcontent]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"

let blk = fillBlock()

type Progress = ref object
  bytes: int          ## cumulative bytes sent across all transfers
  transfers: int      ## completed+verified transfers
  errors: int         ## retried transient transport failures

proc oneUpload(api: Navi, cfg: Config, prog: Progress, url: string) {.async.} =
  var st = newSha1State()
  var remaining = cfg.streamBytes
  var sent = 0
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
      prog.bytes += n
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

proc reporterLoop(cfg: Config, prog: Progress, start, deadline: float) {.async.} =
  var last = start
  while epochTime() < deadline:
    await sleep(1000)                  # 1s granularity: stop within ~1s of the deadline
    if epochTime() - last >= cfg.reportSeconds.float:
      last = epochTime()
      echo cfg.label, " ", prog.bytes div (1 shl 20), "MB tx | ",
           prog.transfers, " done | ", prog.errors, " retried | RSS ",
           fmtBytes(rssBytes()), " | heap ", fmtBytes(getOccupiedMem())

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
  let prog = Progress()
  let rep = reporterLoop(cfg, prog, start, deadline)
  while epochTime() < deadline:
    try:
      await oneUpload(api, cfg, prog, pool.pick() & "/upload")
      inc prog.transfers
    except CatchableError as e:        # transient (recycled/idle-closed conn): retry
      inc prog.errors
      stderr.writeLine cfg.label & " transfer retried: " & e.msg
  await rep

  if prog.transfers == 0:
    stderr.writeLine cfg.label & " FAIL: no transfer completed (" & $prog.errors & " errors)"
    quit(1)
  echo "== streamUpload ", backend, " passed (", prog.transfers, " x ",
       cfg.streamBytes, " bytes, ", prog.errors, " retried) =="

waitFor main()
