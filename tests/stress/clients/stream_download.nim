## stressStreamDownload, async backends (built twice: asyncdispatch, -d:useChronos).
##
## Streams `streamBytes` (default 1 GiB) down from /download and hashes each chunk
## incrementally, discarding it (never buffering the file), then compares to the
## server's `x-sha1`. A checksum mismatch FAILS HARD (exit 1). A transient transport
## error (e.g. the server recycled/idle-closed a pooled connection mid-transfer) is
## retried, not fatal. Repeats while time remains; a background reporter prints
## cumulative progress + RSS on the interval regardless of per-transfer duration.

import std/[times, strutils]
import ../common/[config, reporter, servers, streamcontent]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset

type Progress = ref object
  bytes: int          ## cumulative bytes received across all transfers
  transfers: int      ## completed+verified transfers
  errors: int         ## retried transient transport failures

proc oneDownload(api: Navi, cfg: Config, prog: Progress, url: string) {.async.} =
  var st = newSha1State()
  var got = 0
  let res = await api.stream(GET, url)
  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /download -> " & $res.status
    quit(1)
  cfg.checkVersion(res.httpVersion)   # hard-fail on a silent protocol downgrade
  let expected = res.headers.get("x-sha1").toLowerAscii
  res.each(chunk):
    if chunk.len > 0:
      st.update(chunk)                 # hash then discard: never buffered
      got += chunk.len
      prog.bytes += chunk.len
  let clientSha = st.hex
  if clientSha != expected:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch\n" &
      "  got " & $got & " bytes, client sha1=" & clientSha & "\n" &
      "  server x-sha1=" & expected
    quit(1)

proc reporterLoop(cfg: Config, prog: Progress, start, deadline: float) {.async.} =
  var last = start
  while epochTime() < deadline:
    await sleep(1000)                  # 1s granularity: stop within ~1s of the deadline
    if epochTime() - last >= cfg.reportSeconds.float:
      last = epochTime()
      echo cfg.label, " ", prog.bytes div (1 shl 20), "MB rx | ",
           prog.transfers, " done | ", prog.errors, " retried | RSS ",
           fmtBytes(rssBytes()), " | heap ", fmtBytes(getOccupiedMem())

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)

  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)     # honor NAVI_PROTO (h3 was previously ignored here)
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  # Warm up h3 discovery per origin (an Alt-Svc round-trip) before the measured phase.
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for base in pool.all():
      for _ in 0 ..< 3:
        try:
          if (await api.request(GET, base & "/echo")).httpVersion == expect: break
        except CatchableError: break

  let start = epochTime()
  let deadline = start + cfg.seconds
  let prog = Progress()
  let rep = reporterLoop(cfg, prog, start, deadline)
  while epochTime() < deadline:
    try:
      await oneDownload(api, cfg, prog, pool.pick() & "/download?size=" & $cfg.streamBytes)
      inc prog.transfers
    except CatchableError as e:        # transient (recycled/idle-closed conn): retry
      inc prog.errors
      stderr.writeLine cfg.label & " transfer retried: " & e.msg
  await rep

  if prog.transfers == 0:
    stderr.writeLine cfg.label & " FAIL: no transfer completed (" & $prog.errors & " errors)"
    quit(1)
  echo "== streamDownload ", backend, " passed (", prog.transfers, " x ",
       cfg.streamBytes, " bytes, ", prog.errors, " retried) =="

waitFor main()
