## stressMixed, navi/js backend (Node): four of the five workloads at once
## (requests, ws, sse, streamDownload) against one set of servers, through the
## same `Navi` instances, shared round-robin across every slice.
##
## js cannot stream a request body (`fetch` buffers it, which would defeat a
## 1 GiB soak), so there is no upload slice here and its worker share folds into
## the download slice. That is the one documented gap against the native mixed
## cell; everything else is the same shape: each slice keeps its own verification
## and its own counter, the counters are reported side by side and never summed,
## and each slice has its own zero-work check so a dead slice cannot hide behind
## a healthy one.
##
## The work procs are the shared `parts/*_js_part.nim` includes the four
## single-workload js clients use, so this cell drives the same verified code.
## The runner trusts the self-signed cert via NODE_EXTRA_CA_CERTS.

import std/[strutils, json]
import navi/js
import ../common/[harness_js, payloads, streamcontent, mixsplit]
include parts/requests_js_part          # /echo: verbs x js-safe payload catalog
include parts/ws_js_part                # ws: text+binary echo round-trips
include parts/sse_js_part               # sse: events + Last-Event-ID resume
include parts/stream_download_js_part   # one verified streamed download

proc sleepMs(ms: int): Future[void] {.importjs:
  "new Promise((r) => setTimeout(r, #))".}
  ## A plain timer promise. navi/js does not re-export the backend's `sleep`, and
  ## the download slice needs a beat after a transport error so an immediately
  ## refused connection cannot be retried in a tight microsecond loop.

proc main() {.async.} =
  let cfg = loadJsCfg()
  let label = "[mixed " & cfg.proto & " js]"
  var pool = initJsPool(cfg)
  let payloads = filterPayloads(stressPayloads(), cfg.contentTypes, jsSafe = true)
  if payloads.len == 0:
    # NAVI_CONTENT_TYPES=octet leaves nothing js-safe (binary is excluded on js),
    # so skip cleanly instead of `mod 0`-crashing in the requests slice.
    echo label, " skipped: no js-safe payloads in NAVI_CONTENT_TYPES=", cfg.contentTypes
    return
  let reqCounter = newJsCounter()
  let wsCounter = newJsCounter()
  let sseCounter = newJsCounter()

  # The shared instances, with requests' x-stress middleware (harmless on /ws,
  # /events and /download). Every slice draws from this one seq round-robin by
  # worker index, so the slices share origins and connections, not just servers.
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkJsClient()

  # Same split rule as the native cell (common/mixsplit), with the upload share
  # folded into download since js has no upload slice.
  let sp = mixSplit(cfg.clients * cfg.concurrency)
  let nDown = sp.down + sp.up
  echo label, " split req=", sp.req, " ws=", sp.ws, " sse=", sp.sse,
       " down=", nDown, " (no upload slice: js cannot stream a request body)"

  # Open the sockets and streams up front, so the soak that follows is not racing
  # handshakes for accepts.
  # `k` is ONE running worker counter across every slice, so the instances are
  # dealt out round-robin over the whole mix. Indexing each slice from 0 instead
  # left the last instance carrying requests only, never a stream.
  var k = 0
  var socks: seq[WebSocket]
  for _ in 0 ..< sp.ws:
    socks.add await apis[k mod apis.len].websocket(wsUrl(pool.pick()))
    inc k
  var streams: seq[SseStream]
  for _ in 0 ..< sp.sse:
    streams.add await apis[k mod apis.len].sse(pool.pick() & "/events",
      retryMs = 20, maxRetryMs = 100, minRetryMs = 20)
    inc k

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  var transfers = 0
  var retried = 0                        # retried transient transport errors
  # Float, not int, for the byte totals: the js backend overflow-checks int at
  # 2^31 and a soak crosses 2 GiB in seconds, so StreamRate (whose total is an
  # int) is not usable here -- same reason clients/stream_download_js.nim keeps
  # its own float total. The MB and MB/s math is still streamcontent's.
  var bytes = 0.0                        # cumulative bytes rx
  var lastBytes = 0.0                    # `bytes` as of the previous report line
  var lastMs = start                     # timestamp of the previous report line

  proc mixedReport(final = false) =
    ## ONE line for the whole mix: every slice's own numbers side by side, never
    ## a sum. The rate fields cover the interval since the previous line (the
    ## whole-run average when `final`), so a mid-soak dip shows where it happened.
    ##
    ## Two divisors on the final line, as in the native cell. The req/ws/sse
    ## slices stop at the deadline, so they divide by the time the deadline was
    ## reached (`opsStart` is `start` shifted forward by any overrun, since
    ## `segment` takes a start rather than a duration); the download fields
    ## divide by the full elapsed, which is what those bytes really took.
    let now = nowMs()
    let ran = (now - start) / 1000.0
    let opsStart = if final: now - min(ran, cfg.seconds) * 1000.0 else: start
    let winSecs = (now - (if final: start else: lastMs)) / 1000.0
    let mbps = mbPerSec((if final: bytes else: bytes - lastBytes), winSecs)
    if not final:
      lastBytes = bytes
      lastMs = now
    echo label, " ",
      reqCounter.segment("req", "ops", opsStart, statuses = true, final = final), " | ",
      wsCounter.segment("ws", "rt", opsStart, final = final), " | ",
      sseCounter.segment("sse", "ev", opsStart, final = final), " | ",
      "down ", megabytes(bytes), "MB ", mbps, " MB/s ", transfers,
      " done ", retried, " retried | RSS ", rssMb(), "MB | heap ", heapUsedMb(),
      "MB | t=", int((now - start) / 1000.0), "s"

  let timer = setIntervalJs(proc () = mixedReport(), cfg.reportSeconds * 1000)

  proc downloadSlice(api: Navi) {.async.} =
    ## One download worker: streamed /download transfers back to back until the
    ## deadline. A checksum or status miss inside oneDownload is a hard fail; a
    ## transport error is tallied as retried and the worker carries on, mirroring
    ## the native cell (unhandled, it became a bare promise rejection: no FAIL
    ## line and no banner, so the cell "ended" without saying anything).
    ##
    ## `lastDur` is this worker's last observed transfer duration and gates the
    ## next start: a transfer sharing pooled connections with the other slices
    ## takes seconds, so one begun just before the deadline runs long past it and
    ## charges its overrun to every other slice's rate. The first always starts.
    var lastDur = 0.0
    while true:
      let now = nowMs()
      if now >= deadline: break
      if lastDur > 0.0 and deadline - now < lastDur: break
      try:
        bytes += float(await oneDownload(api, label,
          pool.pick() & "/download?size=" & $cfg.streamBytes))
        inc transfers
        lastDur = nowMs() - now
      except CatchableError as e:
        inc retried
        echo label, " download retried: ", e.msg
        await sleepMs(250)             # do not hot-spin on an immediate refusal

  var futs: seq[Future[void]]
  for i in 0 ..< sp.req:
    futs.add requestsWorker(apis[k mod apis.len], label, pool, payloads,
                            reqCounter, deadline, i)
    inc k
  for s in socks: futs.add wsWorker(s, wsCounter, deadline)
  for s in streams: futs.add sseWorker(label, s, sseCounter, deadline)
  for _ in 0 ..< nDown:
    futs.add downloadSlice(apis[k mod apis.len])
    inc k
  # The interval timer is cleared only after every slice has finished, so a
  # transfer that overruns the deadline stays visible on the interval lines (the
  # native cell drives its reporter off a live-worker count for the same reason).
  for f in futs: await f
  clearIntervalJs(timer)

  # Per-slice zero-work checks: on the sum a dead slice would hide behind a
  # healthy one.
  # `ops - errors`, not `ops`: JsCounter.note() increments ops too, so an
  # all-failing ws or sse slice would otherwise pass this check silently.
  if reqCounter.ops == 0: jsFail(label, "no request completed")
  if wsCounter.ops - wsCounter.errors == 0:
    jsFail(label, "no WebSocket round-trip completed")
  if sseCounter.ops - sseCounter.errors == 0:
    jsFail(label, "no SSE event consumed")
  # Bytes, not transfers: the download slice shares connections with the other
  # slices, so a healthy cell can end with a transfer still in flight and 0
  # completed. Zero bytes moved is the real stall.
  if bytes == 0.0: jsFail(label, "no download bytes moved (" & $retried & " errors)")

  let elapsed = (nowMs() - start) / 1000.0   # the measured phase, overrun included
  let opsRan = min(elapsed, cfg.seconds)     # where the req/ws/sse slices stopped
  mixedReport(final = true)
  echo "== mixed js ", cfg.proto, " passed (req ", reqCounter.ops, " ops ",
    fmtRate(reqCounter.ops, opsRan), " ops/s, ws ", wsCounter.ops,
    " round-trips, sse ", sseCounter.ops, " events, down ",
    megabytes(bytes), "MB rx ", transfers, " transfers, ", retried,
    " retried) =="

discard main()
