## stressRequests, async backends (one source, built twice):
##   nim c -d:ssl ...                -> navi/asyncdispatch
##   nim c -d:ssl -d:useChronos ...  -> navi/chronos
##
## A buffered request/response soak: `clients` navi clients, each fanning out
## `concurrency` workers that loop every verb against `/echo` across the server pool
## until the deadline. A middleware stamps x-stress (exercises the chain). Each
## response is fully verified -- status is 200, x-echo-method matches the verb,
## x-echo-stress is echoed, and the body matches per content-type kind: octet/text
## byte-exact (decompressed), json by parsed-tree equality, form by decoded-pair
## equality (so a server byte-echo cannot make json/form pass). Bodies rotate
## through octet/text/json/form (see common/payloads) via a two-index verb x payload
## cross product, restricted by NAVI_CONTENT_TYPES. A verification miss or transport
## error FAILS HARD. The protocol is pinned via config.http, so a silent downgrade
## also fails.

import std/[times, strutils, json]
import ../zlibcodec
import ../common/[config, reporter, servers, payloads, leakcheck]

when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset
include ../common/chaos
include parts/requests_part   # the verified /echo loop (shared with mixed.nim)

proc reporterLoop(cfg: Config, counter: StatusCounter,
                  start, deadline: float) {.async.} =
  var last = start
  while epochTime() < deadline:
    await sleep(1000)                   # 1s granularity: stop within ~1s of the deadline
    if epochTime() - last >= cfg.reportSeconds.float:
      last = epochTime()
      report(cfg.label, counter, epochTime() - start)

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  let notice = cfg.chaosSkipNotice        # js chaos-skip notice, if any
  if notice.len > 0: echo cfg.label, " ", notice
  var leakBase = sampleBaseline(cfg.chaos)   # before ANY Navi is constructed
  var pool = initServerPool(cfg)
  let payloads = filterPayloads(allPayloads, cfg.contentTypes, jsSafe = false)
  let counter = newStatusCounter()
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  # Warm up each client's per-origin protocol so the measured phase runs pinned from
  # the first request: h3 is discovered via an initial Alt-Svc round-trip, so hit
  # each origin until the expected version is negotiated. After this, any downgrade
  # during the soak fails hard (checkVersion), catching a silent fallback.
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for api in apis:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if (await api.request(GET, base & "/echo")).httpVersion == expect: break
          except CatchableError: break

  await featureChecks(cfg, pool.all()[0])   # redirect/status/auth/cookie coverage

  let start = epochTime()
  let deadline = start + cfg.seconds
  let chaos = chaosMaybeStart(cfg, deadline, cfg.reportSeconds)  # no-op when off
  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add requestsWorker(api, cfg, addr pool, payloads, counter, deadline, i)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f
  await chaosAwait(chaos)                  # drain the chaos workers/watchdog

  if counter.ops == 0: cfg.failHard("no request completed")   # a cell must do work
  let elapsed = epochTime() - start      # the measured phase: also the ops/s divisor
  report(cfg.label & " final", counter, elapsed, final = true)
  await chaosFinish(chaos, leakBase, cfg, apis)  # close all clients, drain, leak assert
  echo "== requests ", backend, " ", cfg.proto, " passed (", counter.ops, " ops, ",
    fmtRate(counter.ops, elapsed), " ops/s) =="

waitFor main()
