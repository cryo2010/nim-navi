## Multi-threaded runner for navi's native bench clients. navi's async backend is one
## event loop per thread, so we scale across cores by running one navi client per
## THREAD (not one process per core): each thread builds its own client, drives its
## share of the offered load on its own event loop, and records into its own
## recorder. The process then merges them into a single RESULT -- a true combined
## histogram (not averaged per-thread percentiles) and a summed byte total. Backend-
## agnostic: no navi import, so sync/asyncdispatch/chronos clients all share it.

import std/[os, strutils, cpuinfo, times]
import ./[config, reporter]

type BenchThread* = object
  ## Per-thread inputs (read-only) plus the output `rec` (written by the thread).
  ## Passed to the thread body by `ptr`; each thread touches only its own slot, so
  ## no locking is needed and `rec` is read by the parent only after the join.
  cfg*: Config
  id*: int                          ## 0-based thread index
  concurrency*: int                 ## in-flight workers this thread drives (>= 1;
                                    ## this thread's share of the cell's total
                                    ## offered load, so the widths sum to exactly
                                    ## clients * concurrency; a blocking sync client
                                    ## just ignores it and runs one sequential loop)
  measureStart*, deadline*: float   ## the shared measured window (absolute epochTime)
  rec*: BenchRecorder               ## result, allocated inside the thread

proc benchThreads*(cfg: Config): int =
  ## Client threads to run: NAVI_THREADS (default: the machine's cores), capped at
  ## the offered load so we never spawn idle threads.
  let total = max(1, cfg.clients * cfg.concurrency)
  var t = 0
  let env = getEnv("NAVI_THREADS", "")
  if env.len > 0:
    try: t = parseInt(env) except ValueError: t = 0
  if t <= 0: t = countProcessors()
  clamp(t, 1, total)

proc runThreaded*(cfg: Config, name: string,
                  body: proc(a: ptr BenchThread) {.thread, nimcall.}) =
  ## Run `body` on N client threads and print one merged RESULT. `body` builds its
  ## own navi client, drives `a.concurrency` workers over [a.measureStart,
  ## a.deadline), and stores its recorder in `a.rec`. The measured window is fixed
  ## once here so every thread times the same wall-clock interval.
  let n = benchThreads(cfg)
  let total = max(1, cfg.clients * cfg.concurrency)
  # Split the cell's offered load so the per-thread widths sum to EXACTLY `total`:
  # `total div n` each, and the first `total mod n` threads take one extra worker.
  # Every reference client drives exactly clients * concurrency in-flight ops (go:
  # clients*concurrency histograms; rust: n_workers), so a per-thread ceil would
  # hand navi up to n-1 extra workers whenever n does not divide total -- 30 on 10
  # cores, 32 on 16, 40 on 20 against the default 24 -- i.e. more offered load than
  # the peers, inflating navi's throughput and worsening its p99 on the same origin.
  # `benchThreads` clamps n to total, so `per + extra` still gives every thread >= 1.
  let per = total div n
  let extra = total mod n
  let now = epochTime()
  let measureStart = now + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  var args = newSeq[BenchThread](n)
  var ths = newSeq[Thread[ptr BenchThread]](n)
  for i in 0 ..< n:
    args[i] = BenchThread(cfg: cfg, id: i,
                          concurrency: per + (if i < extra: 1 else: 0),
                          measureStart: measureStart, deadline: deadline)
  for i in 0 ..< n:
    createThread(ths[i], body, addr args[i])
  for i in 0 ..< n:
    joinThread(ths[i])
  let merged = newBenchRecorder()
  for i in 0 ..< n:
    if args[i].rec != nil: merged.merge(args[i].rec)
  # Divide by the REAL window, not the nominal cfg.seconds. Every client -- navi and
  # every reference client alike -- records a unit that started before the deadline
  # and completed after it, and each reference client puts that overshoot in its own
  # denominator (go: time.Since(measureStart); rust/node/python likewise). Charging
  # navi only the nominal window would inflate its req/s and MB/s: negligible for
  # requests/sse/ws, large for streaming, where one transfer (1 GiB by default) can
  # outlast the window on its own. Fall back to the nominal window only if the run
  # never reached measureStart (elapsed <= 0), which would make the rate meaningless.
  var elapsed = epochTime() - measureStart
  if elapsed <= 0: elapsed = cfg.seconds
  emitResult(name, merged, elapsed)
