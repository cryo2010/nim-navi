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
                                    ## a blocking sync client just ignores it and
                                    ## runs one sequential loop)
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
  let per = max(1, (total + n - 1) div n)     # per-thread in-flight (ceil)
  let now = epochTime()
  let measureStart = now + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  var args = newSeq[BenchThread](n)
  var ths = newSeq[Thread[ptr BenchThread]](n)
  for i in 0 ..< n:
    args[i] = BenchThread(cfg: cfg, id: i, concurrency: per,
                          measureStart: measureStart, deadline: deadline)
  for i in 0 ..< n:
    createThread(ths[i], body, addr args[i])
  for i in 0 ..< n:
    joinThread(ths[i])
  let merged = newBenchRecorder()
  for i in 0 ..< n:
    if args[i].rec != nil: merged.merge(args[i].rec)
  emitResult(name, merged, cfg.seconds)
