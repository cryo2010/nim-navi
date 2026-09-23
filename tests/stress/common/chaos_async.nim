proc runOne(plan: ptr ChaosPlan, api: Navi, r: ptr ChaosRng): Future[void] {.async.} =
  ## One chaos interaction: draw an entry, build its concrete request from the
  ## PRNG coins, issue it against the right port, classify the outcome. `plan`/`r`
  ## are `ptr` (not `var`) because the async transform captures the proc's params
  ## into a closure env, and a `var T` param cannot be captured (memory safety).
  # Draw the entry + build the request BEFORE any await, so the mutable RNG is not
  # touched across a suspension point.
  let e = plan[].drawEntry(r[])
  let target =
    case e.port
    of cpVanishAccept, cpStallAccept: chaosBase(plan[].cfg, e.port) & "/chaos/accept"
    of cpData: chaosBase(plan[].cfg, cpData) & e.build(r[])
  try:
    let res = await api.request(GET, target)
    plan[].classifyResponse(e, res)
  except CatchableError as err:
    plan[].classifyError(e, err)

proc chaosWorker(plan: ptr ChaosPlan, api: Navi, i: int, deadline: float,
                 seed: uint64, stamps: ptr seq[float]) {.async.} =
  var r = initRng(seed)
  let selfTest = plan[].cfg.chaos.selfTest
  var iter = 0
  while epochTime() < deadline:
    stamps[][i] = epochTime()          # progress stamp for the watchdog
    inc iter
    # hang self-test: one worker sleeps forever, so the watchdog must catch it.
    if selfTest == "hang" and i == 0:
      await sleep(1000_000_000)
    await runOne(plan, api, addr r)
    # fd self-test: on worker 0, leak one connected+retained socket every few
    # interactions so the final FD count blows past the slack (proving the FD
    # assertion has teeth). A fast mode (redirectloop's first hop) keeps the leak
    # loop brisk; the client is retained in a global and never closed, so its
    # connection's fd is never reclaimed. Kept on one worker so the count is
    # predictable across backends given the low per-worker op rate.
    if selfTest == "fd":
      # Leak a real, never-closed file descriptor every interaction so /proc/self/fd
      # climbs well past the slack (proving the FD assertion has teeth). The chaos
      # modes are all one-shot (the server closes every connection), so no navi
      # connection stays open to leak; instead dup an existing fd -- a genuine,
      # deterministic fd leak with no network flakiness. All workers leak so the
      # count grows fast even at the low per-worker chaos op rate. Retained in a
      # global so it is never collected/closed.
      selfTestLeakedFdNums.add leakOneFd()
    # mem self-test: retain a large body in a global every couple of interactions
    # so the retained heap clears even a modest slack (4 MiB * a handful of retains
    # exceeds the default 32 MiB well before the run ends).
    if selfTest == "mem" and iter mod 2 == 0:
      selfTestRetained.add newString(4 * 1024 * 1024)
    # Cooperative yield between interactions. The chaos and verified soaks share one
    # event loop by design (a hostile-server bug that corrupts the loop should take
    # the canary down too). But we are testing the client's handling of hostile
    # SERVERS, not the fairness of a single-threaded scheduler: without this yield,
    # N tight chaos workers can starve the canary's socket servicing enough that a
    # loopback keep-alive connection is closed/truncated mid-response -- collateral
    # loop starvation, not a navi bug. The chaos catalog is qualitative (each mode is
    # exercised for coverage, not throughput), so a per-interaction pause keeps the
    # canary healthy while still turning over hundreds of chaos ops per second across
    # the worker pool. Chronos's structured cancellation tolerates the load without
    # this, but the yield is harmless there.
    await sleep(chaosYieldMs)

proc chaosWatchdog(plan: ptr ChaosPlan, deadline: float,
                   stamps: ptr seq[float]) {.async.} =
  ## Wakes every 5s; hard-fails if any worker has not stamped progress within
  ## NAVI_CHAOS_WATCHDOG seconds. This is the layer that catches the exact bug
  ## hunted here: navi's own timeout machinery failing to fire.
  let limit = plan[].cfg.chaos.watchdog.float
  while epochTime() < deadline:
    await sleep(5000)
    let now = epochTime()
    for i in 0 ..< stamps[].len:
      if now - stamps[][i] > limit:
        plan[].cfg.chaosFail("hang", "worker " & $i & " stuck " &
          $int(now - stamps[][i]) & "s (watchdog=" & $int(limit) & "s)")

proc chaosReporter(plan: ptr ChaosPlan, deadline: float, reportSeconds: int) {.async.} =
  var last = epochTime()
  while epochTime() < deadline:
    await sleep(1000)
    if epochTime() - last >= reportSeconds.float:
      last = epochTime()
      reportChaos(chaosLabel(plan[].cfg), plan[].counter, fdCount())

# --- public entry points (called by the clients) ----------------------------

type ChaosRuntime* = object
  ## Opaque handle the client holds between start and finish. Carries the plan,
  ## the chaos Navi instances (closed at the end), and whether it is active.
  active*: bool
  plan: ChaosPlan
  apis: seq[Navi]
  futs: seq[Future[void]]
  stamps: seq[float]

proc chaosMaybeStart*(cfg: Config, deadline: float,
                      reportSeconds: int): ref ChaosRuntime =
  ## Called by an async client alongside its verified workers. Builds the plan,
  ## prints the schedule digest, and launches conc workers + a watchdog +
  ## reporter, all on the shared event loop. Returns a runtime the client passes
  ## to chaosFinish. A no-op (active=false) when chaos is off or no mode applies.
  new(result)
  if not cfg.chaos.enabled:
    result.active = false
    return
  result.plan = buildPlan(cfg)
  if not result.plan.active:
    echo chaosLabel(cfg), " chaos skip: no modes applicable to ", cfg.proto,
         " under NAVI_CHAOS=", cfg.chaos.raw
    result.active = false
    return
  result.active = true
  printSchedule(result.plan)
  let conc = cfg.chaos.conc
  result.stamps = newSeq[float](conc)
  for i in 0 ..< conc: result.stamps[i] = epochTime()
  for _ in 0 ..< conc: result.apis.add mkChaosClient(cfg)
  let planPtr = addr result.plan
  let stampsPtr = addr result.stamps
  for i in 0 ..< conc:
    result.futs.add chaosWorker(planPtr, result.apis[i], i, deadline,
                                result.plan.seed + uint64(i) * 0x9E37'u64, stampsPtr)
  result.futs.add chaosWatchdog(planPtr, deadline, stampsPtr)
  result.futs.add chaosReporter(planPtr, deadline, reportSeconds)

proc chaosAwait*(rt: ref ChaosRuntime) {.async.} =
  ## Await all chaos futures (workers/watchdog/reporter). Called after the
  ## verified workers' futures. A no-op when inactive.
  if not rt.active: return
  for f in rt.futs: await f

proc chaosFinish*(rt: ref ChaosRuntime, base: LeakBaseline, cfg: Config,
                  verified: seq[Navi] = @[]) {.async.} =
  ## End of cell: close BOTH the chaos clients and the verified soak's clients,
  ## drain (5s h1/h2, 20s h3), then run the final leak assertions. Closing the
  ## verified clients too is what makes the process-wide FD/heap bracket honest:
  ## their long-lived pooled connections are legitimate live fds during the run,
  ## so leaving them open would count against the leak bound. The verified soak
  ## itself never needs an explicit close (its clients die with the process), so
  ## we take that responsibility here only when the leak check is armed. A
  ## chaos-enabled cell with zero chaos ops hard-fails, mirroring the existing
  ## zero-ops rule. No-op when inactive.
  if not rt.active:
    return
  if rt.plan.counter.ops == 0:
    cfg.chaosFail("mode", "no chaos interaction completed (a chaos cell must attack)")
  # final chaos report before teardown so the tallies are visible.
  reportChaos(chaosLabel(cfg), rt.plan.counter, fdCount())
  # Re-base heap/RSS to the peak, pre-teardown working set: the memory leak signal
  # is whether close+drain RECLAIMS, not how big the concurrent soak got.
  var base = base
  rebaseMemPreTeardown(base)
  for api in rt.apis:
    try: await api.close()
    except CatchableError: discard
  rt.apis.setLen(0)
  for api in verified:
    try: await api.close()
    except CatchableError: discard
  await sleep(drainMs(cfg.proto))
  assertNoLeak(chaosLabel(cfg), base, cfg.chaos)

