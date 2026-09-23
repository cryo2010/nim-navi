# --- sync-backend variant ---------------------------------------------------
# The sync clients call these inline: one chaos client, one interaction interleaved
# per M verified requests. No fan-out; navi's own timeouts + run.sh's external
# `timeout` wrapper are the hang backstop (documented: sync has no in-proc watchdog).

type SyncChaos* = object
  active*: bool
  plan: ChaosPlan
  api: Navi
  rng: ChaosRng
  base: LeakBaseline

proc syncChaosStart*(cfg: Config, base: LeakBaseline): SyncChaos =
  ## Build the sync chaos state. A no-op (active=false) when off / no mode applies.
  if not cfg.chaos.enabled: return
  result.plan = buildPlan(cfg)
  if not result.plan.active:
    echo chaosLabel(cfg), " chaos skip: no modes applicable to ", cfg.proto
    return
  result.active = true
  result.base = base
  printSchedule(result.plan)
  result.api = mkChaosClient(cfg)
  result.rng = initRng(result.plan.seed)

proc syncChaosStep*(sc: var SyncChaos) =
  ## One interleaved chaos interaction, synchronously. Called by the sync client
  ## every M verified requests. Classifies + tallies exactly like the async path.
  if not sc.active: return
  let e = sc.plan.drawEntry(sc.rng)
  let url =
    case e.port
    of cpVanishAccept, cpStallAccept: chaosBase(sc.plan.cfg, e.port) & "/chaos/accept"
    of cpData: chaosBase(sc.plan.cfg, cpData) & e.build(sc.rng)
  try:
    let res = sc.api.request(GET, url)
    sc.plan.classifyResponse(e, res)
  except CatchableError as err:
    sc.plan.classifyError(e, err)

proc syncChaosReport*(sc: SyncChaos) =
  if not sc.active: return
  reportChaos(chaosLabel(sc.plan.cfg), sc.plan.counter, fdCount())

proc syncChaosFinish*(sc: var SyncChaos, verified: seq[Navi] = @[]) =
  ## Close BOTH the chaos client and the verified soak's clients, drain, assert.
  ## Closing the verified clients too makes the process-wide FD bracket honest
  ## (their long-lived pooled connections are legitimate live fds during the run).
  ## Zero-ops hard-fails like the async path.
  if not sc.active: return
  if sc.plan.counter.ops == 0:
    sc.plan.cfg.chaosFail("mode", "no chaos interaction completed (sync)")
  reportChaos(chaosLabel(sc.plan.cfg), sc.plan.counter, fdCount())
  # Re-base heap/RSS to the peak, pre-teardown working set (see rebaseMemPreTeardown).
  var base = sc.base
  rebaseMemPreTeardown(base)
  try: sc.api.close()
  except CatchableError: discard
  for api in verified:
    try: api.close()
    except CatchableError: discard
  blockingSleep(drainMs(sc.plan.cfg.proto))
  assertNoLeak(chaosLabel(sc.plan.cfg), base, sc.plan.cfg.chaos)
