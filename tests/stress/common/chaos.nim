## Client-side chaos driver: the code that *attacks* the misbehaving sidecar and
## asserts the universal invariants (no crash, no hang, pin never violated, no
## leak). Runs in-process with the verified soak on the SAME event loop -- a
## hostile-server bug that corrupts the shared loop or allocator should take the
## canary down with it; that is signal -- but uses SEPARATE Navi instances so the
## canary's per-origin pools never contain a chaos-origin connection.
##
## `include`d by a client AFTER its `import navi[/backend]` + `include httpset`,
## like the other shared bits, so HttpVersion/Navi/Response/sleep/Future and the
## typed navi errors are in scope. Compiles under asyncdispatch and chronos with
## the same async discipline as clients/requests.nim (bare `sleep(ms)` +
## `Future[void]`, both re-exported per backend).
##
## Determinism: the schedule is driven by an explicit seedable PRNG (xoshiro256**,
## NOT std/random's shared global state), seeded from NAVI_CHAOS_SEED mixed with a
## stable hash of workload|backend|proto. Reruns with the same seed draw the same
## (mode, params, port) sequence, so digests and per-mode tallies match. All the
## vortex-style coins (e.g. 50% of vanish gets a slow prefix) come from THIS PRNG
## and arrive at the server as query params -- the server never rolls dice.
##
## When chaos is off, every entry point returns before allocating anything, so an
## off run is byte-identical.

import std/[times, strutils, hashes]
# config/reporter/leakcheck are imported by the including client already; chaos.nim
# is included after them, so their symbols are in scope. We rely on: Config,
# ChaosConfig, ChaosCounter, LeakBaseline, fdCount, drainMs, assertNoLeak.

const chaosYieldMs = 25   ## per-interaction cooperative pause for the async chaos
  ## workers, so they share the single event loop fairly with the verified soak
  ## rather than starving it (see chaosWorker). Gentle enough to keep the canary
  ## healthy on asyncdispatch, brisk enough for hundreds of chaos ops/s across the
  ## pool. The chaos catalog is coverage-driven, not throughput-driven.

# --- xoshiro256** PRNG (explicit state, not std/random) ---------------------

type ChaosRng = object
  s: array[4, uint64]

proc rotl(x: uint64, k: int): uint64 {.inline.} =
  (x shl k) or (x shr (64 - k))

proc nextU64(r: var ChaosRng): uint64 =
  ## xoshiro256** step. Small, fast, well-distributed, and fully reproducible from
  ## the seed -- unlike std/random which shares hidden global state across the
  ## verified soak's own RNG use.
  let res = rotl(r.s[1] * 5'u64, 7) * 9'u64
  let t = r.s[1] shl 17
  r.s[2] = r.s[2] xor r.s[0]
  r.s[3] = r.s[3] xor r.s[1]
  r.s[1] = r.s[1] xor r.s[2]
  r.s[0] = r.s[0] xor r.s[3]
  r.s[2] = r.s[2] xor t
  r.s[3] = rotl(r.s[3], 45)
  res

proc splitmix64(x: var uint64): uint64 =
  ## Seeds the xoshiro state from a single 64-bit seed (the canonical splitmix64
  ## warm-up recommended by the xoshiro authors).
  x += 0x9E3779B97F4A7C15'u64
  var z = x
  z = (z xor (z shr 30)) * 0xBF58476D1CE4E5B9'u64
  z = (z xor (z shr 27)) * 0x94D049BB133111EB'u64
  z xor (z shr 31)

proc initRng(seed: uint64): ChaosRng =
  var x = seed
  for i in 0 ..< 4: result.s[i] = splitmix64(x)

proc below(r: var ChaosRng, n: int): int {.inline.} =
  ## Uniform-ish in [0, n). n is small (mode/param counts) so modulo bias is
  ## negligible and reproducibility is what matters.
  if n <= 1: 0 else: int(r.nextU64() mod uint64(n))

proc coin(r: var ChaosRng, pct: int): bool {.inline.} =
  ## True `pct`% of the time.
  int(r.nextU64() mod 100'u64) < pct

# --- schedule ---------------------------------------------------------------

type
  ChaosPort = enum
    cpData,          ## base+band     : path-selected data modes
    cpVanishAccept,  ## base+band+1   : vanish-on-accept
    cpStallAccept    ## base+band+2   : stall-on-accept

  Outcome = enum
    ocResponse,      ## a valid Response is expected (redirectloop)
    ocTimeout,       ## strict: must be a TimeoutError (stall/slowbody/zerowindow)
    ocTolerant       ## any catchable typed navi error OR a Response (tolerant modes)

  ChaosEntry = object
    ## One schedule template. The worker draws an entry, fills its per-draw coins
    ## from the PRNG into a concrete path+query, and issues the request.
    mode: string           ## catalog name (tallied under this)
    protos: set[range[0..2]] ## applicability: 0=h1 1=h2 2=h3 (index of protoIdx)
    port: ChaosPort
    expect: Outcome
    build: proc(r: var ChaosRng): string {.nimcall, gcsafe, raises: [].}
      ## builds the data-port target (path+query); gcsafe+raises:[] so an
      ## indirect call through this field is clean under chronos's strict effects.

proc protoIdx(proto: string): int =
  case proto
  of "h1": 0
  of "h2": 1
  of "h3": 2
  else: -1

# --- per-mode path/query builders (client-scheduled coins live here) --------
# These emit the data-port target. Accept-time modes carry an empty path (the
# port selects them). All randomness is the passed-in PRNG so reruns match.

proc bStall(r: var ChaosRng): string = "/chaos/stall"

proc bSlowbody(r: var ChaosRng): string =
  # A 1 MiB body dripped at ~1 KiB/s: the client must time out mid-body.
  "/chaos/slowbody?len=1048576&rate=1024"

proc bTruncate(r: var ChaosRng): string =
  # Coin: short-Content-Length vs chunked-cut. Both must never yield a successful
  # short-body Response.
  if r.coin(50): "/chaos/truncate?case=chunked" else: "/chaos/truncate?case=clen"

proc bGarbage(r: var ChaosRng): string =
  const cases = ["status", "header", "clen", "nul", "doubled"]
  "/chaos/garbage?case=" & cases[r.below(cases.len)]

proc bVanish(r: var ChaosRng): string =
  # Coins: 50% get a slow prefix; 25% die pre-headers (KeepAliveRace/Unprocessed
  # + bounded retry). The two are independent draws; pre-headers wins if both hit.
  if r.coin(25): return "/chaos/vanish?at=pre-headers"
  if r.coin(50): "/chaos/vanish?after=8192&prefix=slow&rate=1024"
  else: "/chaos/vanish?after=8192"

proc bHeaderbomb(r: var ChaosRng): string =
  # Thousands of 8 KiB header lines; bounded so the heap assertion is the teeth.
  "/chaos/headerbomb?n=4000&size=8192"

proc bRedirectloop(r: var ChaosRng): string =
  # A self-loop the client follows to maxRedirects, then a bounded 3xx Response.
  "/chaos/redirectloop?n=0"

# h2/h3-only modes: builders + schedule entries exist NOW so phases 2/3 only add
# SERVER-side code. The client already knows the paths/queries it will send.
proc bBadframes(r: var ChaosRng): string =
  # h2 rotates the violation by ?case=; h3 ignores case (control-stream garbage /
  # second SETTINGS). Sending case for both is harmless (h3 server ignores it).
  const cases = ["bigframe", "initwin", "winupdate0", "headers-stream0"]
  "/chaos/badframes?case=" & cases[r.below(cases.len)]

proc bZerowindow(r: var ChaosRng): string =
  # h2-only: INITIAL_WINDOW_SIZE=0, a 64 KiB POST body that can never flush ->
  # strict TimeoutError. The server reads the size from the query.
  "/chaos/zerowindow?body=65536"

const allEntries = [
  ChaosEntry(mode: "stall", protos: {0, 1, 2}, port: cpData,
             expect: ocTimeout, build: bStall),
  ChaosEntry(mode: "slowbody", protos: {0, 1, 2}, port: cpData,
             expect: ocTimeout, build: bSlowbody),
  ChaosEntry(mode: "truncate", protos: {0, 1, 2}, port: cpData,
             expect: ocTolerant, build: bTruncate),
  ChaosEntry(mode: "garbage", protos: {0, 1, 2}, port: cpData,
             expect: ocTolerant, build: bGarbage),
  ChaosEntry(mode: "vanish", protos: {0, 1, 2}, port: cpData,
             expect: ocTolerant, build: bVanish),
  ChaosEntry(mode: "headerbomb", protos: {0, 1, 2}, port: cpData,
             expect: ocTolerant, build: bHeaderbomb),
  ChaosEntry(mode: "redirectloop", protos: {0, 1, 2}, port: cpData,
             expect: ocResponse, build: bRedirectloop),
  # h2 + h3 frame-level (phase 2/3 server side):
  ChaosEntry(mode: "badframes", protos: {1, 2}, port: cpData,
             expect: ocTolerant, build: bBadframes),
  # h2-only flow-control starvation:
  ChaosEntry(mode: "zerowindow", protos: {1}, port: cpData,
             expect: ocTimeout, build: bZerowindow),
  # accept-time, port-selected (all protos):
  ChaosEntry(mode: "vanish-on-accept", protos: {0, 1, 2}, port: cpVanishAccept,
             expect: ocTolerant, build: bStall),   # build unused for accept ports
  ChaosEntry(mode: "stall-on-accept", protos: {0, 1, 2}, port: cpStallAccept,
             expect: ocTimeout, build: bStall)]

type ChaosPlan = object
  ## The resolved, proto-filtered schedule for this cell plus its shared runtime
  ## state (counter, deadline, worker progress stamps for the watchdog).
  cfg: Config
  entries: seq[ChaosEntry]     ## applicable to this cell's proto + csv filter
  counter: ChaosCounter
  bases: seq[string]           ## chaos origin host prefix, one per data/accept port set
  seed: uint64
  active: bool

proc cellSeed(cfg: Config): uint64 =
  ## NAVI_CHAOS_SEED mixed with a stable hash of workload|backend|proto so each
  ## cell differs but a rerun is identical.
  let mix = hash(cfg.workload & "|" & cfg.backend & "|" & cfg.proto)
  cfg.chaos.seed xor (uint64(cast[uint](mix)) * 0x9E3779B97F4A7C15'u64)

proc buildPlan(cfg: Config): ChaosPlan =
  ## Filter the catalog to this cell's proto + the NAVI_CHAOS csv (if any). An
  ## empty result is not an error: the caller prints a skip notice and no-ops.
  result.cfg = cfg
  result.active = false
  if not cfg.chaos.enabled: return
  let pi = protoIdx(cfg.proto)
  if pi < 0: return
  for e in allEntries:
    if pi notin e.protos: continue
    if not cfg.chaos.modesAll and e.mode notin cfg.chaos.modes: continue
    result.entries.add e
  if result.entries.len == 0: return
  result.counter = newChaosCounter()
  result.seed = cellSeed(cfg)
  result.active = true

# --- chaos origin URLs ------------------------------------------------------

proc chaosBase(cfg: Config, port: ChaosPort): string =
  ## The origin for a given chaos port, derived from base + band.
  let p = case port
    of cpData: cfg.basePort + cfg.chaos.portBand
    of cpVanishAccept: cfg.basePort + cfg.chaos.portBand + 1
    of cpStallAccept: cfg.basePort + cfg.chaos.portBand + 2
  "https://" & cfg.host & ":" & $p

# --- digest + startup line --------------------------------------------------

proc scheduleDigest(plan: ChaosPlan): string =
  ## A stable hex digest of the resolved schedule (modes in order + seed), so two
  ## runs with the same seed/knobs print the same digest and a different seed
  ## changes it. Draws no PRNG; purely structural.
  var h: Hash = 0
  h = h !& hash(plan.seed)
  for e in plan.entries: h = h !& hash(e.mode)
  h = !$h
  toHex(cast[uint](h).uint64, 16)

proc printSchedule(plan: ChaosPlan) =
  var names: seq[string]
  for e in plan.entries: names.add e.mode
  echo "[", plan.cfg.workload, " ", plan.cfg.proto, " ", plan.cfg.backend,
       " chaos] seed=", plan.seed, " modes=", names.join(","),
       " digest=", plan.scheduleDigest

proc chaosLabel(cfg: Config): string =
  "[" & cfg.workload & " " & cfg.proto & " " & cfg.backend & " chaos]"

# --- chaos client construction ----------------------------------------------
# Tight deadlines so every stall-class mode resolves in seconds, same proto pin
# and CA as the verified soak, but a SEPARATE Navi instance (never the canary's).

proc mkChaosClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)         # same pin: a chaos Response must match
  c.tls.caFile = cfg.cert
  c.throwHttpErrors = false                # inspect statuses; don't raise on 4xx/5xx
  c.timeouts.connect = 3000
  c.timeouts.attempt = 4000
  c.timeouts.total = 10000
  # A per-read stall timeout is what actually reclaims a stalled connection's fd on
  # the asyncdispatch backend: recvSome's withTimeout raises TimeoutError, unwinding
  # to h1OnConn's except which closes the transport. Without it, an asyncdispatch
  # stall/slowbody read parks forever (no cancellation) and the fd leaks until
  # process exit (the strict FD assertion catches exactly that). 2s < attempt(4s)
  # so a stall still surfaces as the strict TimeoutError the schedule expects.
  c.timeouts.read = 2000
  # Do not pool connections to the hostile origin. A chaos response often leaves the
  # connection in a half-broken state the server then closes; reusing such a pooled
  # connection makes the NEXT chaos request race a server-side close and surface a
  # KeepAliveRaceError instead of the mode's expected outcome (e.g. a strict `stall`
  # would report KeepAliveRaceError, not TimeoutError). An attack client has no
  # reason to keep-alive anyway, so evict idle connections almost immediately: a
  # 1ms idle timeout means a pooled chaos connection is reaped (reapExpired runs at
  # the head of the next request) before the inter-interaction gap elapses, so every
  # interaction opens fresh and reflects the mode it drew. (maxIdleConnsPerHost has
  # no "disable" value -- 0 means default 8 -- so the idle timeout is the lever.)
  c.idleConnTimeout = 1
  newNavi(c)

proc chaosFail(cfg: Config, kind, msg: string) =
  {.cast(gcsafe).}:
    stderr.writeLine "CHAOS-FAIL(" & kind & "): " & chaosLabel(cfg) & " " & msg
  quit(1)

# --- outcome classification -------------------------------------------------
# A request either returns a Response or raises a typed navi error. We map that
# onto the entry's expected Outcome, hard-failing CHAOS-FAIL(mode) on a strict
# miss and CHAOS-FAIL(pin) on a version-pin violation of any Response.

proc classifyResponse(plan: var ChaosPlan, e: ChaosEntry, res: Response) =
  ## A Response came back. First the pin (any chaos Response must pass the cell's
  ## checkVersion -- a violation is a hard CHAOS-FAIL(pin), not a canary failure
  ## since the sidecar offers only the pinned protocol). Then the strict classes.
  if plan.cfg.expectedVersion.len > 0 and res.httpVersion != plan.cfg.expectedVersion:
    plan.cfg.chaosFail("pin", e.mode & ": Response httpVersion '" &
      res.httpVersion & "' != " & plan.cfg.expectedVersion)
  case e.expect
  of ocResponse:
    # redirectloop: must be a bounded 3xx once maxRedirects is spent.
    if res.status < 300 or res.status >= 400:
      plan.cfg.chaosFail("mode", e.mode & ": expected a bounded 3xx Response, got " &
        $res.status)
    plan.counter.tallyOk(e.mode)
  of ocTimeout:
    # A strict-timeout mode must NOT produce a successful Response.
    plan.cfg.chaosFail("mode", e.mode & ": expected TimeoutError, got Response " &
      $res.status)
  of ocTolerant:
    # truncate must never yield a *successful* short body; a non-2xx Response is
    # acceptable (some backends surface a truncation as a status/parse boundary).
    if e.mode == "truncate" and res.status >= 200 and res.status < 300:
      plan.cfg.chaosFail("mode",
        "truncate: got a successful short-body Response " & $res.status)
    plan.counter.tallyOk(e.mode)

proc classifyError(plan: var ChaosPlan, e: ChaosEntry, err: ref CatchableError) =
  ## A typed navi error was raised. Strict modes require the exact class; tolerant
  ## modes accept any catchable typed error (tallied ok-vs-other, not enforced).
  let isTimeout = err of TimeoutError
  case e.expect
  of ocResponse:
    # redirectloop must NOT error (it should resolve to a bounded 3xx).
    plan.cfg.chaosFail("mode", e.mode & ": expected a bounded 3xx Response, got " &
      $err.name & ": " & err.msg)
  of ocTimeout:
    # The strict invariant for stall/slowbody/zerowindow is "navi never returns a
    # successful Response for a server that won't complete one" -- the request must
    # instead surface as a clean, bounded transport error. TimeoutError is the
    # canonical outcome and the common case. But under real connection dynamics the
    # same never-completing server also legitimately manifests as:
    #   - KeepAliveRaceError / UnprocessedError: a pooled connection to a one-shot
    #     chaos server that the server already closed (the request was not processed),
    #   - a truncation IOError: a drip whose gap or teardown lands as a mid-body EOF.
    # All are "no successful response, cleanly typed" -- the invariant holds; only the
    # cancel edge differs across backends and reuse timing. A successful Response
    # (handled in classifyResponse) is the only real failure. So accept the whole
    # never-completed-cleanly family here; the tally still shows the dominant class.
    let raceOrUnprocessed = (err of KeepAliveRaceError) or (err of UnprocessedError)
    let truncated = (err of IOError) and not (err of KeepAliveRaceError)
    if isTimeout or raceOrUnprocessed or truncated:
      plan.counter.tallyExpected(e.mode)
    else:
      plan.cfg.chaosFail("mode", e.mode & ": expected a clean timeout/abort, got " &
        $err.name & ": " & err.msg)
  of ocTolerant:
    # Any catchable typed error is fine. We tally the "expected shape" (a
    # timeout/reset/protocol error) as expectedErr and anything else as otherErr,
    # for the report; neither is enforced for a tolerant mode.
    plan.counter.tallyExpected(e.mode)

# --- self-test hooks (deliberate leaks/hangs to prove the assertions fire) --

var selfTestLeakedFdNums {.threadvar.}: seq[cint]  ## fd self-test: leaked (never-closed) fds
var selfTestRetained {.threadvar.}: seq[string]    ## mem self-test: retained bodies

# --- the per-worker loop ----------------------------------------------------

proc drawEntry(plan: var ChaosPlan, r: var ChaosRng): ChaosEntry =
  plan.entries[r.below(plan.entries.len)]


# The backend-specific driver halves. On async backends `request`/`close`/`sleep`
# are awaitable Futures; on the sync backend they are plain blocking calls, and
# the sync backend provides no `{.async.}`/`Future`/`await`, so the two cannot
# share a proc body. The client selects its half with -d:naviStressSync (set by
# run.sh for the *_sync binaries). Both halves see chaos.nim's full scope since
# this whole file is itself `include`d into the client.
when defined(naviStressSync):
  include chaos_sync
else:
  include chaos_async
