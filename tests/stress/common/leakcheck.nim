## FD + memory leak sampling for the chaos-enabled stress cells.
##
## The chaos phase's whole point is to expose teardown bugs: a hostile server
## that makes navi leak a socket, retain a body, or grow the heap unboundedly.
## We bracket the *entire process* -- baseline sampled at the top of main() before
## any Navi is constructed, final sampled after the deadline, after every client
## (verified + chaos) is closed, and after a drain sleep (5s h1/h2, 20s h3 for
## QUIC draining timers). Bracketing the whole process rather than just the chaos
## phase is simpler and stronger: leaks from either phase are caught, and the
## numbers are immune to NAVI_RECYCLE pool churn.
##
## Linux-only, like reporter.rssBytes (the harness is Dockerized, so this always
## runs in practice). On a non-Linux host fdCount returns -1 and the FD assertion
## no-ops with a notice, so a local `nim check`/run does not spuriously fail.
##
## Failures use the greppable CHAOS-FAIL(fd)/CHAOS-FAIL(mem) prefixes and quit 1,
## routing through the same hard-fail-to-FAILURES-banner path as the clients.

import std/os
when defined(posix): from std/posix import dup
import config, reporter

type LeakBaseline* = object
  ## Sampled once before any Navi exists. `active` mirrors NAVI_CHAOS != none:
  ## when chaos is off nothing is sampled and the final assertion no-ops, so an
  ## off run is byte-identical.
  active*: bool
  fd*: int          ## /proc/self/fd entries minus the dirfd, or -1 (non-Linux)
  heap*: int        ## getOccupiedMem() bytes
  rss*: int         ## rssBytes() (reused from reporter, not duplicated)

proc fdCount*(): int =
  ## Number of open file descriptors: entries in /proc/self/fd minus the dirfd
  ## that the directory read itself holds open. Linux-only; -1 elsewhere so the
  ## caller can skip the assertion with a notice rather than mis-count.
  when defined(linux):
    try:
      var n = 0
      for _ in walkDir("/proc/self/fd", relative = true):
        inc n
      return max(0, n - 1)     # subtract the fd of the opendir handle itself
    except CatchableError:
      return -1
  else:
    -1

proc sampleBaseline*(cfg: ChaosConfig): LeakBaseline =
  ## Take the baseline sample at the top of main(), before any Navi is built. A
  ## no-op returning `active=false` when chaos is off, so the caller allocates
  ## nothing and prints nothing on an off run. The FD number here is the real
  ## reference for the tight FD leak check. The heap/RSS captured here is only a
  ## fallback: `warmMemBaseline` re-bases them at chaos-start (see below).
  if not cfg.enabled: return LeakBaseline(active: false)
  LeakBaseline(active: true, fd: fdCount(), heap: getOccupiedMem(), rss: rssBytes())

proc rebaseMemPreTeardown*(base: var LeakBaseline) =
  ## Re-base the RSS reference (only) to the end-of-run working set, sampled at the
  ## start of teardown before any client is closed. The two memory metrics need
  ## different baselines because they answer different questions:
  ##
  ##  - Nim heap (getOccupiedMem): keeps its PRE-Navi baseline. The final check
  ##    runs a full GC first, so the soak's transient working set is reclaimed and
  ##    only genuinely-retained memory (a leak: a body held in a global, an
  ##    un-freed structure) remains above the baseline. GC-collectable working set
  ##    does not count against it.
  ##  - RSS (resident pages): the OS allocator almost never returns pages, so RSS
  ##    reflects the peak working set for the process lifetime and cannot drop back
  ##    to a pre-Navi baseline no matter how clean the teardown. So RSS is baselined
  ##    HERE, at the pre-teardown peak, and the final check only asserts it did not
  ##    GROW further through close+drain -- catching gross post-run growth (a
  ##    C-side OpenSSL/ngtcp2 leak that keeps allocating) without flagging the
  ##    soak's normal working set.
  if not base.active: return
  base.rss = rssBytes()

proc drainMs*(proto: string): int =
  ## Post-close drain before the final sample: h3 QUIC keeps draining timers
  ## alive after close, so it needs a longer settle than h1/h2's 5s.
  if proto == "h3": 20000 else: 5000

proc leakOneFd*(): cint =
  ## Deterministically leak one real file descriptor (dup of stdin) for the fd
  ## self-test. Returned so the caller retains it (never closed) -> /proc/self/fd
  ## climbs. Linux/POSIX only; -1 elsewhere (the self-test is Docker-only anyway).
  when defined(posix):
    dup(cint(0))
  else:
    cint(-1)

proc blockingSleep*(ms: int) =
  ## Plain blocking sleep for the sync chaos path's drain. The sync backend's
  ## `sleep` is not re-exported through `import navi`, and the async backends'
  ## `sleep` is a Future, so the sync half uses this os.sleep wrapper (leakcheck
  ## already imports std/os).
  os.sleep(ms)

proc assertNoLeak*(label: string, base: LeakBaseline, cfg: ChaosConfig) =
  ## Final leak check, after close + drain. Prints the margins on a pass so a
  ## green run documents its headroom; hard-fails (quit 1) with a CHAOS-FAIL
  ## prefix on a violation. No-op when chaos was off (base.active == false).
  if not base.active: return
  # Reclaim the soak's transient working set before the heap sample, so the Nim-heap
  # check measures only genuinely-retained memory against the pre-Navi baseline
  # (twice: the first pass can leave cycles/finalizer work the second sweeps).
  GC_fullCollect()
  GC_fullCollect()
  let fdNow = fdCount()
  let heapNow = getOccupiedMem()
  let rssNow = rssBytes()

  # FD: the tight structural check. Skipped with a notice off Linux (fdCount -1).
  if base.fd < 0 or fdNow < 0:
    echo label, " chaos: fd leak check skipped (not Linux)"
  else:
    let bound = base.fd + cfg.fdSlack
    if fdNow > bound:
      stderr.writeLine "CHAOS-FAIL(fd): baseline=" & $base.fd & " final=" &
        $fdNow & " (slack=" & $cfg.fdSlack & ", bound=" & $bound & ") " & label
      quit(1)

  # Nim heap: the tight memory check (getOccupiedMem).
  let heapSlack = cfg.heapSlackMb * 1024 * 1024
  if heapNow > base.heap + heapSlack:
    stderr.writeLine "CHAOS-FAIL(mem): heap baseline=" & fmtBytes(base.heap) &
      " final=" & fmtBytes(heapNow) & " (slack=" & $cfg.heapSlackMb & "MB) " & label
    quit(1)

  # RSS: the generous check -- allocators rarely return pages, so this exists to
  # catch gross retention (headerbomb) and C-side (OpenSSL) leaks the Nim heap
  # number would miss. 0 when unavailable (non-Linux): skip that arm.
  if base.rss > 0 and rssNow > 0:
    let rssSlack = cfg.rssSlackMb * 1024 * 1024
    if rssNow > base.rss + rssSlack:
      stderr.writeLine "CHAOS-FAIL(mem): rss baseline=" & fmtBytes(base.rss) &
        " final=" & fmtBytes(rssNow) & " (slack=" & $cfg.rssSlackMb & "MB) " & label
      quit(1)

  # Green: document the headroom so a passing run still shows the margins.
  let fdStr = if base.fd >= 0 and fdNow >= 0:
                "fd " & $base.fd & "->" & $fdNow & "/+" & $cfg.fdSlack
              else: "fd n/a"
  echo label, " chaos leak-check ok: ", fdStr,
       " | heap ", fmtBytes(base.heap), "->", fmtBytes(heapNow),
       "/+", $cfg.heapSlackMb, "MB",
       " | RSS ", (if base.rss > 0: fmtBytes(base.rss) else: "n/a"),
       "->", (if rssNow > 0: fmtBytes(rssNow) else: "n/a"),
       "/+", $cfg.rssSlackMb, "MB"
