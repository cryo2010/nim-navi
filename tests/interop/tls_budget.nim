## The sync backend's establishment and read budgets against a real TLS peer
## (issue #442). Driven by tests/interop/tls_budget.sh, which stands up three
## python TLS servers (stall_tls_server.py) and exports their ports plus the CA:
##
## * NAVI_BUDGET_DEAF   accepts the TCP connection and never answers the
##   ClientHello, so the handshake can only be stopped by navi's own budget. That
##   expiry has to reach the caller as navi's `TimeoutError` with the connect
##   wording: `Timeouts.connect` documents "TCP connect + TLS handshake", and the
##   readiness wait used to report it as ValueError("TLS handshake timed out"),
##   which also made `connectAcross` drop the address and re-race the pool.
## * NAVI_BUDGET_PARTIAL completes the handshake, reads the request and then, late
##   in the client's read budget, writes a bare TLS record header (no payload, and
##   no session ticket before it). The readiness wait fires on bytes that carry no
##   application data, so SSL_read consumes them and recv's again: arming
##   SO_RCVTIMEO with the pre-wait budget bought that recv a second full window and
##   doubled the stall, overshooting `timeouts.read` and `timeouts.total` by up to
##   one whole budget.
## * NAVI_BUDGET_GOOD   answers normally: the control case for both handshake
##   paths, bounded (poll-driven) and unbounded (plain blocking SSL_connect).
import std/[os, strutils, monotimes, times]
import navi

let ca = getEnv("NAVI_BUDGET_CA")
let deafUrl = "https://127.0.0.1:" & getEnv("NAVI_BUDGET_DEAF") & "/"
let partialUrl = "https://127.0.0.1:" & getEnv("NAVI_BUDGET_PARTIAL") & "/"
let goodUrl = "https://127.0.0.1:" & getEnv("NAVI_BUDGET_GOOD") & "/"
let stallMs = parseInt(getEnv("NAVI_BUDGET_STALL"))   # when the partial record lands

proc base(): NaviConfig =
  result = initNaviConfig()
  result.tls.caFile = ca            # trust the throwaway test cert
  result.retry.limit = 0            # one attempt: every case measures one budget
  result.throwHttpErrors = false

type Outcome = object
  kind: string      # "timeout", "other:<Name>" or "ok"
  msg: string
  ms: int

proc attempt(cfg: NaviConfig, url: string): Outcome =
  let api = newNavi(cfg)
  let t0 = getMonoTime()
  try:
    let res = api.get(url)
    result = Outcome(kind: "ok", msg: $res.status)
  except TimeoutError as e:
    result = Outcome(kind: "timeout", msg: e.msg)
  except CatchableError as e:
    result = Outcome(kind: "other:" & $e.name, msg: e.msg)
  result.ms = (getMonoTime() - t0).inMilliseconds.int
  api.close()

var failures = 0
proc want(cond: bool, what: string) =
  if cond: echo "  OK   ", what
  else:
    echo "  FAIL ", what
    inc failures

# --- the establishment budget bounds the TLS handshake ---------------------
block:
  var cfg = base()
  cfg.timeouts.connect = 300
  let o = attempt(cfg, deafUrl)
  echo "deaf peer, timeouts.connect=300 -> ", o.kind, " (", o.ms, " ms): ", o.msg
  want(o.kind == "timeout", "a handshake that outlives timeouts.connect raises TimeoutError")
  # The wording, not the exact figure: `connect` opens the budget and hands
  # `connectAcross` what is LEFT of it, so the phase names its own remainder (299
  # ms here). What matters is that this is navi's connect timeout rather than the
  # ValueError("TLS handshake timed out for ...") the readiness wait used to report.
  want("connect timed out after" in o.msg, "it carries the connect-timeout wording")
  want(o.ms < 2000, "and it fires at the budget, not per address or per syscall")

block:
  # No explicit connect limit: the total deadline is what caps establishment.
  var cfg = base()
  cfg.timeouts.total = 400
  let o = attempt(cfg, deafUrl)
  echo "deaf peer, timeouts.total=400 -> ", o.kind, " (", o.ms, " ms): ", o.msg
  want(o.kind == "timeout", "a total deadline also bounds the handshake as a TimeoutError")
  want(o.ms < 2000, "at the deadline")

# --- one read cannot overshoot the read budget ----------------------------
block:
  var cfg = base()
  cfg.timeouts.read = 2000
  let o = attempt(cfg, partialUrl)
  echo "partial record at ", stallMs, " ms, timeouts.read=2000 -> ", o.kind,
       " (", o.ms, " ms): ", o.msg
  want(o.kind == "timeout", "a stalled read raises TimeoutError")
  want("read timed out" in o.msg, "with the read-timeout wording")
  want(o.ms >= 1500, "the budget really was spent waiting")
  want(o.ms < 2900,
       "and the read did not buy a second full budget after the late record")

block:
  var cfg = base()
  cfg.timeouts.total = 2000
  let o = attempt(cfg, partialUrl)
  echo "partial record at ", stallMs, " ms, timeouts.total=2000 -> ", o.kind,
       " (", o.ms, " ms): ", o.msg
  want(o.kind == "timeout", "the total deadline is reported as TimeoutError")
  want(o.ms < 2900, "and is not overshot by one read budget")

# --- the healthy peer, on both handshake paths ----------------------------
block:
  # `timeouts.read` alone leaves the establishment budget unset (only connect and
  # total feed it), so this is the unbounded, plain blocking SSL_connect path --
  # with the reads still bounded, so a regression here cannot hang the suite.
  var cfg = base()
  cfg.timeouts.read = 5000
  let o = attempt(cfg, goodUrl)
  echo "good peer, no establishment budget -> ", o.kind, " (", o.ms, " ms): ", o.msg
  want(o.kind == "ok" and o.msg == "200", "an unbounded handshake still connects")

block:
  # A budget set: the poll-driven non-blocking handshake path.
  var cfg = base()
  cfg.timeouts.connect = 5000
  cfg.timeouts.read = 5000
  cfg.timeouts.total = 10000
  let o = attempt(cfg, goodUrl)
  echo "good peer, connect=5000 read=5000 total=10000 -> ", o.kind,
       " (", o.ms, " ms): ", o.msg
  want(o.kind == "ok" and o.msg == "200", "a bounded handshake still connects")
  want(o.ms < 2000, "promptly")

if failures > 0:
  quit("== sync establishment/read budget: " & $failures & " failure(s) ==", 1)
echo "== sync establishment + read budgets are one wall clock each OK =="
