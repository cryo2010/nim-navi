## Cancellation through the chronos TLS pump (#430). Driven by
## tests/interop/tls_stall.sh, which runs a TLS server that handshakes, reads the
## request and then never answers, and exports NAVI_STALL_URL / NAVI_STALL_CA /
## NAVI_STALL_LOG (one line per accepted connection).
##
## `feedIn` used to catch CancelledError along with everything else and report it
## as a clean EOF, so a cancellation that landed while the pump was parked in
## `readOnce` never reached the caller: a read timeout came back as "" (the engine
## then raised KeepAliveRaceError and the retry layer REPLAYED the request on a
## fresh connection) and a total deadline was only honoured once that replay had
## run its course. Both cases are asserted here, the replay via the server's
## accept log.
import std/[os, strutils, times]
import pkg/chronos
import navi/chronos

proc accepts(): int =
  ## Connections the stall server has taken so far. Counts non-empty lines: an
  ## empty log must read as 0, and `splitLines` on "" yields one empty line.
  try:
    for line in readFile(getEnv("NAVI_STALL_LOG")).splitLines():
      if line.len > 0: inc result
  except CatchableError: discard

proc failure(cfg: NaviConfig, url: string): Future[string] {.async.} =
  let api = newNavi(cfg)
  try:
    discard await api.get(url)
    return ""                     # the server never answers: this cannot happen
  except CatchableError as e:
    return e.msg
  finally:
    await api.close()

proc base(): NaviConfig =
  result = initNaviConfig()
  result.tls.caFile = getEnv("NAVI_STALL_CA")   # read here: GC-safe under chronos
  result.throwHttpErrors = false

proc main() {.async.} =
  let url = getEnv("NAVI_STALL_URL") & "/"

  # A per-read stall timeout must surface as navi's read TimeoutError. Before the
  # fix the cancelled `readOnce` was reported as EOF, `withTimeout` saw the read
  # future COMPLETE (with ""), and the engine turned the EOF-before-headers into a
  # KeepAliveRaceError.
  block:
    var cfg = base()
    cfg.timeouts.read = 800
    cfg.retry.limit = 0
    let before = accepts()
    let t0 = epochTime()
    let msg = await failure(cfg, url)
    let took = (epochTime() - t0) * 1000
    doAssert "read timed out" in msg, "read timeout: got " & msg
    doAssert took < 5000, "read timeout took " & $took.int & " ms"
    doAssert accepts() == before + 1, "read timeout opened more than one connection"

  # A total-request deadline, with retries left enabled: the cancel must stop the
  # request where it is. Before the fix it was swallowed in the pump, the engine
  # saw a keep-alive race and replayed the GET on a fresh connection, so the server
  # took a second connection and the deadline was honoured only much later.
  block:
    var cfg = base()
    cfg.timeouts.total = 800
    let before = accepts()
    let t0 = epochTime()
    let msg = await failure(cfg, url)
    let took = (epochTime() - t0) * 1000
    doAssert "timed out" in msg, "total timeout: got " & msg
    doAssert took < 5000, "total timeout took " & $took.int & " ms"
    doAssert accepts() == before + 1,
      "total timeout replayed the request: " & $(accepts() - before) & " connections"

  echo "== chronos: TLS read/total timeouts are not swallowed by the pump OK =="

waitFor main()
