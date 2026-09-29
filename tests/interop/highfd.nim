## Readiness waits on descriptors above FD_SETSIZE (issue #429), sync backend.
##
## Driven by tests/interop/highfd.sh, which stands up an OpenSSL HTTPS server and
## exports NAVI_HIGHFD_URL / NAVI_HIGHFD_CA. The test burns descriptors with
## dup(2) until the next one handed out is above 1024, then runs a request with a
## read timeout armed -- the only case that reaches the readiness wait at all
## (without one the backend skips it and blocks in recv). Before the fix that
## wait was select(2), which FD_SETs the raw descriptor into a 1024-bit fd_set:
## the process aborted on a fortified build, or select returned -1 and the caller
## read that as an expiry and raised a bogus "read timed out".
##
## POSIX only: Windows fd_sets are counted arrays, so they never had the ceiling.
import unittest
import std/[os, posix]
import navi

let
  base = getEnv("NAVI_HIGHFD_URL")   # https://127.0.0.1:port
  ca = getEnv("NAVI_HIGHFD_CA")

var burned: seq[cint]

proc liftNoFile(target: int): int =
  ## Raise the soft descriptor limit toward `target` (never past the hard limit)
  ## and report the limit we ended up with.
  var lim: RLimit
  if getrlimit(RLIMIT_NOFILE, lim) != 0: return 0
  let want = min(target, lim.rlim_max)
  if lim.rlim_cur < want:
    var next = lim
    next.rlim_cur = want
    discard setrlimit(RLIMIT_NOFILE, next)
    if getrlimit(RLIMIT_NOFILE, lim) != 0: return 0
  lim.rlim_cur

proc burnFds(upTo: int) =
  ## Hold descriptors open until the next one the kernel hands out is above
  ## `upTo`, so navi's socket lands there. dup(2) of /dev/null is enough: fds are
  ## allocated lowest-free-first, so this simply walks the table up.
  let devnull = posix.open("/dev/null", O_RDONLY)
  doAssert devnull >= 0, "cannot open /dev/null"
  burned.add devnull
  while burned[^1] <= upTo:
    let fd = posix.dup(devnull)
    if fd < 0: break                   # limit reached; the check below reports it
    burned.add fd

proc releaseFds() =
  for fd in burned: discard posix.close(fd)
  burned.setLen(0)

proc statusOverHighFd(url, caFile: string): int =
  var cfg = initNaviConfig()
  cfg.tls.caFile = caFile
  cfg.timeouts.connect = 10_000
  cfg.timeouts.read = 10_000        # arms the readiness wait: 0 would skip it
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  result = api.get(url).status
  api.close()

suite "sync readiness waits above FD_SETSIZE":
  test "a request with a read timeout succeeds on a descriptor above 1024":
    check liftNoFile(4096) > 1200     # the harness must leave room above 1024
    burnFds(1100)
    try:
      # What the kernel would hand navi next, confirming the fds really are high.
      let probe = posix.socket(AF_INET, SOCK_STREAM, 0)
      check probe.cint > 1024
      discard posix.close(probe.cint)
      check statusOverHighFd(base & "/", ca) == 200   # s_server -www answers 200
    finally:
      releaseFds()
