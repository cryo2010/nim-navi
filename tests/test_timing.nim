## The shared connection-timeout arithmetic in backend/timing: the establishment
## precedence rule, the remaining-deadline math, the per-syscall socket timeout a
## bounded read arms after its readiness wait (issue #442), and the canonical
## timeout wordings. Pure arithmetic, so it runs on every platform with no sockets
## and no OpenSSL; the wall-clock behaviour it feeds is covered by
## tests/interop/tls_budget.sh and the #452 case in test_socks.nim.
import unittest
import std/[monotimes, times, os]
import navi/backend/timing

suite "the establishment budget precedence (timeouts.connect > total > default)":
  test "an explicit connect limit wins over everything else":
    check establishMs(1500, 9000, 30000) == 1500

  test "a total deadline caps establishment when no connect limit is set":
    check establishMs(0, 9000, 30000) == 9000

  test "a backend default applies only when neither is set":
    check establishMs(0, 0, 30000) == 30000
    check establishMs(0, 0) == 0          # no floor: unbounded

  test "non-positive values never leak through as a budget":
    check establishMs(-1, 0) == 0
    check establishMs(0, -5) == 0

suite "remainingMs measures what is left of a deadline":
  test "a future deadline reports a positive remainder no larger than the budget":
    let left = remainingMs(getMonoTime() + initDuration(milliseconds = 500))
    check left > 0
    check left <= 500

  test "a lapsed deadline reports nothing left":
    check remainingMs(getMonoTime() - initDuration(milliseconds = 50)) <= 0

  test "the remainder shrinks as the deadline is approached":
    let deadline = getMonoTime() + initDuration(milliseconds = 400)
    let first = remainingMs(deadline)
    sleep(120)
    let second = remainingMs(deadline)
    check second < first

suite "recvTimeoutMs arms one syscall with the REST of the budget (#442)":
  test "it returns what is left, not the budget the wait started with":
    # This is the defect: SO_RCVTIMEO used to be armed with the pre-wait budget, so
    # a readiness wait that returned late bought the recv inside SSL_read a second
    # full window and one read could stall for ~2x timeouts.read.
    let budgetMs = 400
    let deadline = getMonoTime() + initDuration(milliseconds = budgetMs)
    sleep(150)                              # as if the readiness wait had spent this
    let arm = recvTimeoutMs(deadline)
    check arm > 0
    check arm < budgetMs
    check arm <= budgetMs - 100             # the spent time really is deducted

  test "a lapsed deadline arms 1 ms, never 0 (which means block forever)":
    # A socket timeout of 0 is "no timeout" on both POSIX and Winsock, so an
    # exhausted budget must never be handed to setsockopt as 0.
    check recvTimeoutMs(getMonoTime() - initDuration(milliseconds = 10)) == 1
    check recvTimeoutMs(getMonoTime()) == 1

  test "it is never 0 for any deadline":
    for ms in [-1000, -1, 0, 1, 50, 1000]:
      check recvTimeoutMs(getMonoTime() + initDuration(milliseconds = ms)) >= 1

suite "the canonical timeout wordings are built in one place":
  test "the connect wording names the budget":
    check connectTimeoutMsg(300) == "navi: connect timed out after 300 ms"

  test "the read wording names the budget":
    check readTimeoutMsg(5000) == "navi: read timed out after 5000 ms"
