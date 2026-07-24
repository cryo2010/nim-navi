## End-to-end test of the chronos entry module.

import unittest
import pkg/chronos
import navi/chronos
import ./support

suite "chronos entry end to end":
  test "GET localhost returns parsed response":
    const port = 8973
    var th: Thread[ServerCtx]
    startServer(th, port)

    let api = newNavi()
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.ok
    check res.data["ok"].getBool()
    joinThread(th)

  test "cancel aborts an in-flight request":
    const port = 8969
    var th: Thread[ServerCtx]
    startHang(th, port)  # accepts, reads the request, never replies

    proc run(api: Navi, url: string): Future[void] {.async.} =
      let tok = newCancelToken()
      let f = api.get(url, cancel = tok)
      await sleepAsync(50.milliseconds)  # let the request reach the hung server
      tok.cancel()
      discard await f
    expect RequestCancelledError:
      waitFor run(newNavi(), "http://127.0.0.1:" & $port & "/")
    joinThread(th)
