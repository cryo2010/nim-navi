## End-to-end test of the asyncdispatch entry module.

import unittest
import std/asyncdispatch
import navi/asyncdispatch
import ./support

suite "asyncdispatch entry end to end":
  test "GET localhost returns parsed response":
    const port = 8972
    var th: Thread[ServerCtx]
    startServer(th, port)

    let api = newNavi()
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.ok
    check res.data["ok"].getBool()
    joinThread(th)

  test "retries with async backoff then succeeds":
    const port = 8992
    var th: Thread[ServerCtx]
    startRetry(th, port, failures = 1)

    let api = newNavi()
    let res = waitFor api.get("http://127.0.0.1:" & $port & "/")
    check res.status == 200
    check res.body == "recovered"
    joinThread(th)

  test "cancel aborts an in-flight request":
    const port = 8968
    var th: Thread[ServerCtx]
    startHang(th, port)  # accepts, reads the request, never replies

    let api = newNavi()
    proc run(): Future[void] {.async.} =
      let tok = newCancelToken()
      let f = api.get("http://127.0.0.1:" & $port & "/", cancel = tok)
      await sleepAsync(50)                # let the request reach the hung server
      tok.cancel()
      discard await f
    expect RequestCancelledError:
      waitFor run()
    joinThread(th)
