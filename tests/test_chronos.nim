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
    check res.json()["ok"].getBool()
    joinThread(th)
