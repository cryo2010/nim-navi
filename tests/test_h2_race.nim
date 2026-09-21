## HTTP/2 keep-alive race classification. The mux distinguishes three terminal cases
## when a shared connection dies without a full response:
##   * request was WRITTEN, then the connection dropped before any response HEADERS ->
##     `KeepAliveRaceError` (ambiguous: the retry layer replays it only for idempotent
##     methods or with an Idempotency-Key, matching Go net/http / RFC 9110 9.2.2).
##   * mux found dead BEFORE the request was sent -> `UnprocessedError` (provably not
##     processed: safe to retry any method).
##   * a drop AFTER the response HEADERS began -> plain `IOError` (the peer processed
##     it: not safely replayable).
##
## The peer runs on its own thread with blocking sockets (see support_h2race),
## keeping it off navi's single event loop.
import unittest
import std/asyncdispatch
import navi/backend/asyncdispatch as be
import navi/backend/h2mux
import navi/backend/api            # TlsConfig / ProxyTarget
import navi/core/response          # KeepAliveRaceError
import ./support_h2race

proc connectMux(port: int): Future[H2Mux] {.async.} =
  var lastErr: ref CatchableError
  for _ in 0 ..< 100:
    try:
      let conn = await be.connect("127.0.0.1", port, false, TlsConfig(), ProxyTarget())
      return await newH2Mux(conn)
    except CatchableError as e:
      lastErr = e
      await sleepAsync(20)
  raise lastErr

proc post(mux: H2Mux): Future[ref CatchableError] {.async.} =
  ## Send a non-idempotent POST and return the exception it fails with (nil if it
  ## unexpectedly succeeded), so the test can assert on the exact type.
  let headers = @[(":method", "POST"), (":scheme", "http"),
                  (":authority", "127.0.0.1"), (":path", "/echo")]
  try:
    discard await mux.request(headers, "hello")
    result = nil
  except CatchableError as e:
    result = e

var beforeThread, afterThread, preThread: Thread[RacePeerArg]

suite "http/2 keep-alive race":
  test "a drop before response headers surfaces as KeepAliveRaceError":
    startRacePeer(beforeThread, 9340, pmCloseBeforeHeaders)
    proc run() {.async.} =
      let mux = await connectMux(9340)
      let fut = post(mux)
      check await withTimeout(fut, 5000)   # completed within the timeout (did not hang)
      let e = fut.read
      check e != nil
      check e of KeepAliveRaceError
      await mux.close()
    waitFor run()
    joinThread(beforeThread)

  test "a drop after response headers stays a plain IOError (not a race)":
    startRacePeer(afterThread, 9341, pmCloseAfterHeaders)
    proc run() {.async.} =
      let mux = await connectMux(9341)
      let fut = post(mux)
      check await withTimeout(fut, 5000)
      let e = fut.read
      check e != nil
      check e of IOError
      check not (e of KeepAliveRaceError)
      await mux.close()
    waitFor run()
    joinThread(afterThread)

  test "a request on a mux that died before it was sent is UnprocessedError":
    startRacePeer(preThread, 9342, pmCloseBeforeHeaders)
    proc run() {.async.} =
      let mux = await connectMux(9342)
      await mux.close()                    # force the mux dead BEFORE any request is sent
      let fut = post(mux)
      check await withTimeout(fut, 5000)
      let e = fut.read
      check e != nil
      check e of UnprocessedError          # provably not sent -> retryable for any method
      check not (e of KeepAliveRaceError)  # not the ambiguous written-but-no-response case
    waitFor run()
    joinThread(preThread)
