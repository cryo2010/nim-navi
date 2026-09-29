## A TLS write parked on WANT_WRITE must observe a concurrent close instead of
## calling SSL_write on the freed SSL (issue #421). Driven by tls_write_close.sh
## against a TLS server that finishes the handshake and then never reads.
##
## The unit suite can only cover the pre-write half of the guard (plain TCP has no
## navi-side retry loop once the stdlib's `send` parks); this is the retry-loop half,
## so it needs a real TLS peer and lives here.
import std/[asyncdispatch, os, strutils]
import navi/backend/asyncdispatch as be
import navi/backend/api            # TlsConfig / ProxyTarget

let port = parseInt(getEnv("NAVI_TWC_PORT"))

proc main() {.async.} =
  # No verification (the server is self-signed) and an UNSHARED SSL_CTX (no
  # contextStore), so the close also exercises the owned-context teardown that
  # frees the SSL under the parked write.
  let conn = await be.connect("127.0.0.1", port, true,
                              TlsConfig(insecureSkipVerify: true), ProxyTarget())
  # Far more than any socket buffer pair, against a peer that never reads: SSL_write
  # gets a short write and the loop parks on waitWrite.
  let big = newString(32 * 1024 * 1024)
  let sendFut = be.sendAll(conn, big)
  await sleepAsync(500)
  doAssert not sendFut.finished, "the TLS write should still be parked on WANT_WRITE"

  # The reader side tears the connection down: csClosed, SHUT_RDWR, one tick, then
  # SSL_free + closeSocket. On Linux the shutdown alone does not report EPOLLOUT, so
  # the parked write is woken by closeSocket -- i.e. AFTER SSL_free.
  await be.close(conn)

  # asyncdispatch's `withTimeout` re-raises the awaited future's failure, so the
  # parked write's IOError surfaces here rather than in `sendFut.error`.
  var msg = ""
  var woken = false
  try:
    woken = await withTimeout(sendFut, 5000)
  except CatchableError as e:
    woken = true
    msg = e.msg
  doAssert woken, "the parked TLS write was never woken by the close"
  if msg.len == 0 and sendFut.failed: msg = sendFut.error.msg
  # asyncdispatch appends its injected traceback to the message, so match the text.
  doAssert "navi: connection closed" in msg,
    "the parked sslWrite must see the teardown flag and raise, got: " & msg
  echo "OK  parked TLS write raised \"navi: connection closed\" instead of using the freed SSL"

  # And a fresh write on the same (already freed) conn value is refused outright.
  var lateMsg = ""
  try:
    await be.sendAll(conn, "trailing")
  except IOError as e:
    lateMsg = e.msg
  doAssert "navi: connection closed" in lateMsg,
    "a send after close must raise IOError, got: " & lateMsg
  echo "OK  send after close raised IOError \"navi: connection closed\""

waitFor main()
echo "== TLS write racing close (asyncdispatch) passed =="
