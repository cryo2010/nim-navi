## Writes racing a connection close on the asyncdispatch backend (issue #421).
##
## `freeConn` closes the fd (and SSL_frees the session) without clearing the `fd`
## field of the Conn VALUE copies the stream layers hold, so a send that reaches the
## transport after a close used to write through a descriptor number the process may
## already have reused -- and, on TLS, through a freed SSL. The shared `state` flag is
## the authority, and both `sendAll` paths now consult it.
##
## Plain TCP can only express the pre-write half of the guard: once a write is parked
## inside the stdlib's `send` there is no navi loop left to re-check the flag. The
## retry-loop half (a parked `sslWrite` woken by `closeSocket` AFTER `SSL_free`) needs
## a real TLS server and lives in tests/interop/tls_write_close.sh.
import unittest
import std/[asyncdispatch, net, strutils]
from std/os import sleep
import navi/backend/asyncdispatch as be
import navi/backend/api            # TlsConfig / ProxyTarget

type DeafCtx = object
  portOut: ptr int
  ready: ptr bool
  stop: ptr bool

proc serveDeaf(ctx: DeafCtx) {.thread.} =
  ## Accept one connection and never read a byte from it, so the client's socket
  ## buffers fill and a large write parks mid-send. Blocking sockets on their own
  ## thread, so the peer never shares navi's event loop.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")     # ephemeral: no cross-run collision
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var client: Socket
  server.accept(client)
  while not ctx.stop[]: os.sleep(5)
  client.close()
  server.close()

proc startDeaf(th: var Thread[DeafCtx], port: var int, stop: ptr bool) =
  var ready = false
  createThread(th, serveDeaf, DeafCtx(portOut: addr port, ready: addr ready, stop: stop))
  while not ready: os.sleep(1)

suite "asyncdispatch writes racing a close":
  test "sendAll on a closed connection should raise IOError instead of writing to the fd":
    var port = 0
    var stop = false
    var th: Thread[DeafCtx]
    startDeaf(th, port, addr stop)

    var msg = ""
    var wrongType = false
    proc run() {.async.} =
      let conn = await be.connect("127.0.0.1", port, false, TlsConfig(), ProxyTarget())
      await be.close(conn)
      # `conn` is a value copy: its `fd` still holds the now-closed descriptor
      # number, so only the shared state flag can tell this send it is dead.
      try:
        await be.sendAll(conn, "GET / HTTP/1.1\r\n\r\n")
      except IOError as e:
        msg = e.msg
      except CatchableError as e:
        wrongType = true
        msg = e.msg
    waitFor run()

    check not wrongType          # pre-fix this was the dispatcher's ValueError
    check "closed" in msg
    stop = true
    joinThread(th)

  # POSIX only: on Windows an overlapped loopback send accepts the whole buffer
  # (the kernel copies it, a deaf peer notwithstanding), so the write cannot be
  # parked deterministically and the precondition below never holds. The
  # post-close guard above covers that platform; the retry-loop half needs TLS
  # anyway (tests/interop/tls_write_close.sh).
  when not defined(windows):
    test "a write parked on a full socket buffer should fail cleanly when the conn closes":
      var port = 0
      var stop = false
      var th: Thread[DeafCtx]
      startDeaf(th, port, addr stop)

      var parked = false
      var parkedSettled = false
      var afterMsg = ""
      var afterWrongType = false
      proc run() {.async.} =
        let conn = await be.connect("127.0.0.1", port, false, TlsConfig(), ProxyTarget())
        # Far more than any socket buffer pair, against a peer that never reads: the
        # send is still in flight when the close lands.
        let big = newString(16 * 1024 * 1024)
        let sendFut = be.sendAll(conn, big)
        await sleepAsync(200)
        parked = not sendFut.finished
        await be.close(conn)                        # the reader side tears it down
        # `withTimeout` re-raises the awaited future's failure, so a parked write
        # broken by the close surfaces as an exception here rather than as a result.
        try:
          parkedSettled = await withTimeout(sendFut, 2000)
        except CatchableError:
          parkedSettled = true
        if sendFut.finished and sendFut.failed:
          discard sendFut.error                     # observed: no orphaned stack trace
        # A second write on the same (already freed) conn must be refused outright.
        try:
          await be.sendAll(conn, "trailing")
        except IOError as e:
          afterMsg = e.msg
        except CatchableError as e:
          afterWrongType = true
          afterMsg = e.msg
      waitFor run()

      check parked                 # the precondition: the write really was in flight
      check parkedSettled          # the close woke it rather than stranding it
      check not afterWrongType
      check "closed" in afterMsg
      stop = true
      joinThread(th)
