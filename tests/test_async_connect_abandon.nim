## A timed-out asyncdispatch connect must STOP the abandoned `establish` (issue #443).
##
## asyncdispatch has no cancellation, so the `establish` future a `connectMs` deadline
## gives up on keeps running. Before the fix it caught the error its own shutdown
## caused, dropped that address, and walked the rest of the Happy-Eyeballs pool with a
## fresh TCP race plus a fresh handshake each -- against an origin whose caller had
## already received TimeoutError and moved on.
##
## Reproduced here without TLS (real OpenSSL cannot run on every host that runs this
## suite): the "handshake" that stalls is the SOCKS5 greeting, which `establish` awaits
## on the winning socket exactly where a TLS handshake would be, and whose failure lands
## in the same `except CatchableError` that used to re-race the pool. The address pool is
## `localhost`, which resolves to both loopback families, so a listener per family gives
## the pool the two reachable addresses the re-race needs. The TLS variant (a python
## server that accepts TCP and never finishes the handshake) is exercised on Linux, where
## navi can actually load libcrypto.
import unittest
import std/[asyncdispatch, net, nativesockets, strutils]
import navi/backend/asyncdispatch as be
import navi/backend/api                 # TlsConfig / ProxyTarget
import navi/backend/happyeyeballs       # resolveAddrs: the pool `connect` will race
import navi/core/response as naviresp   # navi's TimeoutError (std/net defines one too)

type StallCtx = object
  fd: SocketHandle
  domain: Domain
  accepts: ptr int
  sawEof: ptr bool
  stop: ptr bool

proc serveStall(ctx: StallCtx) {.thread.} =
  ## Accept every connection, count it, and never answer the SOCKS5 greeting, so the
  ## client parks inside `socksConnect`. After each accept, read until the peer hangs
  ## up: that is how the test sees the abandoned attempt woken and reclaimed rather
  ## than pinned for the rest of the process. Blocking sockets on their own thread, so
  ## the peer never shares navi's event loop.
  var server = newSocket(ctx.fd, ctx.domain, SOCK_STREAM, IPPROTO_TCP)
  var held: seq[Socket] = @[]
  while true:
    var client: Socket
    try:
      server.accept(client)
    except CatchableError:
      break
    if ctx.stop[]:                 # the teardown poke that unblocks this accept
      (try: client.close() except CatchableError: discard)
      break
    inc ctx.accepts[]
    var buf = newString(4096)
    for _ in 0 ..< 8:              # ~2s: the greeting arrives, then wait for the FIN
      var n = -1
      try:
        n = client.recv(buf, buf.len, timeout = 250)
      except CatchableError:
        n = -1                     # nothing readable yet; keep waiting
      if n == 0:
        ctx.sawEof[] = true
        break
    held.add client
  for c in held:
    (try: c.close() except CatchableError: discard)
  (try: server.close() except CatchableError: discard)

proc pickPort(): int =
  ## An ephemeral port, released again so it can be bound on both loopback families.
  var s = newSocket()
  s.setSockOpt(OptReuseAddr, true)
  s.bindAddr(Port(0), "127.0.0.1")
  result = s.getLocalAddr()[1].int
  s.close()

proc listenOn(domain: Domain, address: string, port: int): Socket =
  var s = newSocket(domain, SOCK_STREAM, IPPROTO_TCP)
  s.setSockOpt(OptReuseAddr, true)
  s.bindAddr(Port(port), address)
  s.listen()
  s

proc poke(domain: Domain, address: string, port: int) =
  ## One throwaway connection, to unblock a listener thread parked in `accept`.
  try:
    var s = newSocket(domain, SOCK_STREAM, IPPROTO_TCP)
    s.connect(address, Port(port))
    s.close()
  except CatchableError:
    discard

suite "asyncdispatch abandons a timed-out connect":
  test "a connect that times out mid-handshake must not race the remaining addresses":
    var port = 0
    var lv4, lv6: Socket = nil
    # The pool needs two reachable addresses on ONE port, i.e. the same port on both
    # loopback families. An ephemeral port can be taken between the probe and the
    # binds, so retry a few times before settling for v4 only.
    for _ in 0 ..< 16:
      port = pickPort()
      try:
        lv4 = listenOn(Domain.AF_INET, "127.0.0.1", port)
        lv6 = listenOn(Domain.AF_INET6, "::1", port)
        break
      except CatchableError:
        if not lv4.isNil: (try: lv4.close() except CatchableError: discard)
        lv4 = nil
        lv6 = nil
    if lv4.isNil:
      port = pickPort()
      lv4 = listenOn(Domain.AF_INET, "127.0.0.1", port)

    var acceptsV4, acceptsV6 = 0
    var eofV4, eofV6 = false
    var stop = false
    var thV4, thV6: Thread[StallCtx]
    createThread(thV4, serveStall, StallCtx(fd: lv4.getFd(), domain: Domain.AF_INET,
      accepts: addr acceptsV4, sawEof: addr eofV4, stop: addr stop))
    if not lv6.isNil:
      createThread(thV6, serveStall, StallCtx(fd: lv6.getFd(), domain: Domain.AF_INET6,
        accepts: addr acceptsV6, sawEof: addr eofV6, stop: addr stop))

    # What `establish` will actually race. Both entries are our stalling listeners.
    let pool = resolveAddrs("localhost", port)
    echo "  address pool for localhost:", port, " = ", pool
    let dualHomed = pool.len >= 2
    if not dualHomed:
      # One address means there is nothing to re-race, so the accept count below
      # would be 1 on the UNFIXED code too. Report that as skipped rather than as a
      # pass. skip() only sets the status, so the rest of the case still runs and a
      # later `check` failure still overrides it with a failure.
      echo "  note: localhost is single-homed here, so the pool re-race cannot be shown"
      skip()

    var timedOut = false
    var established = false
    var msg = ""
    var otherErr = ""
    proc run() {.async.} =
      # A SOCKS5 proxy tunnels plain http too, so `establish` awaits the greeting
      # reply on the winning socket before it would hand the conn back: the plaintext
      # stand-in for a stalled TLS handshake. The target host is never resolved (the
      # proxy would do that), so it can be anything.
      let tgt = ProxyTarget(kind: pkSocks5, host: "localhost", port: port)
      try:
        let conn = await be.connect("navi.invalid", 443, false, TlsConfig(), tgt,
                                    connectMs = 300)
        established = true
        await be.close(conn)
      except naviresp.TimeoutError as e:
        msg = e.msg
        timedOut = true
      except CatchableError as e:
        otherErr = e.msg
      # Keep the dispatcher turning: the abandoned `establish` only makes progress
      # while the loop is polled, and before the fix this is the window in which it
      # re-raced the remaining address. A blocking sleep here would hide the defect.
      await sleepAsync(1500)
    waitFor run()

    let attempted = acceptsV4 + acceptsV6
    check not established              # the stalled greeting cannot complete
    check otherErr.len == 0
    check timedOut
    check "connect timed out" in msg
    check (eofV4 or eofV6)             # the in-flight attempt was woken, not pinned
    # The heart of #443: exactly ONE connection ever reached the listeners. Before the
    # fix the abandoned establish opened a second one (the re-race) after the caller
    # had already raised TimeoutError. Only meaningful with a two-address pool: see
    # the skip() above.
    if dualHomed:
      check attempted == 1

    stop = true
    poke(Domain.AF_INET, "127.0.0.1", port)
    joinThread(thV4)
    if not lv6.isNil:
      poke(Domain.AF_INET6, "::1", port)
      joinThread(thV6)
