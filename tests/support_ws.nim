## Shared WebSocket test servers, used by test_ws / test_ws_async / test_ws_chronos.
## Kept out of support.nim so that importing support.nim does not drag in
## navi/proto/ws (and its checksums/sha1 dependency) for tests that only need an
## HTTP server -- e.g. the codec leak check, which compiles with a bare `nim c`
## that has no nimble dependency paths. Filename has no leading 't' so the runner
## does not treat it as a suite.

import std/[net, strutils, os]
import navi/proto/ws   # sans-io WS core (handshake + frame codec)

# --- WebSocket test servers (shared by test_ws / test_ws_async / test_ws_chronos) --
# One in-process server per behavior, built from navi's sans-io WS core (server
# frames unmasked). Each binds an ephemeral loopback port and reports it via a
# `var int`, so concurrent test files never collide on a fixed port. Living here
# (not copy-pasted per backend file) means a fix reaches every backend at once.
#
# WS servers use `buffered = false` sockets and `server.accept` (not `acceptClient`,
# whose Windows path returns a buffered socket): interactive small frames must not
# wait for a full buffer, and IPv4 loopback avoids the IPv6-accept bug acceptClient
# works around.

type WsSrv* = object
  ready: ptr bool
  portOut: ptr int
  sawEof: ptr bool     ## optional: set to whether the client tore the transport down

proc wsBind(ctx: WsSrv): Socket =
  ## Ephemeral loopback listener; report the bound port, then mark ready.
  result = newSocket(buffered = false)
  result.setSockOpt(OptReuseAddr, true)
  result.bindAddr(Port(0), "127.0.0.1")
  result.listen()
  ctx.portOut[] = result.getLocalAddr()[1].int
  ctx.ready[] = true

proc wsReadHead(c: Socket): string =
  ## Read the client's HTTP upgrade request up to the blank line; "" if the peer
  ## disconnected mid-request (EOF or a recv error on an abrupt close).
  try:
    while "\r\n\r\n" notin result:
      let b = c.recv(1)
      if b.len == 0: return ""
      result.add b
  except CatchableError: return ""

proc wsHandshake(c: Socket): bool =
  ## Read the upgrade request and answer with the RFC 6455 101 accept. False if the
  ## client vanished before completing the request.
  let head = wsReadHead(c)
  if head.len == 0: return false
  var key = ""
  for line in head.splitLines:
    let i = line.find(':')
    if i > 0 and cmpIgnoreCase(line[0 ..< i].strip, "sec-websocket-key") == 0:
      key = line[i + 1 .. ^1].strip
  c.send("HTTP/1.1 101 Switching Protocols\r\n" &
         "Upgrade: websocket\r\nConnection: Upgrade\r\n" &
         "Sec-WebSocket-Accept: " & acceptFor(key) & "\r\n\r\n")
  true

template wsAcceptOne(ctx: WsSrv, server, c: untyped; body: untyped) =
  ## Bind, accept one connection, run `body`, then tear both down.
  let server = wsBind(ctx)
  var c: Socket
  server.accept(c)
  body
  c.close(); server.close()

proc serveWsEcho(ctx: WsSrv) {.thread.} =
  ## Echo text/binary, pong pings, reply to "please fragment" with two fragments,
  ## and send a server-initiated close on "bye".
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      var dec: WsDecoder
      var running = true
      while running:
        var f: Frame
        while not dec.next(f):
          let chunk = c.recv(4096)
          if chunk.len == 0: running = false; break
          dec.feed(chunk)
        if not running: break
        case f.opcode
        of opText:
          if f.payload == "please fragment":
            c.send(encodeFrame(opText, "frag", masked = false, fin = false))
            c.send(encodeFrame(opContinuation, "-ment", masked = false, fin = true))
          elif f.payload == "bye":
            c.send(encodeFrame(opClose, closePayload(closeNormal), masked = false))
            running = false
          else:
            c.send(encodeFrame(opText, f.payload, masked = false))
        of opBinary: c.send(encodeFrame(opBinary, f.payload, masked = false))
        of opPing: c.send(encodeFrame(opPong, f.payload, masked = false))
        of opClose: running = false
        else: discard

proc serveWsSilent(ctx: WsSrv) {.thread.} =
  ## Handshake, then never respond (ignore pings), reading and discarding until the
  ## client gives up -- a client with keepalive must time out and drop us.
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      try:                                   # an abrupt client close can raise here
        while c.recv(4096).len > 0: discard  # rather than returning EOF; don't die
      except CatchableError: discard         # in-thread (trips AddressSanitizer's join)

proc serveWsStall(ctx: WsSrv) {.thread.} =
  ## Accept and read the upgrade request but never send the 101, so the client's
  ## open blocks until its timeout fires; then hold briefly and tear down.
  wsAcceptOne(ctx, server, c):
    discard wsReadHead(c)
    sleep(2000)     # hold past the client's connect timeout

proc serveWsPingCounter(ctx: WsSrv) {.thread.} =
  ## Pong every ping, and after the second ping send a text message -- so a client
  ## with keepalive stays alive across pings and finally receives it.
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      var dec: WsDecoder
      var pings = 0
      var running = true
      while running:
        var f: Frame
        while not dec.next(f):
          let chunk = c.recv(4096)
          if chunk.len == 0: running = false; break
          dec.feed(chunk)
        if not running: break
        case f.opcode
        of opPing:
          c.send(encodeFrame(opPong, f.payload, masked = false))
          inc pings
          if pings == 2: c.send(encodeFrame(opText, "alive", masked = false))
        of opClose: running = false
        else: discard

proc serveWsStreamEcho(ctx: WsSrv) {.thread.} =
  ## Reassemble messages and, on the text trigger "fragment", reply with a
  ## 3-fragment message; otherwise echo the whole message as one frame.
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      var dec: WsDecoder
      var asmb: WsAssembler
      var running = true
      while running:
        var f: Frame
        while not dec.next(f):
          let chunk = c.recv(4096)
          if chunk.len == 0: running = false; break
          dec.feed(chunk)
        if not running: break
        let o = asmb.offer(f)
        case o.reply
        of wrPong: c.send(encodeFrame(opPong, o.replyPayload, masked = false))
        of wrCloseEcho: running = false
        of wrNone: discard
        if o.ready:
          case o.message.kind
          of wmText:
            if o.message.data == "fragment":
              c.send(encodeFrame(opText, "one", masked = false, fin = false))
              c.send(encodeFrame(opContinuation, "-two", masked = false, fin = false))
              c.send(encodeFrame(opContinuation, "-three", masked = false, fin = true))
            else:
              c.send(encodeFrame(opText, o.message.data, masked = false))
          of wmBinary: c.send(encodeFrame(opBinary, o.message.data, masked = false))
          of wmClose: running = false

proc serveWsMisbehave(ctx: WsSrv) {.thread.} =
  ## Handshake, then answer the client's first text frame with the deliberately
  ## broken reply its payload names, and watch what the client does next: `sawEof`
  ## records whether it tore the transport down within a second, which is what a
  ## protocol error must do (a leaked transport would just stay open).
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      var dec: WsDecoder
      var cmd = ""
      var alive = true
      while alive and cmd.len == 0:
        var f: Frame
        while not dec.next(f):
          let chunk = c.recv(4096)
          if chunk.len == 0:
            alive = false
            break
          dec.feed(chunk)
        if not alive: break
        if f.opcode == opText: cmd = f.payload
      case cmd
      of "masked":        # RFC 6455 5.1: a server-to-client frame must not be masked
        c.send(encodeFrame(opText, "nope", masked = true))
      of "badutf8":       # RFC 6455 8.1: a text message must be valid UTF-8
        c.send(encodeFrame(opText, "\xff\xfe", masked = false))
      of "orphan":        # RFC 6455 5.4: a continuation with no message open
        c.send(encodeFrame(opContinuation, "x", masked = false, fin = true))
      of "datamid":       # RFC 6455 5.4: a data frame where a continuation is due
        c.send(encodeFrame(opText, "aa", masked = false, fin = false))
        c.send(encodeFrame(opText, "bb", masked = false, fin = true))
      of "splitbad":      # a surrogate (U+D800) split across two frames: invalid
        c.send(encodeFrame(opText, "\xed\xa0", masked = false, fin = false))
        c.send(encodeFrame(opContinuation, "\x80", masked = false, fin = true))
      of "splitok":       # U+1F4A9 split across two frames: valid, must be accepted
        c.send(encodeFrame(opText, "\xf0\x9f", masked = false, fin = false))
        c.send(encodeFrame(opContinuation, "\x92\xa9", masked = false, fin = true))
      of "closebad":      # RFC 6455 7.4: a close code that must never be sent (1006)
        c.send(encodeFrame(opClose, closePayload(closeAbnormal), masked = false))
      of "closeshort":    # RFC 6455 5.5.1: a close body is empty or >= 2 bytes
        c.send(encodeFrame(opClose, "\x03", masked = false))
      of "midclosebad":   # a close with a reserved code interrupts a streamed message
        c.send(encodeFrame(opText, "aa", masked = false, fin = false))
        c.send(encodeFrame(opClose, closePayload(closeAbnormal), masked = false))
      of "midcloseok":    # a valid close interrupts a streamed message
        c.send(encodeFrame(opText, "aa", masked = false, fin = false))
        c.send(encodeFrame(opClose, closePayload(closeGoingAway, "later"), masked = false))
      of "eofnow":        # drop the transport with no close frame (abnormal closure)
        discard           # wsAcceptOne closes the socket on the way out
      else: discard
      if ctx.sawEof != nil and cmd != "eofnow":
        ctx.sawEof[] = false
        try:
          while true:
            let b = c.recv(4096, timeout = 1500)   # bounded: never hang the suite
            if b.len == 0:
              ctx.sawEof[] = true
              break
        except CatchableError: discard             # timeout: the client kept it open

proc startWs(th: var Thread[WsSrv], run: proc(ctx: WsSrv) {.thread.}, port: var int) =
  ## Launch a WS server on an ephemeral port, write it to `port` (a mutable `var`),
  ## and block until it is listening.
  var ready = false
  createThread(th, run, WsSrv(ready: addr ready, portOut: addr port))
  while not ready: sleep(1)

proc startWsEcho*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsEcho, port)
proc startWsSilent*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsSilent, port)
proc startWsStall*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsStall, port)
proc startWsPingCounter*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsPingCounter, port)
proc startWsStreamEcho*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsStreamEcho, port)

proc startWsMisbehave*(th: var Thread[WsSrv], port: var int, sawEof: var bool) =
  ## The misbehaving server, plus the teardown flag it reports (see serveWsMisbehave).
  var ready = false
  createThread(th, serveWsMisbehave,
               WsSrv(ready: addr ready, portOut: addr port, sawEof: addr sawEof))
  while not ready: sleep(1)
