## Async HTTP/3 driver for the asyncdispatch backend (phase 2h, slice 2). Drives
## the non-blocking step functions in h3client.cpp from the asyncdispatch event
## loop: register the UDP fd, pump outgoing datagrams, and await readiness OR the
## QUIC timer, never blocking. One request at a time (multiplexing is slice 3).
## Imports quic for the FFI and the {.compile.}/link pragmas. -d:naviHttp3-only.
when not defined(naviHttp3):
  {.error: "navi/backend/quic_async is a -d:naviHttp3-only module (HTTP/3 WIP).".}

import std/[asyncdispatch, strutils]
import ./quic
export quic

# Blocking send/recv on the connected UDP socket. recv is only called once the fd
# is readable (and QUIC always has data then), and UDP send returns immediately,
# so neither blocks the event loop.
proc sockSend(fd: cint, buf: pointer, len: csize_t, flags: cint): int
  {.importc: "send", header: "<sys/socket.h>".}
proc sockRecv(fd: cint, buf: pointer, len: csize_t, flags: cint): int
  {.importc: "recv", header: "<sys/socket.h>".}

template rawFd(fd: AsyncFD): cint = fd.int.cint

proc waitReadable(fd: AsyncFD, timeoutMs: int): Future[bool] =
  ## Complete with true when `fd` is readable, or false when `timeoutMs` elapses
  ## first. Both callbacks remove themselves when they fire; any that lingers (the
  ## one that lost the race) is cleared by unregister at request end.
  var fut = newFuture[bool]("navi.h3.waitReadable")
  let f = fut
  addRead(fd, proc(a: AsyncFD): bool =
    if not f.finished: f.complete(true)
    true)
  addTimer(timeoutMs, oneshot = true, proc(a: AsyncFD): bool =
    if not f.finished: f.complete(false)
    true)
  fut

proc driveWhile(c: pointer, fd: AsyncFD,
                pending: proc(): bool {.gcsafe.}): Future[void] {.async.} =
  ## Pump the connection until `pending()` becomes false: drain outgoing
  ## datagrams, then wait for readability or the QUIC timer and feed a datagram
  ## or run loss recovery.
  var buf: array[1500, uint8]
  while pending():
    var n = navi_h3_send(c, addr buf[0], csize_t(buf.len))
    while n > 0:
      discard sockSend(rawFd(fd), addr buf[0], csize_t(n), 0)
      n = navi_h3_send(c, addr buf[0], csize_t(buf.len))
    if n < 0:
      raise newException(QuicError, "navi HTTP/3: send failed")
    let readable = await waitReadable(fd, int(navi_h3_timeout_ms(c)))
    if readable:
      let r = sockRecv(rawFd(fd), addr buf[0], csize_t(buf.len), 0)
      if r > 0 and navi_h3_recv(c, addr buf[0], csize_t(r)) != 0:
        raise newException(QuicError, "navi HTTP/3: read_pkt failed")
    elif navi_h3_handle_timeout(c) != 0:
      raise newException(QuicError, "navi HTTP/3: handle_timeout failed")

proc h3RequestAsync*(host: string, port: int, sni, caFile: string, verify: bool,
                     verb, path: string, headers: seq[(string, string)],
                     body: string): Future[Http3Response] {.async.} =
  ## Open a QUIC connection, run one HTTP/3 request, and close, driven by the
  ## asyncdispatch event loop. `sni` defaults to `host`; cert + hostname are
  ## verified unless `verify` is false. Raises `QuicError` on failure.
  let name = if sni.len > 0: sni else: host
  let c = navi_h3_new(host.cstring, ($port).cstring, name.cstring, caFile.cstring,
                      cint(verify))
  if c == nil:
    raise newException(QuicError,
      "navi HTTP/3 connect to " & host & ":" & $port & " failed")
  let fd = navi_h3_fd(c).int.AsyncFD
  register(fd)
  try:
    await driveWhile(c, fd, proc(): bool {.gcsafe.} = navi_h3_handshake_done(c) == 0)
    if navi_h3_bind(c) != 0:
      raise newException(QuicError, "navi HTTP/3 bind failed")

    var reqHdr = ""
    for (k, v) in headers:
      reqHdr.add k; reqHdr.add '\n'; reqHdr.add v; reqHdr.add '\n'
    var b = body
    let bp = if b.len > 0: cast[ptr char](addr b[0]) else: nil
    if navi_h3_submit(c, verb.cstring, path.cstring, reqHdr.cstring, bp,
                      csize_t(b.len)) != 0:
      raise newException(QuicError, "navi HTTP/3 submit failed")
    await driveWhile(c, fd, proc(): bool {.gcsafe.} = navi_h3_request_done(c) == 0)

    var status: clong
    var blen, hlen: csize_t
    var rbody = newString(64 * 1024)
    var hbuf = newString(16 * 1024)
    if navi_h3_take_response(c, addr status, cast[ptr char](addr rbody[0]),
                             csize_t(rbody.len), addr blen,
                             cast[ptr char](addr hbuf[0]), csize_t(hbuf.len),
                             addr hlen) != 0:
      raise newException(QuicError, "navi HTTP/3 take_response failed")
    rbody.setLen(int(blen))
    hbuf.setLen(int(hlen))
    var hs: seq[(string, string)]
    let parts = hbuf.split('\n')
    var i = 0
    while i + 1 < parts.len:
      hs.add((parts[i], parts[i + 1]))
      i += 2
    result = Http3Response(status: int(status), body: rbody, headers: hs)
  finally:
    unregister(fd)
    navi_h3_close(c)
