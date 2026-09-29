## Shared proxy-tunnel drivers (HTTP CONNECT + SOCKS5), one implementation for the
## three native backends. The handshake logic lived in each backend verbatim,
## differing only in the byte-I/O primitive. Here it is written once as templates:
## each backend supplies `sockWrite` / `sockReadExactly` / `sockReadSome` over its
## own connection handle plus an `await` (real in the async backends, an identity
## template in the sync one), and instantiates these. The SOCKS5 wire frames come
## from `core/socks`; this only drives the exchange.

import std/[strutils, base64]
import ../core/socks

const
  proxyConnectChunk = 1024
    ## Size of each read while pulling in the proxy CONNECT reply head.
  proxyConnectHeadMax = 16 * 1024
    ## Cap on the accumulated CONNECT reply head. TCP is free to split the reply
    ## across segments and a proxy may stack up Via/X-Cache headers, so the head
    ## has to be read to its CRLFCRLF terminator; this bounds that loop.

proc proxyStatusOk(line: string): bool =
  ## True when `line` is an "HTTP/1.x <2xx> ..." status line. RFC 9110 9.3.6 says
  ## any 2xx on CONNECT means the tunnel is established, so the three-digit code
  ## is parsed rather than prefix-matched against "200".
  if line.len < 12 or not line.startsWith("HTTP/1."): return false
  if not isDigit(line[7]) or line[8] != ' ': return false
  for i in 9 .. 11:
    if not isDigit(line[i]): return false
  if line.len > 12 and line[12] != ' ': return false   # 4-digit code is not a status
  let code = (ord(line[9]) - ord('0')) * 100 + (ord(line[10]) - ord('0')) * 10 +
             (ord(line[11]) - ord('0'))
  code >= 200 and code <= 299

template proxyConnectDriver*(conn, host, port, user, pass: typed) =
  ## Establish a CONNECT tunnel to `host:port` through an already-connected HTTP
  ## proxy, sending Proxy-Authorization when credentials are supplied. `conn` is
  ## the backend's connection handle; `sockWrite`/`sockReadSome`/`await` are mixed
  ## in from the instantiation site.
  mixin await, sockWrite, sockReadSome
  let target = host & ":" & $port
  var req = "CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n"
  if user.len > 0 or pass.len > 0:
    req.add("Proxy-Authorization: Basic " & encode(user & ":" & pass) & "\r\n")
  req.add("\r\n")
  await sockWrite(conn, req)
  # Read the reply head to its CRLFCRLF terminator instead of trusting one recv:
  # a status line and headers split across TCP segments, or a head larger than one
  # read, would otherwise leave proxy bytes on the socket for OpenSSL to parse as
  # the ServerHello (and a short first segment would look like a CONNECT failure).
  var resp = ""
  var head = -1
  while true:
    let chunk = await sockReadSome(conn, proxyConnectChunk)
    if chunk.len == 0:
      raise newException(IOError,
        "navi: proxy closed the connection before the CONNECT reply was complete")
    resp.add chunk
    head = resp.find("\r\n\r\n")
    if head >= 0: break
    if resp.len >= proxyConnectHeadMax:
      raise newException(ValueError, "navi: proxy CONNECT reply head exceeded " &
        $proxyConnectHeadMax & " bytes without a blank line")
  let statusLine = resp[0 ..< resp.find("\r\n")]
  if not proxyStatusOk(statusLine):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & statusLine)
  # TLS clients speak first, so a conforming proxy sends nothing between the blank
  # line and the tunnelled bytes. Nothing here can push leftovers back into the
  # backend's TLS read path, so fail loudly rather than silently dropping them.
  if resp.len > head + 4:
    raise newException(ValueError, "navi: proxy sent " & $(resp.len - head - 4) &
      " bytes after the CONNECT reply, before the tunnel was handed to TLS")

template socksConnectDriver*(conn, host, port, user, pass: typed) =
  ## Perform the SOCKS5 handshake (RFC 1928 + RFC 1929 user/pass) to tunnel to
  ## `host:port` through a connected proxy. The target is sent as a domain name so
  ## the proxy resolves DNS. `sockWrite`/`sockReadExactly`/`await` are mixed in.
  mixin await, sockWrite, sockReadExactly
  let hasAuth = user.len > 0 or pass.len > 0
  await sockWrite(conn, greeting(hasAuth))
  case selectedMethod(await sockReadExactly(conn, 2))
  of methodUserPass:
    if not hasAuth:
      raise newException(ValueError, "navi: SOCKS5 proxy requires authentication")
    await sockWrite(conn, authRequest(user, pass))
    checkAuthReply(await sockReadExactly(conn, 2))
  of methodNoAuth: discard
  else: raise newException(ValueError, "navi: SOCKS5 proxy rejected the offered auth methods")
  await sockWrite(conn, connectRequest(host, port))
  let header = await sockReadExactly(conn, 4)
  let status = replyStatus(header)
  if status != 0: raiseReply(status)
  let tail = boundTailLen(int(uint8(header[3])))   # discard BND.ADDR + BND.PORT
  if tail >= 0:
    discard await sockReadExactly(conn, tail)
  else:
    let dlen = int(uint8((await sockReadExactly(conn, 1))[0]))
    discard await sockReadExactly(conn, dlen + 2)
