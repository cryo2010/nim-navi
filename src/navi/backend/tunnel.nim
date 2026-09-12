## Shared proxy-tunnel drivers (HTTP CONNECT + SOCKS5), one implementation for the
## three native backends. The handshake logic lived in each backend verbatim,
## differing only in the byte-I/O primitive. Here it is written once as templates:
## each backend supplies `sockWrite` / `sockReadExactly` / `sockReadSome` over its
## own connection handle plus an `await` (real in the async backends, an identity
## template in the sync one), and instantiates these. The SOCKS5 wire frames come
## from `core/socks`; this only drives the exchange.

import std/[strutils, base64]
import ../core/socks

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
  let resp = await sockReadSome(conn, 1024)
  if not (resp.startsWith("HTTP/1.1 200") or resp.startsWith("HTTP/1.0 200")):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

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
