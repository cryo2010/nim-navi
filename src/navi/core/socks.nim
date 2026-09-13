## Sans-io SOCKS5 client handshake (RFC 1928 + RFC 1929 user/pass auth).
##
## No sockets here: these build the request frames and parse the fixed-size
## replies, so the three native backends share one tested implementation and only
## supply the byte I/O. navi always sends the target as a domain name (the
## "socks5h" behavior), letting the proxy resolve DNS -- which is the point of a
## SOCKS proxy on a network the client cannot resolve or reach directly.

import bytewriter

type SocksError* = object of CatchableError

const
  ver = 0x05'u8            ## SOCKS protocol version
  authVer = 0x01'u8        ## username/password auth subnegotiation version
  cmdConnect = 0x01'u8
  atypIpv4 = 0x01
  atypDomain = 0x03
  atypIpv6 = 0x04

  methodNoAuth* = 0x00     ## server chose "no authentication required"
  methodUserPass* = 0x02   ## server chose username/password auth
  methodNone* = 0xFF       ## server rejected every offered method

proc fail(msg: string) {.noreturn.} =
  raise newException(SocksError, "navi: " & msg)

proc greeting*(hasAuth: bool): string =
  ## The client method-selection message: offer no-auth, plus user/pass when creds
  ## are available.
  var w = initByteWriter()
  w.addByte(ver)
  if hasAuth:
    w.addByte(2); w.addByte(methodNoAuth); w.addByte(methodUserPass)
  else:
    w.addByte(1); w.addByte(methodNoAuth)
  w.buf

proc selectedMethod*(reply: string): int =
  ## Parse the 2-byte method-selection reply; returns the chosen method byte.
  if reply.len < 2 or uint8(reply[0]) != ver:
    fail("malformed SOCKS5 greeting reply")
  int(uint8(reply[1]))

proc authRequest*(user, pass: string): string =
  ## The username/password subnegotiation request (RFC 1929).
  if user.len > 255 or pass.len > 255:
    fail("SOCKS5 username/password must be at most 255 bytes")
  var w = initByteWriter()
  w.addByte(authVer)
  w.addLenPrefixed(user)
  w.addLenPrefixed(pass)
  w.buf

proc checkAuthReply*(reply: string) =
  ## Validate the 2-byte auth reply; a non-zero status means bad credentials.
  if reply.len < 2 or uint8(reply[0]) != authVer:
    fail("malformed SOCKS5 auth reply")
  if reply[1] != '\0':
    fail("SOCKS5 proxy authentication failed")

proc connectRequest*(host: string, port: int): string =
  ## A CONNECT command to `host:port` using the domain-name address type, so the
  ## proxy performs DNS resolution.
  if host.len == 0 or host.len > 255:
    fail("SOCKS5 target host must be 1..255 bytes")
  var w = initByteWriter()
  w.addByte(ver); w.addByte(cmdConnect); w.addByte(0)  # RSV
  w.addByte(atypDomain)
  w.addLenPrefixed(host)
  w.addU16BE(uint16(port))
  w.buf

proc replyStatus*(header: string): int =
  ## Validate the 4-byte reply header (VER,REP,RSV,ATYP) and return REP (0 = ok).
  if header.len < 4 or uint8(header[0]) != ver:
    fail("malformed SOCKS5 connect reply")
  int(uint8(header[1]))

proc replyError*(status: int): string =
  ## RFC 1928 reply-code text, for a clear error on a failed CONNECT.
  case status
  of 1: "general SOCKS server failure"
  of 2: "connection not allowed by ruleset"
  of 3: "network unreachable"
  of 4: "host unreachable"
  of 5: "connection refused"
  of 6: "TTL expired"
  of 7: "command not supported"
  of 8: "address type not supported"
  else: "unknown error " & $status

proc raiseReply*(status: int) {.noreturn.} =
  fail("SOCKS5 connect failed: " & replyError(status))

proc boundTailLen*(atyp: int): int =
  ## Bytes of BND.ADDR + BND.PORT that follow the 4-byte reply header, by address
  ## type; -1 for a domain name (its length is a byte the caller must read first,
  ## then read that many bytes plus the 2 port bytes).
  case atyp
  of atypIpv4: 4 + 2
  of atypIpv6: 16 + 2
  of atypDomain: -1
  else: fail("unknown SOCKS5 address type in reply")
