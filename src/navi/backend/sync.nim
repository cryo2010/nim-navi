## Synchronous transport backend: blocking std/net sockets.
##
## `await` is an identity template here so the shared engine's `await`-shaped
## body compiles to straight-line blocking code.

import std/[net, os, strutils, nativesockets]
import ./api, ./openssl_alpn, ./openssl_creds
import ../core/response  # for navi's TimeoutError
when defined(ssl):
  import std/openssl  # SSL_pending

export api

type
  Conn* = object
    socket: Socket
    protocol*: string   ## ALPN-negotiated protocol ("h2" or "", meaning http/1.1)
    timeout: int        ## per-recv timeout in ms; 0 means block indefinitely
    when defined(ssl):
      ctx: SslContext   ## kept so `close` can free the SSL_CTX (destroyContext)

template await*(x: untyped): untyped = x

proc sleep*(ms: int) = os.sleep(ms)

proc proxyConnect(socket: Socket, host: string, port: int) =
  ## Establish a CONNECT tunnel to `host:port` through an already-dialed proxy.
  let target = host & ":" & $port
  socket.send("CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n\r\n")
  var resp = newString(1024)
  let n = socket.recv(addr resp[0], resp.len)
  resp.setLen(max(n, 0))
  if not resp.startsWith("HTTP/1.1 200") and not resp.startsWith("HTTP/1.0 200"):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget, alpn: seq[string] = @[], timeout = 0): Conn =
  ## Dial `host:port` (IPv4 or IPv6, resolved by std/net), upgrading to TLS for
  ## https. Through a proxy, https targets get a CONNECT tunnel and http targets
  ## are dialed directly to the proxy (the engine sends an absolute-URI).
  ## TLS requires compiling with `-d:ssl` (OpenSSL).
  ##
  ## The socket is unbuffered: std/net's buffered `recv(pointer, size)` blocks
  ## until it fills the whole buffer, which deadlocks on a kept-alive connection
  ## where the response is smaller than the buffer.
  result.timeout = timeout
  # Release the socket and TLS context if we don't finish connecting -- a failed
  # proxy CONNECT or TLS handshake (e.g. cert rejection) would otherwise leak
  # both, the SSL_CTX permanently (it has no destructor).
  var established = false
  defer:
    if not established:
      if not result.socket.isNil:
        try: result.socket.close()
        except CatchableError: discard
      when defined(ssl):
        if not result.ctx.isNil: result.ctx.destroyContext()
  if proxy.isSet:
    result.socket = dial(proxy.host, Port(proxy.port), buffered = false)
    if tls:
      proxyConnect(result.socket, host, port)
  else:
    result.socket = dial(host, Port(port), buffered = false)
  if tls:
    when defined(ssl):
      # Any configured client certificate (PEM/DER/PKCS#12/in-memory) is installed
      # by loadClientCert; newContext then only handles verification and the CA.
      let custom = hasClientCert(cfg)
      let ctx = newContext(
        verifyMode = if cfg.wantsVerify: CVerifyPeer else: CVerifyNone,
        certFile = if custom: "" else: cfg.certFile,
        keyFile = if custom: "" else: cfg.clientKeyFile,
        caFile = cfg.caFile)
      result.ctx = ctx     # store before the handshake so cleanup can free it
      if custom: loadClientCert(ctx.context, cfg)
      setAlpn(ctx.context, alpn)
      ctx.wrapConnectedSocket(result.socket, handshakeAsClient, host)
      result.protocol = negotiatedProtocol(result.socket.sslHandle)
    else:
      raise newException(ValueError,
        "navi: https requires compiling with -d:ssl")
  established = true

proc sendAll*(c: Conn, data: string) =
  c.socket.send(data)

proc waitReadable(c: Conn): bool =
  ## True when the socket has data ready within `c.timeout` ms. Checks OpenSSL's
  ## decrypted buffer first: `select` sees the raw fd, so bytes SSL already
  ## drained off it and buffered would otherwise be missed.
  when defined(ssl):
    let ssl = c.socket.sslHandle
    if not ssl.isNil and SSL_pending(ssl) > 0:
      return true
  var fds = @[c.socket.getFd()]
  selectRead(fds, c.timeout) > 0

proc recvSome*(c: Conn): string =
  ## One chunk of up to 4096 bytes; "" means the peer closed. With a timeout set,
  ## wait up to `timeout` ms for data (raising navi's TimeoutError on a stall),
  ## then read what is available. We deliberately avoid std/net's timeout `recv`:
  ## it loops until it has filled the whole buffer, so a response that ends
  ## mid-buffer on a kept-alive connection stalls it until the timeout even
  ## though the response is complete. A single plain `recv` returns immediately
  ## with whatever is ready (the same reason `connect` uses an unbuffered socket).
  result = newString(4096)
  if c.timeout > 0 and not c.waitReadable():
    raise newException(response.TimeoutError, "navi: read timed out")
  let n = c.socket.recv(addr result[0], result.len)
  if n <= 0:
    result.setLen(0)
  else:
    result.setLen(n)

proc close*(c: Conn) =
  c.socket.close()
  when defined(ssl):
    # std/net closes the SSL on socket.close but never the SSL_CTX; free it so a
    # long-lived client does not leak one context (~85 KB) per connection.
    if not c.ctx.isNil: c.ctx.destroyContext()
