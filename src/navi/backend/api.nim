## The transport contract every engine backend fulfils.
##
## A backend provides a `Conn` type and four operations, each blocking in the
## sync backend and returning a Future in the async backends:
##
##   connect(host, port, tls, cfg) -> Conn
##   sendAll(conn, data)
##   recvSome(conn) -> string        ## "" signals the peer closed
##   close(conn)
##
## The shared engine (`core/engine.nim`) drives these through `await`, which is
## the real await in async backends and an identity template in the sync one.
## TLS is negotiated inside `connect` based on the `tls` flag, so the engine
## stays transport- and scheme-agnostic.

type
  TlsConfig* = object
    ## TLS options, including the client certificate for mTLS. The client
    ## credential can come from several sources; precedence is `pkcs12File`, then
    ## in-memory (`certPem`/`keyPem`), then the `certFile`/`keyFile` pair. Files
    ## may be PEM or DER (detected by content). All are honored only on the
    ## OpenSSL backends (sync, asyncdispatch); chronos (BearSSL) and js do not
    ## present client certificates.
    verify*: bool          ## verify the cert chain and hostname (default on)
    caFile*: string        ## custom CA bundle path; "" uses the system trust store
    certFile*: string      ## client certificate file (PEM or DER) for mTLS
    keyFile*: string       ## private key file for `certFile`; "" reuses certFile
    password*: string      ## passphrase for an encrypted key, or the PKCS#12 bundle password
    pkcs12File*: string    ## a PKCS#12/PFX bundle (cert + key + chain); highest precedence
    certPem*: string       ## client certificate as an in-memory PEM string (may hold a chain)
    keyPem*: string        ## private key as an in-memory PEM string ("" reuses `certPem`)
    resumeSessions*: bool  ## reuse TLS sessions across connections to the same origin
                           ## (abbreviated handshake); on by default via `defaultTls()`
    sessionCache*: RootRef ## per-client session store, set by `newNavi`; the TLS
                           ## backend owns the concrete type. Not user-configurable.

  ProxyTarget* = object
    ## The HTTP proxy to dial through. An empty `host` means a direct
    ## connection. For https targets the backend issues a CONNECT tunnel.
    host*: string
    port*: int

proc wantsVerify*(tls: TlsConfig): bool = tls.verify
  ## Whether to verify the cert chain and hostname. `defaultTls()` /
  ## `initNaviConfig()` turn it on; a bare `TlsConfig()` leaves it off, so build
  ## configs through those to stay secure by default.

proc wantsResume*(tls: TlsConfig): bool = tls.resumeSessions
  ## Whether to reuse TLS sessions across connections to the same origin (a
  ## resumed handshake skips the certificate exchange and the server's signature).
  ## `defaultTls()` / `initNaviConfig()` turn it on.

proc clientKeyFile*(tls: TlsConfig): string =
  ## Path to the client private key: `keyFile` when set, otherwise `certFile`
  ## (a single PEM commonly holds both the certificate and its key).
  if tls.keyFile.len > 0: tls.keyFile else: tls.certFile

proc defaultTls*(): TlsConfig =
  TlsConfig(verify: true, resumeSessions: true)  # secure + fast by default

proc direct*(): ProxyTarget = ProxyTarget()
proc isSet*(p: ProxyTarget): bool = p.host.len > 0
