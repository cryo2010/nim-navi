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
  TlsVersion* = enum
    ## A TLS protocol version for `TlsConfig.minVersion` / `maxVersion`.
    ## `tlsDefault` (the zero value) leaves that bound to the backend's default.
    tlsDefault, tls10, tls11, tls12, tls13

  CertVerifyProc* = proc(leafDer: string): bool {.closure, gcsafe, raises: [CatchableError].}
    ## User hook run after the built-in chain + hostname checks pass, receiving the
    ## peer's leaf certificate in DER form. Return false to reject the connection.
    ## Use it for extra checks (custom pinning, CT, name policy). To replace
    ## verification entirely, set `verify=false` and do all the checking here.

  TlsConfig* = object
    ## TLS options, including the client certificate for mTLS. The client
    ## credential can come from several sources; precedence is `pkcs12File`, then
    ## in-memory (`certPem`/`keyPem`), then the `certFile`/`keyFile` pair. Files
    ## may be PEM or DER (detected by content). Honored on all three native
    ## backends (sync, asyncdispatch, chronos), which run OpenSSL; `navi/js` does
    ## not present client certificates.
    verify*: bool          ## verify the cert chain and hostname (default on)
    caFile*: string        ## custom CA bundle path; "" uses the system trust store
    caBundle*: string      ## additional trusted CA certificates as an in-memory PEM
                           ## string; added to the trust store alongside `caFile` /
                           ## the system roots (supplements, does not replace)
    pinnedKeys*: seq[string] ## SPKI SHA-256 pins (base64, HPKP form). When non-empty,
                           ## the peer's public key must match one pin or the
                           ## connection is rejected -- checked after chain + hostname
    verifyCallback*: CertVerifyProc ## optional post-verification hook (see CertVerifyProc)
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
    contextStore*: RootRef ## per-client shared TLS-context store, set by `newNavi`;
                           ## lets every connection reuse one SSL_CTX instead of
                           ## rebuilding it. Backend-owned type; not user-configurable.
    minVersion*: TlsVersion ## lowest TLS version to negotiate (`tlsDefault` = unset)
    maxVersion*: TlsVersion ## highest TLS version to negotiate (`tlsDefault` = unset)
    ciphers*: string       ## TLS <=1.2 cipher list, OpenSSL format (colon-separated,
                           ## e.g. "ECDHE-RSA-AES128-GCM-SHA256"); "" = library default
    cipherSuites*: string  ## TLS 1.3 ciphersuites (colon-separated, e.g.
                           ## "TLS_AES_128_GCM_SHA256"); "" = library default

  ProxyKind* = enum
    pkHttp     ## an HTTP proxy: CONNECT tunnel for https, absolute-URI for http
    pkSocks5   ## a SOCKS5 proxy: a raw TCP tunnel for both http and https targets
    pkUnix     ## not a proxy: dial this Unix socket path directly (`host` holds the
               ## path). The request still uses origin form and the URL host for
               ## Host + TLS SNI; proxies are bypassed.

  ProxyTarget* = object
    ## The proxy to dial through. An empty `host` means a direct connection. An
    ## HTTP proxy issues a CONNECT tunnel for https targets; a SOCKS5 proxy tunnels
    ## every target. `user`/`pass` authenticate to the proxy (Proxy-Authorization
    ## for HTTP CONNECT, RFC 1929 for SOCKS5).
    kind*: ProxyKind
    host*: string
    port*: int
    user*: string
    pass*: string

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

proc usesAbsoluteForm*(p: ProxyTarget, isTls: bool): bool =
  ## Whether a request should use absolute-URI form on its request line: only for a
  ## plain-http target through an HTTP proxy. A SOCKS5 proxy tunnels raw TCP, so the
  ## request uses origin form as if talking to the server directly.
  p.isSet and p.kind == pkHttp and not isTls
