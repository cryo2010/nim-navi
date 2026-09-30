## The transport contract every engine backend fulfils.
##
## A backend provides a `Conn` type and four operations, each blocking in the
## sync backend and returning a Future in the async backends:
##
##   connect(host, port, tls, cfg, proxy, alpn = @[],
##           connectMs = 0, readMs = 0, totalMs = 0) -> Conn
##   sendAll(conn, data)
##   recvSome(conn) -> string        ## "" signals the peer closed
##   close(conn)
##
## All three native backends declare `connect` with the same signature (including
## `totalMs`) so the shared engine can call it uniformly. `connectMs` bounds
## establishment; `readMs` is the per-read stall limit; `totalMs` is the overall
## per-attempt deadline (enforced inside `connect` on the sync backend, and by the
## async entry's `guard` on asyncdispatch/chronos, where the parameter is unused).
##
## The shared engine (`core/engine.nim`) drives these through `await`, which is
## the real await in async backends and an identity template in the sync one.
## TLS is negotiated inside `connect` based on the `tls` flag, so the engine
## stays transport- and scheme-agnostic.

const naviReadBufSize* = 65536
  ## Socket read chunk size in bytes. Draining the socket in 64 KiB reads instead of
  ## 4 KiB cuts read syscalls, per-read allocations, and framing/parse/decode passes
  ## ~16x on a fast stream -- the dominant throughput lever for downloads and SSE.

type
  TlsVersion* = enum
    ## A TLS protocol version for `TlsConfig.minVersion` / `maxVersion`.
    ## `tlsDefault` (the zero value) leaves that bound to the backend's default.
    tlsDefault, tls10, tls11, tls12, tls13

  CertVerifyProc* = proc(leafDer: string): bool {.closure, gcsafe, raises: [CatchableError].}
    ## User hook run after the built-in chain + hostname checks pass, receiving the
    ## peer's leaf certificate in DER form. Return false to reject the connection.
    ## Use it for extra checks (custom pinning, CT, name policy). To replace
    ## verification entirely, set `insecureSkipVerify = true` and do all the
    ## checking here.

  TlsConfig* = object
    ## TLS options, including the client certificate for mTLS. Honored on all
    ## three native backends (sync, asyncdispatch, chronos), which run OpenSSL;
    ## `navi/js` does not present client certificates.
    ##
    ## The fields fall into four groups, laid out in order below: peer
    ## verification, the client credential (mTLS), session/context reuse, and
    ## protocol/cipher selection. (They are kept as flat fields rather than
    ## nested sub-objects so the `TlsConfig(caFile: "ca.pem")` construction
    ## idiom keeps working; the grouping is expressed by layout.)
    ##
    ## Every field's zero value is the safe one, so a bare `TlsConfig()` or
    ## `TlsConfig(caFile: "ca.pem")` still verifies the peer. `defaultTls()` adds
    ## the performance defaults on top (session resumption).

    # --- Peer verification -------------------------------------------------
    insecureSkipVerify*: bool ## skip the cert chain and hostname checks entirely.
                           ## Off by default (the zero value verifies), and meant
                           ## only for tests against a self-signed server. The
                           ## legacy `verify` accessor below is its inverse.
    caFile*: string        ## custom CA bundle path. "" uses the system trust store;
                           ## set, it REPLACES the system roots rather than adding to
                           ## them (curl's `--cacert` semantics), on every backend
                           ## including h3, so only chains anchored in this file
                           ## verify and public sites stop verifying. Use `caBundle`
                           ## to trust an extra CA *and* keep the system roots.
    caBundle*: string      ## additional trusted CA certificates as an in-memory PEM
                           ## string; added to the trust store alongside `caFile` /
                           ## the system roots (supplements, does not replace). This
                           ## is the additive option: with `caFile` empty, a
                           ## `caBundle` extends the system roots.
    pinnedKeys*: seq[string] ## SPKI SHA-256 pins (base64, HPKP form). When non-empty,
                           ## the peer's public key must match one pin or the
                           ## connection is rejected -- checked after chain + hostname
    verifyCallback*: CertVerifyProc ## optional post-verification hook (see CertVerifyProc)

    # --- Client credential for mTLS ----------------------------------------
    # The credential can come from several sources; precedence is `pkcs12File`,
    # then in-memory (`certPem`/`keyPem`), then the `certFile`/`keyFile` pair.
    # Files may be PEM or DER, detected by content rather than extension: PEM when
    # a `-----BEGIN` boundary starts one of the file's lines (RFC 7468 allows
    # explanatory text before it), DER otherwise. An encrypted key is decrypted
    # with `password` in either encoding, PEM or PKCS#8 DER.
    pkcs12File*: string    ## a PKCS#12/PFX bundle (cert + key + chain); highest precedence
    certPem*: string       ## client certificate as an in-memory PEM string (may hold a chain)
    keyPem*: string        ## private key as an in-memory PEM string ("" reuses `certPem`)
    certFile*: string      ## client certificate file (PEM or DER) for mTLS
    keyFile*: string       ## private key file for `certFile`; "" reuses certFile
    password*: string      ## passphrase for an encrypted key (PEM or an encrypted
                           ## PKCS#8 DER key), or the PKCS#12 bundle password

    # --- Session + context reuse (performance) -----------------------------
    resumeSessions*: bool  ## reuse TLS sessions across connections to the same origin
                           ## (abbreviated handshake); on by default via `defaultTls()`
    sessionCache*: RootRef ## per-client session store, set by `newNavi`; the TLS
                           ## backend owns the concrete type. Not user-configurable.
    contextStore*: RootRef ## per-client shared TLS-context store, set by `newNavi`;
                           ## lets every connection reuse one SSL_CTX instead of
                           ## rebuilding it. Backend-owned type; not user-configurable.

    # --- Protocol version + cipher selection -------------------------------
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

  ResolvedProxy* = ref object
    ## The proxy configuration resolved ONCE at client construction (see
    ## core/proxy.nim `buildResolvedProxy`), so per-request resolution is a cheap
    ## NO_PROXY host match rather than repeated env reads and URL parsing. Held by
    ## `ref` so copying a config by value shares the immutable cache. `unix` (when
    ## its kind is `pkUnix`) short-circuits everything: a Unix socket bypasses
    ## proxies. Otherwise `httpTarget`/`httpsTarget` are the pre-parsed dial
    ## targets the request scheme selects between (identical when `proxy` is set,
    ## since an explicit proxy applies to both), and `noProxy` is the prepared
    ## exclusion list.
    unix*: ProxyTarget
    httpTarget*: ProxyTarget
    httpsTarget*: ProxyTarget
    noProxy*: seq[string]

proc wantsVerify*(tls: TlsConfig): bool = not tls.insecureSkipVerify
  ## Whether to verify the cert chain and hostname. On unless
  ## `insecureSkipVerify` was set, so every way of building a `TlsConfig`
  ## (including a bare one) authenticates the peer.

proc verify*(tls: TlsConfig): bool = not tls.insecureSkipVerify
  ## Compatibility accessor for the old `verify` field, which was replaced by
  ## `insecureSkipVerify` so the zero value would be the secure one. Prefer
  ## `insecureSkipVerify` (or `wantsVerify` to read it) in new code.

proc `verify=`*(tls: var TlsConfig, v: bool) =
  ## Compatibility setter for the old `verify` field: `tls.verify = false` is
  ## the same as `tls.insecureSkipVerify = true`.
  tls.insecureSkipVerify = not v

proc requireVerifiableHost*(host: string, verify: bool) =
  ## Fail closed when peer verification is on but there is no identity to check
  ## the certificate against. An empty `host` used to mean "chain-only": no SNI
  ## was sent, `SSL_set1_host` was never called and the post-handshake
  ## `X509_check_host` / `X509_check_ip_asc` step was skipped, so ANY certificate
  ## chaining to a trusted CA was accepted for the connection (#435). "Verify on"
  ## must never quietly become "verify the chain but not who is on the other end",
  ## so the handshake is refused instead.
  ##
  ## Reachable only through a URL the application built with no authority
  ## (`https:///path`) over a `unixSocket`, or on a platform whose
  ## `getaddrinfo("")` resolves to loopback; `buildRequest` and the WebSocket
  ## openers now reject such a URL up front, and this is the transport-level
  ## backstop for anything that reaches TLS another way (a middleware that
  ## rewrites `ctx.req.url`, a backend used directly). `insecureSkipVerify` is
  ## still an explicit opt-out: with verification off there is nothing to check
  ## and an empty host stays legal, which is what the Unix-socket and
  ## raw-fd test paths use.
  ##
  ## Pure logic, deliberately placed here rather than in `openssl_ctx` so it is
  ## unit-testable without loading libssl.
  if verify and host.len == 0:
    raise newException(ValueError,
      "navi: no hostname to verify against (the URL has an empty host); " &
      "give the URL a host, or set tls.insecureSkipVerify to connect without " &
      "an identity check")

proc h3TlsUsable*(tls: TlsConfig): bool =
  ## Whether an HTTP/3 leg can be taken at all under this TLS policy. QUIC always
  ## uses TLS 1.3 (RFC 9001 4.2), so a `maxVersion` below it can never be met on
  ## h3. The dispatcher then skips the advertised h3 endpoint and stays on h2/h1
  ## rather than failing a request over a bound the TCP legs satisfy fine.
  tls.maxVersion == tlsDefault or tls.maxVersion >= tls13

proc wantsResume*(tls: TlsConfig): bool = tls.resumeSessions
  ## Whether to reuse TLS sessions across connections to the same origin (a
  ## resumed handshake skips the certificate exchange and the server's signature).
  ## `defaultTls()` / `initNaviConfig()` turn it on.

proc clientKeyFile*(tls: TlsConfig): string =
  ## Path to the client private key: `keyFile` when set, otherwise `certFile`
  ## (a single PEM commonly holds both the certificate and its key).
  if tls.keyFile.len > 0: tls.keyFile else: tls.certFile

proc defaultTls*(): TlsConfig =
  TlsConfig(resumeSessions: true)  # verification is on by default; add resumption

proc direct*(): ProxyTarget = ProxyTarget()
proc isSet*(p: ProxyTarget): bool = p.host.len > 0

proc usesAbsoluteForm*(p: ProxyTarget, isTls: bool): bool =
  ## Whether a request should use absolute-URI form on its request line: only for a
  ## plain-http target through an HTTP proxy. A SOCKS5 proxy tunnels raw TCP, so the
  ## request uses origin form as if talking to the server directly.
  p.isSet and p.kind == pkHttp and not isTls
