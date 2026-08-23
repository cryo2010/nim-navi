## First-party OpenSSL TLS context builder for the sync and asyncdispatch
## backends. This is the single place navi configures an `SSL_CTX`:
##
##   * certificate verification and CA trust come from std/net's `newContext`,
##     which owns the security-critical chain and hostname checks;
##   * ALPN (h2 / http/1.1) is set here;
##   * the client certificate -- encrypted PEM, DER, PKCS#12, or in-memory PEM --
##     is installed here, covering everything `newContext` cannot.
##
## Backends call `newTlsContext` and, after the handshake, `negotiatedProtocol`;
## they no longer touch `newContext`, ALPN, or the credential loader directly.
## Empty unless compiled with `-d:ssl`.

import ./api

when defined(ssl):
  import std/[net, openssl, nativesockets, tables, strutils]
  # Re-export the context type + destructor so backends can own the socket and
  # handshake while still building the (verified) context through newContext.
  export net.SslContext, net.destroyContext

  # --- ALPN --------------------------------------------------------------

  proc setAlpn(ctx: SslCtx, protos: openArray[string]) =
    ## Offer `protos` (e.g. @["h2", "http/1.1"]) on the context before the
    ## handshake; SSL handles created from it inherit the list.
    if protos.len == 0: return
    var wire: string
    for p in protos:
      wire.add char(p.len)
      wire.add p
    discard SSL_CTX_set_alpn_protos(ctx, wire.cstring, cuint(wire.len))

  proc negotiatedProtocol*(ssl: SslPtr): string =
    ## The ALPN protocol the peer selected, read after the handshake.
    var data: cstring
    var length: cuint
    SSL_get0_alpn_selected(ssl, addr data, addr length)
    if length > 0:
      result = newString(int(length))
      copyMem(addr result[0], data, int(length))

  # --- client certificate (mTLS) -----------------------------------------
  #
  # libssl / libcrypto entry points not exposed by std/openssl (mirrors the
  # importc style of the wrapper). SSL_CTX_use_certificate/PrivateKey both bump
  # the object's refcount, so we free our reference after handing it over;
  # add_extra_chain_cert (via SSL_CTX_ctrl) transfers ownership, so we do not.

  const SSL_CTRL_EXTRA_CHAIN_CERT = 14

  proc SSL_CTX_use_certificate(ctx: SslCtx, x: PX509): cint
    {.cdecl, dynlib: DLLSSLName, importc.}
  proc SSL_CTX_use_PrivateKey(ctx: SslCtx, pkey: EVP_PKEY): cint
    {.cdecl, dynlib: DLLSSLName, importc.}
  proc PEM_read_bio_X509(bp: BIO, x: ptr PX509, cb: pointer, u: pointer): PX509
    {.cdecl, dynlib: DLLUtilName, importc.}
  proc d2i_PKCS12_bio(bp: BIO, p12: ptr pointer): pointer
    {.cdecl, dynlib: DLLUtilName, importc.}
  proc PKCS12_parse(p12: pointer, pass: cstring, pkey: ptr EVP_PKEY,
                    cert: ptr PX509, ca: ptr PSTACK): cint
    {.cdecl, dynlib: DLLUtilName, importc.}
  proc PKCS12_free(p12: pointer) {.cdecl, dynlib: DLLUtilName, importc.}
  # Match an IP literal against the certificate's iPAddress SANs. std/openssl
  # wraps X509_check_host (DNS names) but not this IP variant.
  proc X509_check_ip_asc(cert: PX509, ipasc: cstring, flags: cuint): cint
    {.cdecl, dynlib: DLLUtilName, importc.}

  when defined(windows):
    # std/openssl hides its whole X509 block behind `not defined(windows)`, so on
    # Windows we declare the three entry points navi's verification path needs.
    # They live in libcrypto (std/openssl asks libssl for them, which only works
    # where libssl re-exports libcrypto); the peer-certificate getter is libssl's,
    # and it was renamed in OpenSSL 3.0 -- `useOpenssl3` follows the -d:sslVersion
    # used to pick the DLL names, so both eras resolve to the name they export.
    proc X509_free(cert: PX509) {.cdecl, dynlib: DLLUtilName, importc.}
    proc X509_check_host(cert: PX509, name: cstring, namelen: cint, flags: cuint,
                         peername: cstring): cint
      {.cdecl, dynlib: DLLUtilName, importc.}
    when useOpenssl3:
      proc SSL_get1_peer_certificate(ssl: SslCtx): PX509
        {.cdecl, dynlib: DLLSSLName, importc.}
      proc SSL_get_peer_certificate(ssl: SslCtx): PX509 =
        SSL_get1_peer_certificate(ssl)
    else:
      proc SSL_get_peer_certificate(ssl: SslCtx): PX509
        {.cdecl, dynlib: DLLSSLName, importc.}

  proc fail(msg: string) {.noreturn.} =
    raise newException(ValueError, "navi: " & msg)

  proc memBio(data: string): BIO =
    if data.len == 0: fail("empty certificate/key data")
    result = BIO_new_mem_buf(unsafeAddr data[0], data.len.cint)
    if result.isNil: fail("could not allocate a memory BIO")

  proc addChainCert(ctx: SslCtx, x: PX509): bool =
    ## Transfers ownership of `x` to `ctx` on success.
    SSL_CTX_ctrl(ctx, SSL_CTRL_EXTRA_CHAIN_CERT.cint, 0, cast[pointer](x)) > 0

  proc useCertChainPem(ctx: SslCtx, pem: string) =
    ## Install the leaf certificate, then any following certs as the chain.
    let bio = memBio(pem)
    defer: discard BIO_free(bio)
    let leaf = PEM_read_bio_X509(bio, nil, nil, nil)
    if leaf.isNil: fail("no certificate found in the PEM data")
    if SSL_CTX_use_certificate(ctx, leaf) != 1:
      X509_free(leaf); fail("could not use the client certificate")
    X509_free(leaf)
    while true:
      let extra = PEM_read_bio_X509(bio, nil, nil, nil)
      if extra.isNil: break                      # end of certs (or the key block)
      if not addChainCert(ctx, extra):
        X509_free(extra); fail("could not add an intermediate certificate")

  proc useKeyPem(ctx: SslCtx, pem, password: string) =
    let bio = memBio(pem)
    defer: discard BIO_free(bio)
    # OpenSSL treats a non-nil `u` with a nil callback as the passphrase itself.
    let u = if password.len > 0: password.cstring else: nil
    let pkey = PEM_read_bio_PrivateKey(bio, nil, nil, u)
    if pkey.isNil: fail("could not read the private key (wrong password?)")
    if SSL_CTX_use_PrivateKey(ctx, pkey) != 1:
      EVP_PKEY_free(pkey); fail("the private key does not match the certificate")
    EVP_PKEY_free(pkey)

  proc usePkcs12(ctx: SslCtx, data, password: string) =
    ## Install the leaf certificate and key from a PKCS#12 bundle. The bundle's
    ## extra chain certs are not installed (nil chain out-param): the stack
    ## helpers needed to walk them are not portably exported across
    ## OpenSSL/LibreSSL, and a client only needs to present its leaf + key.
    let bio = memBio(data)
    defer: discard BIO_free(bio)
    let p12 = d2i_PKCS12_bio(bio, nil)
    if p12.isNil: fail("could not parse the PKCS#12 bundle")
    defer: PKCS12_free(p12)
    var pkey: EVP_PKEY
    var cert: PX509
    if PKCS12_parse(p12, password.cstring, addr pkey, addr cert, nil) != 1:
      fail("could not decrypt the PKCS#12 bundle (wrong password?)")
    if cert.isNil or pkey.isNil: fail("the PKCS#12 bundle lacks a cert or key")
    if SSL_CTX_use_certificate(ctx, cert) != 1:
      X509_free(cert); fail("could not use the PKCS#12 certificate")
    X509_free(cert)
    if SSL_CTX_use_PrivateKey(ctx, pkey) != 1:
      EVP_PKEY_free(pkey); fail("the PKCS#12 key does not match the certificate")
    EVP_PKEY_free(pkey)

  proc isDer(data: string): bool =
    ## DER starts with the ASN.1 SEQUENCE tag (0x30); PEM starts with '-'.
    data.len > 0 and data[0] == '\x30'

  proc useCertFile(ctx: SslCtx, path: string) =
    let data = readFile(path)
    if isDer(data):                           # single DER cert (no chain)
      if SSL_CTX_use_certificate_file(ctx, path.cstring, SSL_FILETYPE_ASN1) != 1:
        fail("could not load the DER certificate: " & path)
    else:
      useCertChainPem(ctx, data)              # PEM leaf + any following chain

  proc useKeyFile(ctx: SslCtx, path, password: string) =
    let data = readFile(path)
    if isDer(data):
      if SSL_CTX_use_PrivateKey_file(ctx, path.cstring, SSL_FILETYPE_ASN1) != 1:
        fail("could not load the DER private key: " & path)
    else:
      useKeyPem(ctx, data, password)          # PEM, possibly encrypted

  proc hasClientCert(tls: TlsConfig): bool =
    tls.pkcs12File.len > 0 or tls.certPem.len > 0 or tls.certFile.len > 0

  proc loadClientCert(ctx: SslCtx, tls: TlsConfig) =
    ## Install the client credential described by `tls`. Precedence: PKCS#12,
    ## then in-memory PEM, then the cert/key files. File encoding (PEM vs DER) is
    ## detected from the content. Raises `ValueError` if the material is missing,
    ## malformed, or mismatched.
    if tls.pkcs12File.len > 0:
      usePkcs12(ctx, readFile(tls.pkcs12File), tls.password)
    elif tls.certPem.len > 0:
      useCertChainPem(ctx, tls.certPem)
      useKeyPem(ctx, (if tls.keyPem.len > 0: tls.keyPem else: tls.certPem),
                tls.password)
    else:
      useCertFile(ctx, tls.certFile)
      useKeyFile(ctx, clientKeyFile(tls), tls.password)
    if SSL_CTX_check_private_key(ctx) != 1:
      fail("the client certificate and private key do not match")

  # --- the builder -------------------------------------------------------

  const
    SSL_CTRL_SET_MIN_PROTO_VERSION = 123
    SSL_CTRL_SET_MAX_PROTO_VERSION = 124

  proc osslTlsVersion(v: TlsVersion): clong =
    ## The OpenSSL protocol-version constant for a navi `TlsVersion`.
    case v
    of tlsDefault: 0
    of tls10: 0x0301   # TLS1_VERSION
    of tls11: 0x0302   # TLS1_1_VERSION
    of tls12: 0x0303   # TLS1_2_VERSION
    of tls13: 0x0304   # TLS1_3_VERSION

  proc setVersionBounds(ctx: SslCtx, cfg: TlsConfig) =
    ## Pin the negotiated TLS version range; `tlsDefault` leaves a bound unset.
    ## SSL_CTX_set_min/max_proto_version is a macro over SSL_CTX_ctrl in OpenSSL;
    ## we call the ctrl directly so it links against both OpenSSL and LibreSSL. A
    ## 0 return means the loaded library doesn't support it (e.g. the old LibreSSL
    ## macOS ships) -- surface that rather than silently ignore the pin.
    if cfg.minVersion != tlsDefault:
      if SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MIN_PROTO_VERSION.cint,
                      osslTlsVersion(cfg.minVersion), nil) != 1:
        fail("the loaded OpenSSL/LibreSSL does not support setting the minimum TLS version")
    if cfg.maxVersion != tlsDefault:
      if SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MAX_PROTO_VERSION.cint,
                      osslTlsVersion(cfg.maxVersion), nil) != 1:
        fail("the loaded OpenSSL/LibreSSL does not support setting the maximum TLS version")

  proc setCiphers(ctx: SslCtx, cfg: TlsConfig) =
    ## Restrict the offered ciphers. TLS <=1.2 and TLS 1.3 use separate OpenSSL
    ## APIs, so `ciphers` and `cipherSuites` are set independently; a non-1 return
    ## means every name was invalid/unknown, which we surface.
    if cfg.ciphers.len > 0:
      if SSL_CTX_set_cipher_list(ctx, cfg.ciphers.cstring) != 1:
        fail("no usable cipher in TlsConfig.ciphers: " & cfg.ciphers)
    if cfg.cipherSuites.len > 0:
      # SSL_CTX_set_ciphersuites (TLS 1.3) is missing from some old LibreSSL builds
      # (e.g. macOS system LibreSSL); std/openssl raises LibraryError there. Turn
      # that into a clear message rather than leaking the FFI error.
      var applied: cint
      try:
        applied = SSL_CTX_set_ciphersuites(ctx, cfg.cipherSuites.cstring)
      except CatchableError:
        fail("the loaded OpenSSL/LibreSSL does not support setting TLS 1.3 ciphersuites")
      if applied != 1:
        fail("no usable ciphersuite in TlsConfig.cipherSuites: " & cfg.cipherSuites)

  proc newTlsContext*(cfg: TlsConfig, alpn: openArray[string] = @[]): SslContext =
    ## Build a client TLS context. Verification and CA trust come from
    ## `newContext` (the security-critical path); then ALPN, the TLS version
    ## bounds, and any configured client certificate are applied. When a client
    ## cert is present it is installed by `loadClientCert`, so `newContext` is
    ## handed an empty cert/key and every credential form (plain PEM included)
    ## takes one path. Raises `ValueError` on malformed or mismatched TLS material.
    let custom = hasClientCert(cfg)
    result = newContext(
      verifyMode = if cfg.wantsVerify: CVerifyPeer else: CVerifyNone,
      certFile = if custom: "" else: cfg.certFile,
      keyFile = if custom: "" else: cfg.clientKeyFile,
      caFile = cfg.caFile)
    if custom: loadClientCert(result.context, cfg)
    setAlpn(result.context, alpn)
    setVersionBounds(result.context, cfg)
    setCiphers(result.context, cfg)

  # --- TLS session resumption --------------------------------------------
  #
  # A resumed handshake skips the certificate exchange and the server's
  # signature, so repeat connections to the same origin are much cheaper. We keep
  # a per-client cache of SSL_SESSIONs keyed by origin: OpenSSL hands us a session
  # through the new-session callback (in TLS 1.3 the ticket arrives after the
  # handshake, during the first reads), and we present it on the next connection.

  type
    TlsSessionCache* = ref object of RootObj
      ## Per-client store of resumable TLS sessions, keyed by "host:port". Held by
      ## the client through `TlsConfig.sessionCache`; freed with `close`.
      sessions: Table[string, pointer]   # origin -> SSL_SESSION*
    SessionSlot* = ref object
      ## Per-connection link from an SSL back to its cache and origin. Kept alive
      ## by the connection (its address lives in the SSL's ex_data), so the
      ## new-session callback can reach the cache while the connection is open.
      cache: TlsSessionCache
      origin: string

  proc CRYPTO_get_ex_new_index(classIndex: cint, argl: clong, argp: pointer,
    newf, dupf, freef: pointer): cint {.cdecl, dynlib: DLLUtilName, importc.}
  proc SSL_set_ex_data(ssl: SslPtr, idx: cint, arg: pointer): cint
    {.cdecl, dynlib: DLLSSLName, importc.}
  proc SSL_get_ex_data(ssl: SslPtr, idx: cint): pointer
    {.cdecl, dynlib: DLLSSLName, importc.}
  proc SSL_set_session(ssl: SslPtr, session: pointer): cint
    {.cdecl, dynlib: DLLSSLName, importc.}
  proc SSL_SESSION_free(session: pointer) {.cdecl, dynlib: DLLSSLName, importc.}
  proc SSL_get_SSL_CTX(ssl: SslPtr): SslCtx {.cdecl, dynlib: DLLSSLName, importc.}
  proc SSL_CTX_sess_set_new_cb(ctx: SslCtx,
    cb: proc(ssl: SslPtr, session: pointer): cint {.cdecl.})
    {.cdecl, dynlib: DLLSSLName, importc.}

  const
    CRYPTO_EX_INDEX_SSL = 0
    SSL_CTRL_SET_SESS_CACHE_MODE = 44
    SSL_SESS_CACHE_CLIENT = 0x0001
    SSL_SESS_CACHE_NO_INTERNAL_STORE = 0x0200

  var slotExIdx {.global.}: cint = -1
  proc ensureExIdx() =
    if slotExIdx < 0:
      slotExIdx = CRYPTO_get_ex_new_index(CRYPTO_EX_INDEX_SSL, 0, nil, nil, nil, nil)

  proc onNewSession(ssl: SslPtr, session: pointer): cint {.cdecl.} =
    ## Called by OpenSSL when a resumable session becomes available; we take
    ## ownership (return 1) and cache it under the connection's origin.
    {.cast(gcsafe).}:
      let p = SSL_get_ex_data(ssl, slotExIdx)
      if p.isNil: return 0
      let slot = cast[SessionSlot](p)
      let prev = slot.cache.sessions.getOrDefault(slot.origin, nil)
      if not prev.isNil: SSL_SESSION_free(prev)
      slot.cache.sessions[slot.origin] = session
      return 1

  proc newTlsSessionCache*(): TlsSessionCache =
    TlsSessionCache(sessions: initTable[string, pointer]())

  proc close*(cache: TlsSessionCache) =
    ## Free every cached session. Call when the client is closed.
    if cache.isNil: return
    for s in cache.sessions.values: SSL_SESSION_free(s)
    cache.sessions.clear()

  proc enableResumption*(ctx: SslContext, cache: TlsSessionCache) =
    ## Arm `ctx` to hand new sessions to `cache`. Call before creating the SSL.
    if cache.isNil: return
    ensureExIdx()
    discard SSL_CTX_ctrl(ctx.context, SSL_CTRL_SET_SESS_CACHE_MODE.cint,
      (SSL_SESS_CACHE_CLIENT or SSL_SESS_CACHE_NO_INTERNAL_STORE).clong, nil)
    SSL_CTX_sess_set_new_cb(ctx.context, onNewSession)

  proc newSlot*(cache: TlsSessionCache, origin: string): SessionSlot =
    SessionSlot(cache: cache, origin: origin)

  proc applySession*(ssl: SslPtr, slot: SessionSlot) =
    ## Link `ssl` to its cache/origin and, if a session is cached for that origin,
    ## present it so the handshake resumes. Call after SSL_new, before SSL_connect.
    if slot.isNil: return
    ensureExIdx()
    discard SSL_set_ex_data(ssl, slotExIdx, cast[pointer](slot))
    let s = slot.cache.sessions.getOrDefault(slot.origin, nil)
    if not s.isNil: discard SSL_set_session(ssl, s)

  # --- shared per-client context store -----------------------------------
  #
  # Building an SSL_CTX -- parsing the trust store, wiring up verification, ALPN,
  # version bounds, ciphers -- is the dominant per-connection cost on a cold
  # (unpooled) request. The context is immutable once built and safe to share, so
  # a client builds one per ALPN offer and every connection reuses it; only the
  # SSL is per-connection (cheap). The store is freed with the client, not per
  # connection.

  type
    TlsContextStore* = ref object of RootObj
      ## Per-client cache of shared TLS contexts, keyed by the ALPN offered (a
      ## client dials at most a couple of ALPN shapes: h2+http/1.1, or none over a
      ## proxy tunnel). Held through `TlsConfig.contextStore`; freed with `close`.
      contexts: Table[string, SslContext]

  proc newTlsContextStore*(): TlsContextStore =
    TlsContextStore(contexts: initTable[string, SslContext]())

  proc close*(store: TlsContextStore) =
    ## Free every shared context. Call when the client is closed, after its pooled
    ## connections have been closed (so no live SSL still references a context).
    if store.isNil: return
    for ctx in store.contexts.values: ctx.destroyContext()
    store.contexts.clear()

  proc buildContext(cfg: TlsConfig, alpn: openArray[string]): SslContext =
    ## A verified context with resumption armed (when enabled). Arming happens once
    ## here, not per connection, since the context is shared.
    result = newTlsContext(cfg, alpn)
    if cfg.wantsResume and not cfg.sessionCache.isNil:
      enableResumption(result, cast[TlsSessionCache](cfg.sessionCache))

  proc obtainContext*(store: RootRef, cfg: TlsConfig,
                      alpn: openArray[string]): tuple[ctx: SslContext, owned: bool] =
    ## The client's shared context for `alpn`, built once and cached. `owned` is
    ## true only when there is no store (a bare `TlsConfig`, e.g. in the interop
    ## tests): then the caller owns the context and must destroy it on close. With a
    ## store the context lives until the client is closed.
    if store.isNil:
      return (buildContext(cfg, alpn), true)
    let s = cast[TlsContextStore](store)
    let key = alpn.join("\x00")
    var ctx = s.contexts.getOrDefault(key, nil)
    if ctx.isNil:
      ctx = buildContext(cfg, alpn)
      s.contexts[key] = ctx
    (ctx, false)

  # --- per-connection handshake ------------------------------------------

  proc checkCertName(ssl: SslPtr, host: string) =
    ## Match the peer certificate's SAN/CN against `host` with X509_check_host,
    ## the same identity check std/net's `wrapConnectedSocket` runs. Callers skip
    ## it for IP literals (as std/net does): X509_check_host matches DNS names.
    let cert = SSL_get_peer_certificate(ssl)
    if cert.isNil: fail("server presented no certificate")
    const X509_CHECK_FLAG_ALWAYS_CHECK_SUBJECT = 0x1.cuint
    let match = X509_check_host(cert, host.cstring, host.len.cint,
                                X509_CHECK_FLAG_ALWAYS_CHECK_SUBJECT, nil)
    X509_free(cert)
    if match != 1: fail("certificate does not match host " & host)

  proc checkCertIp(ssl: SslPtr, host: string) =
    ## Match the peer certificate's iPAddress SAN against the IP literal `host`
    ## with X509_check_ip_asc. X509_check_host only matches DNS names, so without
    ## this an https-to-IP target would accept any chain-valid certificate.
    let cert = SSL_get_peer_certificate(ssl)
    if cert.isNil: fail("server presented no certificate")
    let match = X509_check_ip_asc(cert, host.cstring, 0)
    X509_free(cert)
    if match != 1: fail("certificate does not match IP " & host)

  proc newClientSsl*(ctx: SslContext, fd: SocketHandle, host: string,
                     slot: SessionSlot = nil): SslPtr =
    ## Create a client SSL bound to `fd`, set SNI (DNS-name hosts only, as
    ## std/net does), and present any cached session for resumption. The caller
    ## drives the handshake (blocking in the sync backend, await-based in the
    ## async one) and frees the SSL on failure.
    result = SSL_new(ctx.context)
    if result.isNil: fail("SSL_new failed")
    discard SSL_set_fd(result, fd)
    applySession(result, slot)   # present a cached session before the handshake
    if host.len > 0 and not isIpAddress(host):
      discard SSL_set_tlsext_host_name(result, host.cstring)   # SNI

  proc newClientSslMem*(ctx: SslContext, host: string,
                        slot: SessionSlot = nil): tuple[ssl: SslPtr, rbio, wbio: BIO] =
    ## Create a client SSL driven through a pair of memory BIOs instead of a
    ## socket fd, for an async backend that owns the ciphertext transport itself
    ## (chronos over a `StreamTransport`). Sets SNI, presents any cached session,
    ## and puts the SSL in connect state. The caller drives the handshake by
    ## feeding `rbio` (ciphertext in) and draining `wbio` (ciphertext out), then
    ## runs `verifyPeer`. `SSL_set_bio` transfers BIO ownership to the SSL, so the
    ## returned `rbio`/`wbio` are for pumping only -- freeing the SSL frees them.
    let ssl = SSL_new(ctx.context)
    if ssl.isNil: fail("SSL_new failed")
    let rbio = bioNew(bioSMem())
    let wbio = bioNew(bioSMem())
    if rbio.isNil or wbio.isNil:
      SSL_free(ssl); fail("could not allocate TLS memory BIOs")
    sslSetBio(ssl, rbio, wbio)   # SSL now owns both BIOs (freed by SSL_free)
    applySession(ssl, slot)      # present a cached session before the handshake
    if host.len > 0 and not isIpAddress(host):
      discard SSL_set_tlsext_host_name(ssl, host.cstring)   # SNI
    sslSetConnectState(ssl)
    (ssl, rbio, wbio)

  proc verifyPeer*(ssl: SslPtr, host: string, verify: bool) =
    ## After a completed handshake, confirm the chain (SSL_VERIFY_PEER already
    ## aborts the handshake on a bad chain; this is the belt-and-suspenders check)
    ## and the certificate identity: the SAN/CN for a DNS host, or the iPAddress
    ## SAN for an IP literal. No-op when `verify` is off. Raises `ValueError` on
    ## mismatch.
    if not verify: return
    if SSL_get_verify_result(ssl) != X509_V_OK:
      fail("certificate verification failed for " & host)
    if host.len > 0:
      if isIpAddress(host): checkCertIp(ssl, host)   # match the iPAddress SAN
      else: checkCertName(ssl, host)

  proc startClientTls*(ctx: SslContext, fd: SocketHandle, host: string,
                       verify: bool, slot: SessionSlot = nil): SslPtr =
    ## Blocking client handshake (sync backend): bind + SNI + resume, drive the
    ## handshake, verify. Replaces std/net's `wrapConnectedSocket` so SNI and the
    ## verified hostname stay under navi's control. Raises `ValueError` on failure;
    ## `negotiatedProtocol(result)` reads the ALPN. The async backend composes
    ## `newClientSsl` + an await-based handshake + `verifyPeer` itself.
    result = newClientSsl(ctx, fd, host, slot)
    var ok = false
    defer:
      if not ok: SSL_free(result)
    if SSL_connect(result) != 1:
      fail("TLS handshake failed for " & host)
    verifyPeer(result, host, verify)
    ok = true
