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
  import std/[net, openssl, nativesockets]
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

  proc newTlsContext*(cfg: TlsConfig, alpn: openArray[string] = @[]): SslContext =
    ## Build a client TLS context. Verification and CA trust come from
    ## `newContext` (the security-critical path); then ALPN and any configured
    ## client certificate are applied. When a client cert is present it is
    ## installed by `loadClientCert`, so `newContext` is handed an empty cert/key
    ## and every credential form (plain PEM included) takes one path. Raises
    ## `ValueError` on malformed or mismatched TLS material.
    let custom = hasClientCert(cfg)
    result = newContext(
      verifyMode = if cfg.wantsVerify: CVerifyPeer else: CVerifyNone,
      certFile = if custom: "" else: cfg.certFile,
      keyFile = if custom: "" else: cfg.clientKeyFile,
      caFile = cfg.caFile)
    if custom: loadClientCert(result.context, cfg)
    setAlpn(result.context, alpn)

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

  proc startClientTls*(ctx: SslContext, fd: SocketHandle, host: string,
                       verify: bool): SslPtr =
    ## Attach a client SSL to the already-connected socket `fd`, set SNI, drive
    ## the handshake, and -- when `verify` -- confirm the chain and the peer's
    ## identity against `host`. Replaces std/net's `wrapConnectedSocket` so SNI
    ## and the verified hostname stay under navi's control; the context from
    ## `newTlsContext` carries the CA/chain trust and ALPN. Mirrors std/net: SNI
    ## and the hostname check apply to DNS-name hosts, not IP literals. Raises
    ## `ValueError` on any failure; `negotiatedProtocol(result)` reads the ALPN.
    result = SSL_new(ctx.context)
    if result.isNil: fail("SSL_new failed")
    var ok = false
    defer:
      if not ok: SSL_free(result)
    discard SSL_set_fd(result, fd)
    let nameHost = host.len > 0 and not isIpAddress(host)
    if nameHost:
      discard SSL_set_tlsext_host_name(result, host.cstring)   # SNI
    if SSL_connect(result) != 1:
      fail("TLS handshake failed for " & host)
    if verify:
      # The context verifies the chain against the CA (SSL_VERIFY_PEER, so a bad
      # chain already aborts SSL_connect); confirm the result, then match the
      # certificate identity for DNS-name hosts.
      if SSL_get_verify_result(result) != X509_V_OK:
        fail("certificate verification failed for " & host)
      if nameHost: checkCertName(result, host)
    ok = true
