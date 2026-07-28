## Load a client certificate + key into an OpenSSL `SslCtx` from sources that
## std/net's `newContext` cannot handle: an encrypted PEM key, a DER cert/key,
## a PKCS#12 (`.p12`/`.pfx`) bundle, or in-memory PEM strings. Shared by the
## OpenSSL backends (sync, asyncdispatch). Empty unless compiled with `-d:ssl`.
##
## When `hasClientCert` reports a credential is configured, the backend hands
## `newContext` an empty cert/key and calls `loadClientCert` to install it, so
## every client-cert case (including plain PEM files) takes one uniform path.

import ./api

when defined(ssl):
  import std/openssl

  const SSL_CTRL_EXTRA_CHAIN_CERT = 14

  # libssl / libcrypto entry points not exposed by std/openssl (mirrors the
  # importc style of the wrapper). SSL_CTX_use_certificate/PrivateKey both bump
  # the object's refcount, so we free our reference after handing it over;
  # add_extra_chain_cert (via SSL_CTX_ctrl) transfers ownership, so we do not.
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

  proc loadClientCert*(ctx: SslCtx, tls: TlsConfig) =
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

proc hasClientCert*(tls: TlsConfig): bool =
  ## Whether a client certificate is configured (so the backend routes through
  ## `loadClientCert` and hands `newContext` an empty cert/key).
  tls.pkcs12File.len > 0 or tls.certPem.len > 0 or tls.certFile.len > 0
