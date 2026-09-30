## First-party OpenSSL TLS context builder for the sync and asyncdispatch
## backends. This is the single place navi configures an `SSL_CTX`:
##
##   * certificate verification and CA trust come from std/net's `newContext`,
##     which owns the security-critical chain and hostname checks;
##   * ALPN (h2 / http/1.1) is set here;
##   * the client certificate -- encrypted PEM, DER (plain or encrypted PKCS#8),
##     PKCS#12, or in-memory PEM -- is installed here, covering everything
##     `newContext` cannot.
##
## Backends call `newTlsContext` and, after the handshake, `negotiatedProtocol`;
## they no longer touch `newContext`, ALPN, or the credential loader directly.
## Empty unless compiled with `-d:ssl`.

import ./api

when defined(ssl):
  import std/[net, openssl, nativesockets, tables, strutils, base64, dynlib]
  import checksums/sha2
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
  const SSL_CTRL_CHAIN = 88   # SSL_CTX_set0_chain; takes ownership of the stack
  const X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS = 0x4.cuint
    ## X509_VERIFY_PARAM flag: reject partial wildcards (`fo*.example.com`), which
    ## RFC 6125 6.4.4 / RFC 9525 forbid; only `*.example.com` stays valid. This is the one hostname policy every
    ## navi transport uses: the pre-handshake `SSL_set_hostflags` below and the
    ## post-handshake `X509_check_host` both pass it, matching the h3 transport
    ## (h3client.cpp). We deliberately do not add X509_CHECK_FLAG_ALWAYS_CHECK_SUBJECT
    ## (std/net does): with it a certificate whose dNSName SANs all mismatch is
    ## still accepted when its subject CN happens to match. Without it OpenSSL
    ## applies its default, consulting the CN only when the certificate carries no
    ## dNSName SAN, so CN-only certificates from private CAs keep working.

  proc SSL_CTX_use_certificate(ctx: SslCtx, x: PX509): cint
    {.cdecl, dynlib: DLLSSLName, importc.}
  proc SSL_CTX_use_PrivateKey(ctx: SslCtx, pkey: EVP_PKEY): cint
    {.cdecl, dynlib: DLLSSLName, importc.}
  proc PEM_read_bio_X509(bp: BIO, x: ptr PX509, cb: pointer, u: pointer): PX509
    {.cdecl, dynlib: DLLUtilName, importc.}
  # DER private keys. d2i_PrivateKey_bio is d2i_AutoPrivateKey over a BIO: it
  # reads a traditional RSA/EC key or an unencrypted PKCS#8 PrivateKeyInfo.
  # d2i_PKCS8PrivateKey_bio reads the encrypted PKCS#8 shape
  # (EncryptedPrivateKeyInfo) and decrypts it with the passphrase callback.
  proc d2i_PrivateKey_bio(bp: BIO, a: ptr EVP_PKEY): EVP_PKEY
    {.cdecl, dynlib: DLLUtilName, importc.}
  proc d2i_PKCS8PrivateKey_bio(bp: BIO, x: ptr EVP_PKEY, cb: pointer,
                               u: pointer): EVP_PKEY
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
  # Optional entry points: the pre-handshake identity binding
  # (SSL_set1_host and friends, OpenSSL 1.1.0+ / LibreSSL 2.9+). A `dynlib`
  # importc is resolved when this module initialises and kills the process if the
  # symbol is absent, and old LibreSSL builds -- the one macOS resolves among
  # them -- export none of these four. So they are looked up by hand, once per
  # thread, and `bindExpectedIdentity` falls back to the post-handshake check
  # when the loaded library is too old. X509_VERIFY_PARAM is an opaque handle.
  type
    Set1HostProc = proc(ssl: SslPtr, hostname: cstring): cint {.cdecl, gcsafe, raises: [].}
    SetHostflagsProc = proc(ssl: SslPtr, flags: cuint) {.cdecl, gcsafe, raises: [].}
    Get0ParamProc = proc(ssl: SslPtr): pointer {.cdecl, gcsafe, raises: [].}
    Set1IpAscProc = proc(param: pointer, ipasc: cstring): cint {.cdecl, gcsafe, raises: [].}

  var
    sslSet1Host {.threadvar.}: Set1HostProc
    sslSetHostflags {.threadvar.}: SetHostflagsProc
    sslGet0Param {.threadvar.}: Get0ParamProc
    paramSet1IpAsc {.threadvar.}: Set1IpAscProc
    identityApiReady {.threadvar.}: bool

  proc tlsLib(pattern: string): LibHandle {.raises: [].} =
    ## The already-loaded libssl / libcrypto: `loadLibPattern` walks the same
    ## name list std/openssl bound to, so it returns that same handle. Wrapped
    ## because std/dynlib forward-declares `loadLib` without a raises annotation,
    ## which infers `Exception` and the strict async paths reject (see `certDer`);
    ## it only ever returns nil on failure.
    try: loadLibPattern(pattern)
    except Exception: nil

  proc tlsSym(lib: LibHandle, name: cstring): pointer {.raises: [].} =
    ## `lib`'s `name`, or nil when the library does not export it. Wrapped for
    ## the same reason as `tlsLib`.
    if lib.isNil: return nil
    try: lib.symAddr(name)
    except Exception: nil

  proc resolveIdentityApi() {.raises: [].} =
    ## Look the four identity-binding symbols up once per thread; they stay nil
    ## when the loaded library predates them.
    if identityApiReady: return
    identityApiReady = true
    let ssl = tlsLib(DLLSSLName)
    sslSet1Host = cast[Set1HostProc](ssl.tlsSym("SSL_set1_host"))
    sslSetHostflags = cast[SetHostflagsProc](ssl.tlsSym("SSL_set_hostflags"))
    sslGet0Param = cast[Get0ParamProc](ssl.tlsSym("SSL_get0_param"))
    paramSet1IpAsc = cast[Set1IpAscProc](
      tlsLib(DLLUtilName).tlsSym("X509_VERIFY_PARAM_set1_ip_asc"))
  when defined(windows):
    # std/openssl hides its X509_STORE type and DER helpers behind
    # `not defined(windows)` (like the X509 block below), so declare the ones the
    # caBundle / pinning / verify-callback paths use. PX509_STORE is an opaque
    # handle, like the other pointer aliases.
    type PX509_STORE = SslPtr
    proc X509_STORE_add_cert(store: PX509_STORE, x: PX509): cint
      {.cdecl, dynlib: DLLUtilName, importc.}
    proc i2d_X509(cert: PX509, o: ptr ptr uint8): cint
      {.cdecl, dynlib: DLLUtilName, importc.}
  # In-memory CA trust: reach the context's X509_STORE and add certs to it.
  proc SSL_CTX_get_cert_store(ctx: SslCtx): PX509_STORE
    {.cdecl, dynlib: DLLSSLName, importc.}
  # SPKI pinning: the peer's public key, DER-encoded as SubjectPublicKeyInfo.
  proc X509_get_pubkey(cert: PX509): EVP_PKEY
    {.cdecl, dynlib: DLLUtilName, importc.}
  proc i2d_PUBKEY(pkey: EVP_PKEY, o: ptr ptr uint8): cint
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

  type SkPopFreeProc =
    proc(st: PSTACK, freeFunc: pointer) {.cdecl, gcsafe, raises: [].}
  var skPopFree {.threadvar.}: SkPopFreeProc
  var skPopFreeReady {.threadvar.}: bool

  proc freeCertStack(ca: PSTACK) =
    ## Free a PKCS12_parse CA stack and the certificates it holds. Resolved by
    ## hand like the identity entry points: old LibreSSL does not export
    ## OPENSSL_sk_pop_free and a dynlib importc would abort the process at
    ## startup there. Without it the stack leaks once, on a path only an
    ## SSL_CTX_ctrl the library does not implement can reach.
    if ca.isNil: return
    if not skPopFreeReady:
      skPopFreeReady = true
      skPopFree = cast[SkPopFreeProc](
        tlsLib(DLLUtilName).tlsSym("OPENSSL_sk_pop_free"))
    if skPopFree.isNil: return
    skPopFree(ca, cast[pointer](X509_free))

  proc addChainCert(ctx: SslCtx, x: PX509): bool =
    ## Transfers ownership of `x` to `ctx` on success.
    SSL_CTX_ctrl(ctx, SSL_CTRL_EXTRA_CHAIN_CERT.cint, 0, cast[pointer](x)) > 0

  proc useCertChainPem(ctx: SslCtx, pem: string) =
    ## Install the leaf certificate, then any following certs as the chain.
    let bio = memBio(pem)
    defer: discard BIO_free(bio)
    let leaf = PEM_read_bio_X509(bio, nil, nil, nil)
    if leaf.isNil: fail("no certificate found in the PEM data")
    # SSL_CTX_use_certificate bumps the object's refcount, so we free our
    # reference on every exit path (success or failure) once the leaf exists.
    defer: X509_free(leaf)
    if SSL_CTX_use_certificate(ctx, leaf) != 1:
      fail("could not use the client certificate")
    while true:
      var extra = PEM_read_bio_X509(bio, nil, nil, nil)
      if extra.isNil: break                      # end of certs (or the key block)
      # addChainCert transfers ownership of `extra` to `ctx` on success, so we do
      # NOT free it then; on failure ownership stays with us, so we free it. The
      # defer covers both: null `extra` after a successful transfer so it is only
      # freed when the transfer failed (before `fail` unwinds this iteration).
      defer:
        if not extra.isNil: X509_free(extra)
      if addChainCert(ctx, extra):
        extra = nil
      else:
        fail("could not add an intermediate certificate")

  proc pemPassword(buf: cstring, size: cint, rwflag: cint, u: pointer): cint {.cdecl.} =
    ## PEM passphrase callback. `u` is the configured password as a C string, or
    ## nil when none is configured; then we return 0, which OpenSSL turns into
    ## PEM_R_BAD_PASSWORD_READ. Installing this unconditionally is the point: with
    ## a nil callback OpenSSL substitutes PEM_def_callback, which prompts for the
    ## passphrase on /dev/tty (falling back to stdin) and blocks there -- inside
    ## `newTlsContext`, so under asyncdispatch or chronos the whole event loop
    ## stops until someone types into a terminal that, in a service, is a pipe
    ## nobody is writing to.
    if u.isNil or size <= 0: return 0
    let pw = cast[cstring](u)
    let n = pw.len
    if n == 0 or n > size.int: return 0
    copyMem(buf, pw, n)
    n.cint

  proc useKeyPem(ctx: SslCtx, pem, password: string) =
    let bio = memBio(pem)
    defer: discard BIO_free(bio)
    # Never a nil callback: see pemPassword. `u` stays alive for the call, which
    # is the only time OpenSSL dereferences it.
    let u = if password.len > 0: cast[pointer](password.cstring) else: nil
    let pkey = PEM_read_bio_PrivateKey(bio, nil, cast[pointer](pemPassword), u)
    if pkey.isNil: fail("could not read the private key (wrong password?)")
    # SSL_CTX_use_PrivateKey bumps the object's refcount, so we free our
    # reference on every exit path (success or failure) once the key exists.
    defer: EVP_PKEY_free(pkey)
    if SSL_CTX_use_PrivateKey(ctx, pkey) != 1:
      fail("the private key does not match the certificate")

  proc usePkcs12(ctx: SslCtx, data, password: string) =
    ## Install the leaf certificate, the key, and any intermediates the bundle
    ## carries. The chain matters: a server that trusts only the root cannot build
    ## a path from a bare leaf issued by an intermediate CA, which is how a
    ## corporate `.p12` (root -> issuing CA -> client) is usually shaped.
    let bio = memBio(data)
    defer: discard BIO_free(bio)
    let p12 = d2i_PKCS12_bio(bio, nil)
    if p12.isNil: fail("could not parse the PKCS#12 bundle")
    defer: PKCS12_free(p12)
    var pkey: EVP_PKEY
    var cert: PX509
    var ca: PSTACK
    # PKCS12_parse hands us a fresh stack of the bundle's other certificates.
    # SSL_CTRL_CHAIN transfers it to the context; until then (and on every
    # failure exit) it is ours to free.
    var caOwned = true
    defer:
      if caOwned: freeCertStack(ca)
    if PKCS12_parse(p12, password.cstring, addr pkey, addr cert, addr ca) != 1:
      fail("could not decrypt the PKCS#12 bundle (wrong password?)")
    if cert.isNil or pkey.isNil: fail("the PKCS#12 bundle lacks a cert or key")
    # PKCS12_parse hands us fresh references to both the cert and the key.
    # SSL_CTX_use_certificate/PrivateKey bump their refcounts, so we free our
    # references on every exit path (success or failure) once they exist.
    defer: X509_free(cert)
    defer: EVP_PKEY_free(pkey)
    if SSL_CTX_use_certificate(ctx, cert) != 1:
      fail("could not use the PKCS#12 certificate")
    if SSL_CTX_use_PrivateKey(ctx, pkey) != 1:
      fail("the PKCS#12 key does not match the certificate")
    if not ca.isNil:
      # SSL_CTX_set0_chain: replaces the context's chain and takes ownership of
      # the stack and the certificates in it.
      if SSL_CTX_ctrl(ctx, SSL_CTRL_CHAIN.cint, 0, cast[pointer](ca)) <= 0:
        fail("could not install the PKCS#12 certificate chain")
      caOwned = false

  const pemBoundary = "-----BEGIN"

  proc isPem(data: string): bool =
    ## Whether `data` is armoured PEM: a `-----BEGIN` encapsulation boundary
    ## starts one of its lines. RFC 7468 section 5.2 allows explanatory text
    ## before that boundary and OpenSSL's PEM readers skip it, so the marker is
    ## not necessarily at offset 0 -- which is why the sniff below looks for the
    ## boundary instead of testing the first byte for the ASN.1 SEQUENCE tag
    ## (0x30, the ASCII digit '0'): a PEM file whose first character is '0' is
    ## still valid PEM and used to be misrouted to the DER loader (#436).
    if data.startsWith(pemBoundary): return true
    var nl = data.find('\n')
    while nl >= 0:
      if data.continuesWith(pemBoundary, nl + 1): return true
      nl = data.find('\n', nl + 1)
    false

  proc isDer(data: string): bool =
    ## Binary ASN.1: non-empty and carrying no PEM encapsulation boundary.
    data.len > 0 and not isPem(data)

  proc useCertFile(ctx: SslCtx, path: string) =
    # Cleansed on the way out even though a certificate is public: a single PEM
    # commonly holds the certificate AND its key (which is why `clientKeyFile`
    # falls back to `certFile`), so this buffer can be secret (issue #438).
    var data = readFile(path)
    defer: cleanse(data)
    if isDer(data):                           # single DER cert (no chain)
      if SSL_CTX_use_certificate_file(ctx, path.cstring, SSL_FILETYPE_ASN1) != 1:
        fail("could not load the DER certificate: " & path)
    else:
      useCertChainPem(ctx, data)              # PEM leaf + any following chain

  proc useKeyDer(ctx: SslCtx, der, password, path: string) =
    ## Install a DER private key. Three shapes reach here: a traditional RSA/EC
    ## key, an unencrypted PKCS#8 PrivateKeyInfo, and an encrypted PKCS#8
    ## EncryptedPrivateKeyInfo (`openssl pkcs8 -topk8 -outform DER -v2
    ## aes-256-cbc`). SSL_CTX_use_PrivateKey_file with SSL_FILETYPE_ASN1, which
    ## this replaces, decodes only the first two: it calls d2i_PrivateKey and
    ## never consults the passphrase callback, so an encrypted DER key failed
    ## with a generic error while `tls.password` was silently ignored (#436).
    var pkey: EVP_PKEY
    block:
      let plain = memBio(der)
      pkey = d2i_PrivateKey_bio(plain, nil)   # traditional or plain PKCS#8
      discard BIO_free(plain)
    if pkey.isNil:
      # Not an unencrypted key. The failed attempt left entries on this thread's
      # error queue, which later SSL_get_error reads rely on being empty.
      ErrClearError()
      let enc = memBio(der)
      # Never a nil callback: see pemPassword. Without one OpenSSL substitutes
      # PEM_def_callback, which prompts on /dev/tty and blocks the event loop.
      let u = if password.len > 0: cast[pointer](password.cstring) else: nil
      pkey = d2i_PKCS8PrivateKey_bio(enc, nil, cast[pointer](pemPassword), u)
      discard BIO_free(enc)
    if pkey.isNil:
      ErrClearError()
      if password.len > 0:
        fail("could not load the DER private key (wrong password?): " & path)
      fail("could not load the DER private key; set tls.password if it is an " &
           "encrypted PKCS#8 key: " & path)
    # SSL_CTX_use_PrivateKey bumps the object's refcount, so we free our
    # reference on every exit path once the key exists.
    defer: EVP_PKEY_free(pkey)
    if SSL_CTX_use_PrivateKey(ctx, pkey) != 1:
      fail("the DER private key does not match the certificate: " & path)

  proc useKeyFile(ctx: SslCtx, path, password: string) =
    # The key file's plaintext, zeroed before the buffer is freed: OpenSSL holds
    # the decoded key by now and nothing should be able to read the PEM/DER back
    # out of the heap (issue #438). The `defer` covers the failure paths too, and
    # deliberately wraps the whole body rather than each branch, so it keeps
    # covering a loader that decodes `data` itself instead of re-reading `path`.
    var data = readFile(path)
    defer: cleanse(data)
    if isDer(data):
      useKeyDer(ctx, data, password, path)    # DER, possibly encrypted PKCS#8
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
      # Named so it can be cleansed: a PKCS#12 bundle is the encrypted key, and
      # `readFile(...)` inline leaves that plaintext in a temporary navi never
      # gets to zero (issue #438).
      var p12 = readFile(tls.pkcs12File)
      defer: cleanse(p12)
      usePkcs12(ctx, p12, tls.password)
    elif tls.certPem.len > 0:
      useCertChainPem(ctx, tls.certPem)
      # Two calls rather than `useKeyPem(ctx, (if ...: tls.keyPem else: ...))`:
      # the `if` expression materialises a COPY of the key in a temporary, and a
      # temporary is precisely what cannot be cleansed. Passing the field itself
      # hands OpenSSL the one buffer the config already holds.
      if tls.keyPem.len > 0: useKeyPem(ctx, tls.keyPem, tls.password)
      else: useKeyPem(ctx, tls.certPem, tls.password)
    else:
      useCertFile(ctx, tls.certFile)
      useKeyFile(ctx, clientKeyFile(tls), tls.password)
    if SSL_CTX_check_private_key(ctx) != 1:
      fail("the client certificate and private key do not match")

  proc addCaBundle(ctx: SslCtx, pem: string) =
    ## Add every certificate in the in-memory PEM `pem` to the context's trust
    ## store, so a chain anchored at one of them verifies. Supplements the system
    ## roots / `caFile` rather than replacing them -- unlike `caFile`, which
    ## replaces the system roots (see `newTlsContext`), this is the additive way to
    ## trust an extra CA while public roots keep working.
    let store = SSL_CTX_get_cert_store(ctx)
    if store.isNil: fail("could not access the TLS trust store")
    let bio = memBio(pem)
    defer: discard BIO_free(bio)
    var added = 0
    while true:
      let cert = PEM_read_bio_X509(bio, nil, nil, nil)
      if cert.isNil: break
      # X509_STORE_add_cert bumps the object's refcount, so we free our reference
      # after handing it over; the defer covers this iteration's exit either way.
      defer: X509_free(cert)
      discard X509_STORE_add_cert(store, cert)
      inc added
    if added == 0: fail("no certificate found in TlsConfig.caBundle")

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

  const SSL_OP_NO_RENEGOTIATION = 0x40000000'u64
    ## Refuse renegotiation: OpenSSL then drops a server's TLS 1.2 HelloRequest
    ## with a `no_renegotiation` warning alert instead of starting a new
    ## handshake. TLS 1.3 has no renegotiation at all, and RFC 9113 9.2.1 forbids
    ## it for HTTP/2 regardless of version. navi never asks for one either
    ## (nothing calls SSL_renegotiate), so the only thing this removes is a
    ## peer-driven mid-connection handshake, which no navi backend can serve: on
    ## the chronos pump it makes SSL_write return WANT_READ, and that path cannot
    ## read the transport (chronos permits one pending read per transport, and the
    ## mux reader owns it), so it used to fail the whole connection (issue #444).
    ##
    ## This NUMBER is OpenSSL 1.1.0's and only OpenSSL 1.1.0's. The option-bit
    ## space is not shared across the libraries navi loads: OpenSSL 1.0.x spends
    ## 0x40000000 on SSL_OP_NETSCAPE_DEMO_CIPHER_CHANGE_BUG, and LibreSSL spends it
    ## on SSL_OP_NO_DTLSv1 while numbering its own SSL_OP_NO_RENEGOTIATION
    ## 0x00040000 (which is SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION on OpenSSL, so
    ## the two cannot be ORed in together either). `addCtxOptions` below is what
    ## keeps the bit off a library that would read it as something else.

  proc osslVersionNumber(): uint {.raises: [].} =
    ## `getOpenSSLVersion()` with its inferred `Exception` contained -- std/openssl
    ## forward-declares it without a raises annotation, which the strict paths here
    ## reject (the same reason `tlsLib` is wrapped). When the number cannot be read
    ## at all we answer 0x10100000 (OpenSSL 1.1.0), the era whose entry points this
    ## module hand-resolves; both callers then take their conservative branch on
    ## the symbol lookup instead.
    try: uint(getOpenSSLVersion())
    except Exception: 0x10100000'u

  proc isOpenSsl11OrNewer*(version: uint): bool =
    ## Whether `version` (an OPENSSL_VERSION_NUMBER, i.e. `getOpenSSLVersion()`)
    ## is a real OpenSSL 1.1.0-or-newer, the only library family whose option bits
    ## match the SSL_OP_* constants above. LibreSSL pins the number at 0x20000000
    ## whatever its real version -- which sorts ABOVE 1.1.0 and must not be taken
    ## for it -- and anything below 0x10100000 is OpenSSL 1.0.x or older. Exported
    ## logic kept separate from the FFI so it can be unit-tested for the libraries
    ## this host cannot install.
    version != 0x20000000'u and version >= 0x10100000'u

  type
    SetOptions64Proc =
      proc(ctx: SslCtx, op: uint64): uint64 {.cdecl, gcsafe, raises: [].}
        ## OpenSSL 3.x: `uint64_t SSL_CTX_set_options(SSL_CTX *, uint64_t)`.
    SetOptionsLongProc =
      proc(ctx: SslCtx, op: culong): culong {.cdecl, gcsafe, raises: [].}
        ## OpenSSL 1.1.x: `unsigned long SSL_CTX_set_options(SSL_CTX *, unsigned
        ## long)`. Identical to the above wherever `long` is 64 bits, and NOT
        ## interchangeable where it is 32: on AAPCS (32-bit ARM) and MIPS o32 a
        ## 64-bit argument is passed in an even-aligned register pair, so a callee
        ## expecting one word reads the wrong register and ORs garbage into the
        ## option mask -- which is where SSL_OP_LEGACY_SERVER_CONNECT,
        ## SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION and SSL_OP_NO_TLSv1_3 live.
  var ctxSetOptionsAddr {.threadvar.}: pointer
  var ctxSetOptionsReady {.threadvar.}: bool
  var ctxSetOptionsVersion {.threadvar.}: uint

  proc addCtxOptions(ctx: SslCtx, op: uint64) =
    ## OR `op` (an SSL_OP_* bit in OpenSSL 1.1.0+ numbering) into the context's
    ## option mask, or do nothing at all when the loaded library is not one whose
    ## mask uses that numbering.
    ##
    ## `SSL_CTX_set_options` is a real exported function only from OpenSSL 1.1.0
    ## on: OpenSSL 1.0.x and every LibreSSL define it as a macro over
    ## SSL_CTX_ctrl(SSL_CTRL_OPTIONS), so the symbol does not resolve there. That
    ## is not a case to fall back for, it is the case to skip: those are exactly
    ## the libraries that read our bits as different options (see
    ## SSL_OP_NO_RENEGOTIATION above), so the old ctrl fallback could only ever
    ## set the wrong flag. The runtime version is checked as well, in case a
    ## library ships a compatibility export while keeping its own numbering.
    ##
    ## Resolved by hand rather than with a `dynlib` importc for the reason
    ## `resolveIdentityApi` explains: an unresolved importc kills the process when
    ## this module initialises.
    if not ctxSetOptionsReady:
      ctxSetOptionsReady = true
      ctxSetOptionsVersion = osslVersionNumber()
      if isOpenSsl11OrNewer(ctxSetOptionsVersion):
        ctxSetOptionsAddr = tlsLib(DLLSSLName).tlsSym("SSL_CTX_set_options")
    if ctxSetOptionsAddr.isNil: return
    # OpenSSL 3.0 widened the parameter from `unsigned long` to `uint64_t`, so the
    # declaration has to follow the loaded library rather than be one compromise
    # for both (see SetOptionsLongProc). No SSL_OP_* navi sets lives above bit 31,
    # so the narrowing on a 32-bit `long` drops nothing.
    if ctxSetOptionsVersion >= 0x30000000'u:
      discard cast[SetOptions64Proc](ctxSetOptionsAddr)(ctx, op)
    else:
      discard cast[SetOptionsLongProc](ctxSetOptionsAddr)(ctx, culong(op))

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
    # A key with no certificate cannot be used, and handing it to `newContext`
    # would run it through std/net's SSL_CTX_use_PrivateKey_file, whose default
    # PEM callback prompts for the passphrase on /dev/tty and blocks (see
    # pemPassword). Reject the misconfiguration instead.
    if not custom and (cfg.keyFile.len > 0 or cfg.keyPem.len > 0):
      fail("TlsConfig has a client key but no certificate " &
           "(set certFile, certPem or pkcs12File)")
    # A non-empty caFile REPLACES the system roots: std/net's newContext calls
    # SSL_CTX_load_verify_locations(caFile) and takes the `else` branch that scans
    # the system store (`scanSSLCertificates`) only when caFile and caDir are both
    # empty. h3client.cpp mirrors that in `build_ssl_ctx`, whose ca_file branch
    # calls load_verify_locations and whose else branch calls
    # set_default_verify_paths, so the semantics are curl's --cacert on every
    # backend. caBundle is the additive option; see addCaBundle.
    result = newContext(
      verifyMode = if cfg.wantsVerify: CVerifyPeer else: CVerifyNone,
      certFile = if custom: "" else: cfg.certFile,
      keyFile = if custom: "" else: cfg.clientKeyFile,
      caFile = cfg.caFile)
    # Every step below can raise: a malformed or mismatched credential, a CA
    # bundle that will not parse, a version bound or a cipher name the loaded
    # library refuses. The caller is then handed an exception instead of a
    # context, so nothing else can ever free the SSL_CTX `newContext` just built
    # -- and that is not a small object: it carries the whole trust store the
    # verify locations loaded (measured at ~700 KB with the system roots). A
    # caller that retries after fixing its config, or one that probes a
    # credential per tenant, leaked one per attempt.
    var ok = false
    defer:
      if not ok: destroyContext(result)
    if custom: loadClientCert(result.context, cfg)
    if cfg.caBundle.len > 0: addCaBundle(result.context, cfg.caBundle)
    setAlpn(result.context, alpn)
    setVersionBounds(result.context, cfg)
    setCiphers(result.context, cfg)
    addCtxOptions(result.context, SSL_OP_NO_RENEGOTIATION)
    ok = true

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
      closed: bool
        ## Set by `close` and never cleared: the client is gone, so the cache
        ## takes no further session (see `offerSession`). It cannot simply be
        ## dropped instead, because a connection that was checked out rather than
        ## pooled (a live WebSocket or SSE stream, an in-flight h1 request) still
        ## holds its `SessionSlot` and can still be handed a TLS 1.3
        ## NewSessionTicket after the client was closed (issue #441).
    SessionSlot* = ref object
      ## Per-connection link from an SSL back to its cache and origin. Kept alive
      ## by the connection (its address lives in the SSL's ex_data), so the
      ## new-session callback can reach the cache while the connection is open.
      cache: TlsSessionCache
      origin: string
      rejected: bool
        ## Set by `rejectSession` when this connection's peer failed a
        ## post-handshake check (hostname/IP, SPKI pin, verify callback), cleared by
        ## `applySession` when a fresh SSL is bound to the slot. While set, no
        ## session from this connection is cached -- which is how a TLS 1.3 ticket
        ## that arrives AFTER the rejection is kept out of the cache (issue #440).

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
    SSL_CTRL_SET_SESS_CACHE_MODE = 44
    SSL_SESS_CACHE_CLIENT = 0x0001
    SSL_SESS_CACHE_NO_INTERNAL_STORE = 0x0200

  # --- allocating the ex_data index in the SSL class ----------------------
  #
  # `CRYPTO_get_ex_new_index` takes the *class* whose counter to draw from, and
  # the class numbering changed with OpenSSL 1.1.0: it put SSL at 0 (BIO moved to
  # 12), while OpenSSL 1.0.x -- and every LibreSSL, which inherited that header --
  # numbers BIO 0 and SSL 1. Drawing from the wrong counter is not a harmless
  # off-by-one: the returned number is then unregistered in the SSL class, so a
  # co-resident library that legitimately allocates an SSL-class index can be
  # handed the SAME number with its own dup/free callbacks attached, and
  # SSL_free / SSL_dup would invoke those on navi's `SessionSlot` pointer (issue
  # #439). Measured on the macOS LibreSSL (OPENSSL_VERSION_NUMBER 0x20000000):
  # SSL_get_ex_new_index hands out 0 then 1, CRYPTO_get_ex_new_index(0, ...)
  # returns 0 from the *BIO* counter, and CRYPTO_get_ex_new_index(1, ...) returns
  # 2 -- i.e. the class-0 call collides with SSL-class index 0.
  #
  # So: prefer the library's own `SSL_get_ex_new_index`, which knows its class
  # number. It is a real exported function exactly where the numbering differs
  # (OpenSSL 1.0.x, LibreSSL) and a macro over CRYPTO_get_ex_new_index from
  # OpenSSL 1.1.0 on, so when it does not resolve we pick the class from the
  # runtime version instead. It is looked up by hand, not with a `dynlib` importc,
  # for the reason `resolveIdentityApi` explains: an unresolved importc kills the
  # process at module init.

  const
    CRYPTO_EX_INDEX_SSL_MODERN = 0.cint
      ## CRYPTO_EX_INDEX_SSL from OpenSSL 1.1.0 on (that header numbers BIO 12).
    CRYPTO_EX_INDEX_SSL_LEGACY = 1.cint
      ## CRYPTO_EX_INDEX_SSL on OpenSSL 1.0.x and every LibreSSL, where
      ## CRYPTO_EX_INDEX_BIO is 0 and the SSL class follows it.

  proc sslExIndexClass*(version: uint): cint =
    ## The `CRYPTO_EX_INDEX_SSL` class number for the library whose
    ## OPENSSL_VERSION_NUMBER is `version` (i.e. `getOpenSSLVersion()`). LibreSSL
    ## pins that number at 0x20000000 whatever its real version, which is how
    ## std/net's `newContext` recognises it too; anything below OpenSSL 1.1.0
    ## (0x10100000) predates the renumbering. Exported so the selection can be
    ## unit-tested for libraries this host cannot run.
    if version == 0x20000000'u: CRYPTO_EX_INDEX_SSL_LEGACY      # LibreSSL, any version
    elif version < 0x10100000'u: CRYPTO_EX_INDEX_SSL_LEGACY     # OpenSSL 1.0.x and older
    else: CRYPTO_EX_INDEX_SSL_MODERN                            # OpenSSL 1.1.0+

  type SslGetExNewIndexProc = proc(argl: clong, argp: pointer,
    newf, dupf, freef: pointer): cint {.cdecl, gcsafe, raises: [].}
  var sslGetExNewIndex {.threadvar.}: SslGetExNewIndexProc
  var sslGetExNewIndexReady {.threadvar.}: bool

  proc newSslExIndex*(): cint {.raises: [].} =
    ## Allocate an ex_data index in the SSL class, through the library's own
    ## `SSL_get_ex_new_index` when it exports one and otherwise through
    ## `CRYPTO_get_ex_new_index` with the version-selected class.
    ##
    ## Exported so the allocation itself can be unit-tested against whatever
    ## library the suite links: `sslExIndexClass` covers the fallback's decision,
    ## but on the libraries where the numbering actually differs the export
    ## resolves and the fallback never runs, so the class constant is not what is
    ## under test there. Each call consumes one index, as OpenSSL's own API does.
    if not sslGetExNewIndexReady:
      sslGetExNewIndexReady = true
      sslGetExNewIndex = cast[SslGetExNewIndexProc](
        tlsLib(DLLSSLName).tlsSym("SSL_get_ex_new_index"))
    if not sslGetExNewIndex.isNil:
      return sslGetExNewIndex(0, nil, nil, nil, nil)
    CRYPTO_get_ex_new_index(sslExIndexClass(osslVersionNumber()),
                            0, nil, nil, nil, nil)

  # Per-thread (threadvar): each thread registers its own ex-data index and uses it
  # for the SSL objects it owns, so the lazy init never races across threads (works
  # under plain orc with one navi client per thread). threadvars can't carry an
  # initializer, so a companion flag stands in for the old `-1` "unset" sentinel.
  var slotExIdx {.threadvar.}: cint
  var slotExIdxReady {.threadvar.}: bool
  proc ensureExIdx() =
    if not slotExIdxReady:
      slotExIdx = newSslExIndex()
      slotExIdxReady = true

  proc offerSession*(slot: SessionSlot, session: pointer): bool =
    ## Offer `session` (an SSL_SESSION OpenSSL is handing over) to `slot`'s cache,
    ## under `slot`'s origin, freeing whatever was cached there before. Returns true
    ## when the cache TOOK OWNERSHIP, which is what the new-session callback reports
    ## to OpenSSL as 1; false leaves ownership with OpenSSL, which then frees the
    ## session itself.
    ##
    ## Ownership is declined when the cache has been closed: the client is gone and
    ## nothing will ever free the table again, so a session inserted now would leak
    ## until a second `close` that never comes (issue #441). This is reachable
    ## precisely because a connection that was checked out rather than pooled -- a
    ## live WebSocket or SSE stream, an in-flight h1 request -- outlives
    ## `client.close()` and can still receive a TLS 1.3 NewSessionTicket.
    ##
    ## Ownership is also declined once this connection's peer has been rejected by a
    ## post-handshake check (issue #440): for TLS <= 1.2 the callback fires inside the
    ## handshake, before those checks run, but under TLS 1.3 the ticket arrives during
    ## the first reads and can therefore land after the rejection.
    ##
    ## Split out of the callback so the whole insert policy is ordinary Nim that can
    ## be unit-tested; `onNewSession` is just the C shim over it.
    if slot.isNil or slot.cache.isNil or session.isNil: return false
    if slot.cache.closed or slot.rejected: return false
    let prev = slot.cache.sessions.getOrDefault(slot.origin, nil)
    if not prev.isNil: SSL_SESSION_free(prev)
    slot.cache.sessions[slot.origin] = session
    true

  proc onNewSession(ssl: SslPtr, session: pointer): cint {.cdecl.} =
    ## Called by OpenSSL when a resumable session becomes available. `offerSession`
    ## decides whether we take ownership; 1 says we did, 0 leaves it with OpenSSL.
    {.cast(gcsafe).}:
      let p = SSL_get_ex_data(ssl, slotExIdx)
      if p.isNil: return 0
      if offerSession(cast[SessionSlot](p), session): 1.cint else: 0.cint

  proc rejectSession*(slot: SessionSlot) {.raises: [].} =
    ## This connection's peer failed a post-handshake check, so nothing of its
    ## session may stay cached for the origin (issue #440). Two things are needed,
    ## because the callback fires at different times either side of TLS 1.3:
    ##
    ##   * remove and free the origin's cached entry. For TLS <= 1.2 the session was
    ##     already stored during `SSL_connect`, before `verifyPeer` /
    ##     `postHandshakeVerify` could run, so only an eviction can undo it.
    ##   * mark the slot, so a TLS 1.3 NewSessionTicket that arrives on this
    ##     connection AFTER the rejection is declined by `offerSession` instead of
    ##     re-populating the entry we just dropped.
    ##
    ## Without this, the next connect to the origin would present a session bound to
    ## a peer navi refused (an interception proxy with a chain-valid but unpinned
    ## certificate, say) and keep that peer's session, with its certificate, in
    ## memory. There is no verification bypass either way -- on resumption OpenSSL
    ## restores verify_result and the peer certificate, so the same check fails again
    ## -- but the client should not be advertising it, and a real server that does not
    ## know the session pays a pointless round of resumption before falling back to a
    ## full handshake.
    ##
    ## No-op for a nil slot (resumption off, or no session cache), so call sites need
    ## no guard.
    if slot.isNil or slot.cache.isNil: return
    slot.rejected = true
    let s = slot.cache.sessions.getOrDefault(slot.origin, nil)
    if not s.isNil:
      slot.cache.sessions.del(slot.origin)
      SSL_SESSION_free(s)

  proc isRejected*(slot: SessionSlot): bool =
    ## Whether `rejectSession` has marked this connection's peer as refused. For
    ## tests and introspection.
    not slot.isNil and slot.rejected

  proc newTlsSessionCache*(): TlsSessionCache =
    TlsSessionCache(sessions: initTable[string, pointer]())

  proc isClosed*(cache: TlsSessionCache): bool =
    ## Whether `close` has been called on `cache`. A closed cache is never reopened:
    ## `newTlsStore` mints a fresh one per client instead.
    not cache.isNil and cache.closed

  proc sessionCount*(cache: TlsSessionCache): int =
    ## Cached sessions (one per origin at most). For tests and introspection.
    if cache.isNil: 0 else: cache.sessions.len

  proc hasSession*(cache: TlsSessionCache, origin: string): bool =
    ## Whether a resumable session is cached for `origin` ("host:port"). For tests
    ## and introspection.
    not cache.isNil and not cache.sessions.getOrDefault(origin, nil).isNil

  proc close*(cache: TlsSessionCache) =
    ## Free every cached session and mark the cache closed, so a late ticket on a
    ## connection that outlived the client is declined rather than inserted into a
    ## table nothing will free again (issue #441). Call when the client is closed.
    ## Idempotent.
    if cache.isNil: return
    cache.closed = true   # set FIRST: nothing may land in the table after this
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
    ##
    ## Clears any `rejectSession` mark: the mark belongs to the SSL that was refused,
    ## and one slot can be reused for a second SSL when `connectAcross` drops a
    ## verification-failing address and re-races the remaining ones. Without the
    ## reset, a pool whose first address is broken would stop caching sessions for
    ## the address that actually worked.
    if slot.isNil: return
    ensureExIdx()
    slot.rejected = false
    discard SSL_set_ex_data(ssl, slotExIdx, cast[pointer](slot))
    if slot.cache.isNil or slot.cache.closed: return
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

  proc contextCount*(store: TlsContextStore): int =
    ## Shared contexts built so far, i.e. how many ALPN shapes this client has
    ## dialled. For tests and introspection, in the spirit of `sessionCount` and
    ## `h3CtxCacheStats`; `prebuildContexts` brings it to `naviAlpnShapes.len`.
    if store.isNil: 0 else: store.contexts.len

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

  const naviAlpnShapes*: array[2, seq[string]] = [@[], @["h2", "http/1.1"]]
    ## Every ALPN shape navi's engines offer, and the complete key set of a
    ## `TlsContextStore`: `@["h2", "http/1.1"]` for an https target when h2 is
    ## enabled, and none at all otherwise (plain http, h2 disabled, or a WebSocket
    ## over an h1 Upgrade). Every `connect` call in navi passes one of these two.

  proc prebuildContexts*(store: RootRef, cfg: TlsConfig) =
    ## Build and cache the shared context for BOTH `naviAlpnShapes`, so no later
    ## connect has to build one. That is what lets the credential be wiped out of
    ## a live client (`clearTlsSecrets`, issue #438): contexts are normally built
    ## lazily, on the first connect of each shape, and a context built after the
    ## wipe would have no key material to install.
    ##
    ## A no-op without a store: a bare `TlsConfig` builds a context the caller
    ## owns per connect, so there is nothing to build ahead of time. Raises
    ## whatever the credential loader raises (`ValueError`, or an `IOError` for an
    ## unreadable file), here rather than at the first connect.
    if store.isNil: return
    for alpn in naviAlpnShapes:
      discard obtainContext(store, cfg, alpn)

  # --- per-connection handshake ------------------------------------------

  proc checkCertName(ssl: SslPtr, host: string) =
    ## Match the peer certificate's SAN (or, for a SAN-less certificate, its
    ## subject CN) against `host` with X509_check_host. Redundant once
    ## `bindExpectedIdentity` has bound the same name into the handshake, and kept
    ## as the belt-and-braces check. Callers skip it for IP literals:
    ## X509_check_host matches DNS names, `checkCertIp` matches iPAddress SANs.
    let cert = SSL_get_peer_certificate(ssl)
    if cert.isNil: fail("server presented no certificate")
    let match = X509_check_host(cert, host.cstring, host.len.cint,
                                X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS, nil)
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

  proc bindExpectedIdentity(ssl: SslPtr, host: string, verify: bool): bool =
    ## Bind the identity we expect the peer to prove into the SSL's
    ## X509_VERIFY_PARAM *before* the handshake: the DNS name through
    ## SSL_set1_host (with the shared host-check flags) or, for an IP literal,
    ## the address through X509_VERIFY_PARAM_set1_ip_asc. OpenSSL then folds the
    ## identity check into its in-handshake verification, so a mismatched peer is
    ## rejected before the client sends its Certificate/CertificateVerify -- with
    ## the post-handshake check alone, an mTLS client disclosed its identity to
    ## any chain-valid impostor. `verifyPeer` still re-checks afterwards, so on a
    ## library too old to offer these entry points we simply keep that check (the
    ## identity is then enforced one flight later, as it was before). A no-op
    ## (true) when verification is off.
    ##
    ## Verification ON with no host is a failure (#435): the request must fail
    ## before the handshake rather than after the client has presented its
    ## certificate to an unauthenticated peer. Both constructors below raise it
    ## BEFORE `SSL_new`, so the check here can no longer fire -- it is kept as the
    ## local invariant for anything that grows a third caller, and deliberately
    ## sits ahead of every allocation this proc makes (none today).
    requireVerifiableHost(host, verify)
    if not verify: return true
    resolveIdentityApi()
    if isIpAddress(host):
      if paramSet1IpAsc.isNil or sslGet0Param.isNil: return true
      let param = sslGet0Param(ssl)
      if param.isNil: return false
      return paramSet1IpAsc(param, host.cstring) == 1
    if sslSet1Host.isNil or sslSetHostflags.isNil: return true
    sslSetHostflags(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS)
    sslSet1Host(ssl, host.cstring) == 1

  proc newClientSsl*(ctx: SslContext, fd: SocketHandle, host: string,
                     verify: bool, slot: SessionSlot = nil): SslPtr =
    ## Create a client SSL bound to `fd`, set SNI (DNS-name hosts only, as
    ## std/net does), bind the expected peer identity when `verify` is on, and
    ## present any cached session for resumption. The caller drives the handshake
    ## (blocking in the sync backend, await-based in the async one) and frees the
    ## SSL on failure.
    ##
    ## The "verify on, no host" refusal (#435) runs first, before anything is
    ## allocated: `bindExpectedIdentity` raises it, and raising it from there would
    ## abandon the SSL this proc had already created (the caller only frees what it
    ## was returned, and it is returned nothing).
    requireVerifiableHost(host, verify)
    result = SSL_new(ctx.context)
    if result.isNil: fail("SSL_new failed")
    discard SSL_set_fd(result, fd)
    applySession(result, slot)   # present a cached session before the handshake
    if host.len > 0 and not isIpAddress(host):
      discard SSL_set_tlsext_host_name(result, host.cstring)   # SNI
    if not bindExpectedIdentity(result, host, verify):
      SSL_free(result)
      fail("could not require the certificate to match " & host)

  proc newClientSslMem*(ctx: SslContext, host: string, verify: bool,
                        slot: SessionSlot = nil): tuple[ssl: SslPtr, rbio, wbio: BIO] =
    ## Create a client SSL driven through a pair of memory BIOs instead of a
    ## socket fd, for an async backend that owns the ciphertext transport itself
    ## (chronos over a `StreamTransport`). Sets SNI, binds the expected peer
    ## identity when `verify` is on, presents any cached session, and puts the SSL
    ## in connect state. The caller drives the handshake by feeding `rbio`
    ## (ciphertext in) and draining `wbio` (ciphertext out), then runs
    ## `verifyPeer`. `SSL_set_bio` transfers BIO ownership to the SSL, so the
    ## returned `rbio`/`wbio` are for pumping only -- freeing the SSL frees them.
    ##
    ## As in `newClientSsl`, the "verify on, no host" refusal (#435) runs before
    ## anything is allocated: raised from `bindExpectedIdentity` it would abandon
    ## the SSL and both memory BIOs, since the caller is handed nothing to free.
    requireVerifiableHost(host, verify)
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
    if not bindExpectedIdentity(ssl, host, verify):
      SSL_free(ssl)
      fail("could not require the certificate to match " & host)
    sslSetConnectState(ssl)
    (ssl, rbio, wbio)

  proc verifyPeer*(ssl: SslPtr, host: string, verify: bool,
                   slot: SessionSlot = nil) =
    ## After a completed handshake, confirm the chain and the certificate
    ## identity. Both are already enforced during the handshake (SSL_VERIFY_PEER
    ## for the chain, `bindExpectedIdentity` for the name), so this is the
    ## belt-and-suspenders repeat: the SAN/CN for a DNS host, or the iPAddress SAN
    ## for an IP literal. No-op when `verify` is off. Raises `ValueError` on
    ## mismatch.
    ##
    ## An empty `host` with verification on is a failure, not a licence to check
    ## only the chain (#435): it would accept any certificate issued by a trusted
    ## CA for whatever answered on the socket. `bindExpectedIdentity` already
    ## refuses it before the handshake; this keeps the invariant local, for a
    ## caller that drives its own SSL and only reaches us here.
    ##
    ## Pass the connection's `slot` so a rejection also drops whatever session this
    ## peer got cached for the origin, and keeps a late TLS 1.3 ticket out of it
    ## (`rejectSession`, issue #440). Optional only so the interop tests can call it
    ## without a cache.
    if not verify: return
    requireVerifiableHost(host, verify)
    try:
      if SSL_get_verify_result(ssl) != X509_V_OK:
        fail("certificate verification failed for " & host)
      if isIpAddress(host): checkCertIp(ssl, host)   # match the iPAddress SAN
      else: checkCertName(ssl, host)
    except CatchableError:
      rejectSession(slot)
      raise

  proc certDer(cert: PX509): string =
    ## DER encoding of `cert`, via the pointer-form i2d_X509 (std/openssl's string
    ## wrapper is inferred to raise `Exception`, which the strict async paths reject).
    let n = i2d_X509(cert, nil)
    if n <= 0: return ""
    result = newString(n)
    var p = cast[ptr uint8](addr result[0])
    if i2d_X509(cert, addr p) <= 0: return ""

  proc peerSpkiPin(ssl: SslPtr): string =
    ## Base64 SHA-256 of the peer certificate's SubjectPublicKeyInfo -- the HPKP
    ## pin form (`openssl ... | openssl dgst -sha256 -binary | base64`). "" when the
    ## peer presented no certificate or its key could not be encoded.
    let cert = SSL_get_peer_certificate(ssl)
    if cert.isNil: return ""
    defer: X509_free(cert)
    let pkey = X509_get_pubkey(cert)
    if pkey.isNil: return ""
    defer: EVP_PKEY_free(pkey)
    let n = i2d_PUBKEY(pkey, nil)
    if n <= 0: return ""
    var der = newString(n)
    var p = cast[ptr uint8](addr der[0])
    if i2d_PUBKEY(pkey, addr p) <= 0: return ""
    var h = initSha_256()
    h.update(der)
    base64.encode(h.digest())

  proc postHandshakeVerify*(ssl: SslPtr, host: string, cfg: TlsConfig,
                            slot: SessionSlot = nil)
      {.raises: [CatchableError].} =
    ## Extra peer checks after `verifyPeer`'s chain + hostname verification: SPKI
    ## pinning and the user's verify callback. Both run even when `verify` is off,
    ## so an app that disables chain checking can still pin or inspect the leaf. A
    ## no-op when neither is configured. Raises `ValueError` on rejection.
    ##
    ## Pass the connection's `slot` so a rejection also drops whatever session this
    ## peer got cached for the origin, and keeps a late TLS 1.3 ticket out of it
    ## (`rejectSession`, issue #440). Optional only so the interop tests can call it
    ## without a cache.
    try:
      if cfg.pinnedKeys.len > 0:
        let pin = peerSpkiPin(ssl)
        if pin.len == 0 or pin notin cfg.pinnedKeys:
          fail("certificate public key does not match any pin for " & host)
      if cfg.verifyCallback != nil:
        let cert = SSL_get_peer_certificate(ssl)
        if cert.isNil: fail("server presented no certificate")
        let der = certDer(cert)
        X509_free(cert)
        if not cfg.verifyCallback(der):
          fail("the verify callback rejected the certificate for " & host)
    except CatchableError:
      rejectSession(slot)
      raise

  type TlsWait* = proc(fd: SocketHandle, forWrite: bool): bool {.closure, gcsafe,
                                                                raises: [CatchableError].}
    ## Readiness wait that lets `startClientTls` bound a handshake by WALL CLOCK.
    ## Returns true once `fd` is readable (or writable, when `forWrite`), false when
    ## the caller's budget lapsed; it may also raise the caller's own timeout
    ## instead. A blocking socket cannot express this on its own: SO_RCVTIMEO bounds
    ## each recv, and a peer trickling one byte just inside every window restarts it
    ## indefinitely, so the handshake outlives the connect budget (issue #452).

  proc startClientTls*(ctx: SslContext, fd: SocketHandle, host: string,
                       verify: bool, slot: SessionSlot = nil,
                       wait: TlsWait = nil): SslPtr =
    ## Blocking client handshake (sync backend): bind + SNI + expected identity +
    ## resume, drive the
    ## handshake, verify. Replaces std/net's `wrapConnectedSocket` so SNI and the
    ## verified hostname stay under navi's control. Raises `ValueError` on failure;
    ## `negotiatedProtocol(result)` reads the ALPN. The async backend composes
    ## `newClientSsl` + an await-based handshake + `verifyPeer` itself.
    ##
    ## With `wait` supplied the socket is switched non-blocking for the handshake
    ## and every would-block is spent in `wait`, so the whole exchange is bounded by
    ## the caller's single deadline rather than by a per-recv socket timeout; the
    ## socket is restored to blocking before returning either way.
    result = newClientSsl(ctx, fd, host, verify, slot)
    var ok = false
    defer:
      if not ok: SSL_free(result)
    if wait.isNil:
      ErrClearError()   # SSL_get_error / the error text below are only reliable on an
                        # empty per-thread queue (see the backends' read loops)
      if SSL_connect(result) != 1:
        fail("TLS handshake failed for " & host)
    else:
      fd.setBlocking(false)
      defer: fd.setBlocking(true)   # every other path here expects a blocking socket
      while true:
        ErrClearError()             # see the comment above: per-thread error queue
        let r = SSL_connect(result)
        if r == 1: break
        let err = SSL_get_error(result, r)
        if err == SSL_ERROR_WANT_READ or err == SSL_ERROR_WANT_WRITE:
          # `wait` normally raises the caller's TimeoutError when the budget is
          # spent; a plain `false` (a bare readiness wait that just expired) is the
          # same verdict, so report it rather than spinning.
          if not wait(fd, err == SSL_ERROR_WANT_WRITE):
            fail("TLS handshake timed out for " & host)
        else:
          fail("TLS handshake failed for " & host)
    verifyPeer(result, host, verify, slot)   # a rejection also evicts the session
    ok = true
