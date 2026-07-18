## Build a chronos/BearSSL trust-anchor store from a PEM CA bundle, so the
## chronos backend can verify a server against a custom CA (TlsConfig.caFile).
##
## BearSSL ships no PEM-to-trust-anchor loader, so this ports
## `certificate_to_trust_anchor` from BearSSL's `tools/certs.c`: decode each CA
## certificate and extract its subject DN plus public key. The extracted bytes
## live in `CaTrustStore.backing`; BearSSL keeps raw pointers into them, so the
## returned store must outlive every connection that uses it.

import pkg/bearssl/x509
import pkg/chronos/streams/tlsstream  # pemDecode, PEMElement, TrustAnchorStore

type
  CaTrustStore* = ref object
    store*: TrustAnchorStore   ## pass to newTLSClientAsyncStream(trustAnchors = ...)
    backing: seq[seq[byte]]    ## DN + key bytes the anchors point into; keep alive

proc dnAppend(ctx: pointer; buf: pointer; length: csize_t) {.cdecl.} =
  ## BearSSL calls this during decoding to hand us the raw subject DN, in one or
  ## more chunks. `ctx` is the `seq[byte]` accumulator we passed to the decoder.
  let n = int(length)
  if n <= 0: return
  let acc = cast[ptr seq[byte]](ctx)
  let start = acc[].len
  acc[].setLen(start + n)
  copyMem(addr acc[][start], buf, n)

proc keep(cs: CaTrustStore, src: ptr byte, len: uint): ptr byte =
  ## Copy `len` bytes into a store-owned buffer and return a stable pointer to it.
  ## Growing `backing` later moves the seq headers but not their payloads, so
  ## pointers handed out here stay valid for the store's lifetime.
  if len == 0: return nil
  var b = newSeq[byte](int(len))
  copyMem(addr b[0], src, int(len))
  cs.backing.add(b)
  addr cs.backing[^1][0]

proc toAnchor(cs: CaTrustStore, der: seq[byte]): X509TrustAnchor =
  var dn: seq[byte]
  var dc: X509DecoderContext
  x509DecoderInit(dc, dnAppend, addr dn)
  if der.len > 0:
    x509DecoderPush(dc, unsafeAddr der[0], csize_t(der.len))
  let pk = x509DecoderGetPkey(dc)
  if pk == nil:
    raise newException(IOError, "navi: could not decode CA certificate (bearssl error " &
                                $x509DecoderLastError(dc) & ")")
  cs.backing.add(dn)
  result.dn.data = addr cs.backing[^1][0]
  result.dn.len = uint(dn.len)
  result.flags = if x509DecoderIsCA(dc) != 0: cuint(X509_TA_CA) else: 0
  case int(pk.keyType)
  of KEYTYPE_RSA:
    result.pkey.keyType = byte(KEYTYPE_RSA)
    result.pkey.key.rsa.n = cs.keep(pk.key.rsa.n, pk.key.rsa.nlen)
    result.pkey.key.rsa.nlen = pk.key.rsa.nlen
    result.pkey.key.rsa.e = cs.keep(pk.key.rsa.e, pk.key.rsa.elen)
    result.pkey.key.rsa.elen = pk.key.rsa.elen
  of KEYTYPE_EC:
    result.pkey.keyType = byte(KEYTYPE_EC)
    result.pkey.key.ec.curve = pk.key.ec.curve
    result.pkey.key.ec.q = cs.keep(pk.key.ec.q, pk.key.ec.qlen)
    result.pkey.key.ec.qlen = pk.key.ec.qlen
  else:
    raise newException(IOError, "navi: unsupported CA public key type (not RSA or EC)")

proc loadCaTrustStore*(caPem: string): CaTrustStore =
  ## Parse a PEM CA bundle (one or more certificates) into a trust-anchor store.
  ## Raises IOError if no certificate is found or one fails to decode.
  result = CaTrustStore()
  var anchors: seq[X509TrustAnchor]
  for el in pemDecode(caPem):
    if el.name in ["CERTIFICATE", "X509 CERTIFICATE", "TRUSTED CERTIFICATE"]:
      anchors.add(result.toAnchor(el.data))
  if anchors.len == 0:
    raise newException(IOError, "navi: no certificates found in CA file")
  result.store = TrustAnchorStore.new(anchors)
