## Shared ALPN helpers for the OpenSSL-based backends (sync, asyncdispatch).
## Empty unless compiled with -d:ssl.

when defined(ssl):
  import std/openssl

  proc setAlpn*(ctx: SslCtx, protos: openArray[string]) =
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
