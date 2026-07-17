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

import std/options

type
  TlsConfig* = object
    verify*: Option[bool]  ## verify the cert chain and hostname; unset means on
    caFile*: string        ## custom CA bundle path; "" uses the system trust store
    certFile*: string      ## client certificate (PEM) for mTLS; "" presents none
    keyFile*: string       ## private key (PEM) for `certFile`; "" reuses certFile

  ProxyTarget* = object
    ## The HTTP proxy to dial through. An empty `host` means a direct
    ## connection. For https targets the backend issues a CONNECT tunnel.
    host*: string
    port*: int

proc wantsVerify*(tls: TlsConfig): bool = tls.verify.get(true)
  ## Certificate verification is on unless explicitly disabled. This is secure
  ## by default even when a TlsConfig (or the NaviOptions around it) is built
  ## partially, leaving `verify` unset.

proc clientKeyFile*(tls: TlsConfig): string =
  ## Path to the client private key: `keyFile` when set, otherwise `certFile`
  ## (a single PEM commonly holds both the certificate and its key).
  if tls.keyFile.len > 0: tls.keyFile else: tls.certFile

proc defaultTls*(): TlsConfig =
  TlsConfig()  # verify defaults on via wantsVerify

proc direct*(): ProxyTarget = ProxyTarget()
proc isSet*(p: ProxyTarget): bool = p.host.len > 0
