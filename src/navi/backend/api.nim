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
    verify*: bool     ## verify the server certificate chain and hostname
    caFile*: string   ## custom CA bundle path; "" uses the system trust store

  ProxyTarget* = object
    ## The HTTP proxy to dial through. An empty `host` means a direct
    ## connection. For https targets the backend issues a CONNECT tunnel.
    host*: string
    port*: int

proc defaultTls*(): TlsConfig =
  TlsConfig(verify: true)

proc direct*(): ProxyTarget = ProxyTarget()
proc isSet*(p: ProxyTarget): bool = p.host.len > 0
