## Per-client TLS session and context stores, shared by every native backend.
##
## The session cache (abbreviated resumption) and the shared SSL_CTX store live on
## `config.tls.sessionCache` / `config.tls.contextStore`; an entry mints them in
## `newNavi` and frees them in `close`. Their construction depends only on
## `openssl_ctx` and `TlsConfig`, nothing backend-specific, so the sync,
## asyncdispatch, and chronos backends `import` and re-export this instead of each
## carrying an identical copy. Everything is a no-op on a non-`-d:ssl` build.

import ./api
when defined(ssl):
  import ./openssl_ctx

proc newTlsStore*(cfg: TlsConfig): RootRef =
  ## The per-client TLS session cache, or nil when resumption is off or on a
  ## non-`-d:ssl` build. The entry puts it on `config.tls.sessionCache`.
  when defined(ssl):
    if cfg.wantsResume: result = newTlsSessionCache()
  else:
    discard cfg

proc closeTlsStore*(store: RootRef) =
  ## Free the sessions held by a `newTlsStore` cache. Entries call this in `close`.
  when defined(ssl):
    if not store.isNil: close(cast[TlsSessionCache](store))
  else:
    discard store

proc newTlsCtxStore*(cfg: TlsConfig): RootRef =
  ## The per-client shared TLS-context store (empty until the first TLS connect),
  ## or nil on a non-`-d:ssl` build. The entry puts it on `config.tls.contextStore`.
  when defined(ssl):
    result = newTlsContextStore()
  else:
    discard cfg

proc closeTlsCtxStore*(store: RootRef) =
  ## Free the shared contexts held by a `newTlsCtxStore`. Entries call this in
  ## `close`, after `closeIdle` has shut the pooled connections.
  when defined(ssl):
    if not store.isNil: close(cast[TlsContextStore](store))
  else:
    discard store

when defined(ssl):
  proc resumeSlot*(cfg: TlsConfig, origin: string): SessionSlot =
    ## When resumption is on and the client has a session cache, return a slot keyed
    ## by `origin`; otherwise nil. The context itself is armed once in
    ## `obtainContext`, so this only mints the per-connection link.
    if cfg.wantsResume and not cfg.sessionCache.isNil:
      result = newSlot(cast[TlsSessionCache](cfg.sessionCache), origin)
