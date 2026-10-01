## The "verify on, no host" refusal must happen before an SSL exists (#435).
##
## With verification on and an empty host there is no identity to check the
## certificate against, so `newClientSsl` / `newClientSslMem` refuse to build one.
## That refusal used to be raised from `bindExpectedIdentity`, which both
## constructors call AFTER `SSL_new` -- and on that path they return nothing, so the
## SSL (plus, in the memory-BIO variant, the two BIOs it had taken ownership of) was
## abandoned with no owner: a middleware rewriting `ctx.req.url` to an authority-less
## URL leaked one SSL per request.
##
## The ordering is observable without a server, through a context whose SSL_CTX is
## NULL: `SSL_new(NULL)` is a documented failure on every library navi loads (it
## returns NULL and queues SSL_R_NULL_SSL_CTX), so WHICH of the two checks ran first
## decides the message. "SSL_new failed" means the SSL was created first.
import unittest
import std/[net, openssl, nativesockets]
from std/strutils import contains
import navi/backend/api
import navi/backend/openssl_ctx

suite "the identity precondition runs before the SSL is created (#435)":
  test "newClientSsl should refuse an empty host before touching the context":
    var msg = ""
    try:
      discard newClientSsl(SslContext(), SocketHandle(-1), "", verify = true)
    except ValueError as e:
      msg = e.msg
    check "no hostname to verify against" in msg

  test "newClientSslMem should refuse an empty host before touching the context":
    var msg = ""
    try:
      discard newClientSslMem(SslContext(), "", verify = true)
    except ValueError as e:
      msg = e.msg
    check "no hostname to verify against" in msg

  test "verification off should still allow an empty host":
    # `insecureSkipVerify` is the explicit opt-out the Unix-socket and raw-fd paths
    # use: there is no identity to check, so an empty host stays legal and the
    # constructor must get as far as really building an SSL.
    let ctx = newTlsContext(defaultTls())
    let ssl = newClientSslMem(ctx, "", verify = false).ssl
    check not ssl.isNil
    SSL_free(ssl)
    destroyContext(ctx)

  test "a host with verification on should still build an SSL":
    # The counterpart, so the refusals above cannot pass by refusing everything.
    let ctx = newTlsContext(defaultTls())
    let ssl = newClientSslMem(ctx, "example.com", verify = true).ssl
    check not ssl.isNil
    SSL_free(ssl)
    destroyContext(ctx)
