## Partial-wildcard rejection, sync backend. Driven by tests/interop/ca_verify.sh,
## which serves a certificate carrying both a partial wildcard SAN
## (`fo*.example.com`, forbidden by RFC 6125 6.4.4 / RFC 9525) and an ordinary
## leftmost wildcard (`*.wild.example.com`). OpenSSL rejects a wildcard in the
## middle of a label (`f*o`) whatever the flags, so the partial form that only
## X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS rejects is the one tested here.
##
## Neither name resolves, so the check goes through the sync backend's
## `connectAcross`, which dials a literal address while SNI and verification use
## the name we hand it -- the same path `connect` takes for a resolved host.
import std/[os, strutils]
import navi/backend/sync
import navi/backend/openssl_ctx
import navi/backend/api

let
  ca = getEnv("NAVI_HOSTV_CA")
  port = parseInt(getEnv("NAVI_HOSTV_WILD_PORT"))

var tls = defaultTls()
tls.caFile = ca                 # trust the test CA: only the name can reject
let ctx = newTlsContext(tls, @["http/1.1"])

proc reaches(name: string): bool =
  ## True when a verified handshake for `name` completes and the server answers.
  try:
    var c = connectAcross(ctx, @["127.0.0.1"], name, port, true)
    sendAll(c, "GET / HTTP/1.1\r\nHost: " & name & "\r\nConnection: close\r\n\r\n")
    let line = recvSome(c).splitLines()[0]
    c.close()
    result = "200" in line
  except CatchableError:
    result = false

doAssert reaches("a.wild.example.com"),
  "an ordinary leftmost wildcard (*.wild.example.com) must still match"
doAssert not reaches("foo.example.com"),
  "a partial wildcard (fo*.example.com) must not match foo.example.com"

echo "== partial-wildcard rejection passed =="
