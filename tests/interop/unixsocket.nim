## Unix domain socket transport on the sync backend.
##
## Driven by tests/interop/unixsocket.sh, which starts an AF_UNIX HTTP server that
## echoes the request's Host header and exports NAVI_UDS_PATH. Validates the round
## trip, that the URL host (not the socket path) becomes the Host header, and that
## an over-long socket path is rejected.
import unittest
import std/[os, strutils]
import navi

let sock = getEnv("NAVI_UDS_PATH")

proc client(path = sock): Navi =
  var cfg = initNaviConfig()
  cfg.unixSocket = path
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  newNavi(cfg)

suite "Unix domain socket (sync backend)":
  test "a request should round-trip over the Unix socket":
    let r = client().get("http://localhost/hello")
    check r.status == 200

  test "the Host header should carry the URL host, not the socket path":
    check client().get("http://example.test/").body == "example.test"

  test "an over-long socket path should be rejected":
    let bad = "/" & repeat("a", 200)
    expect CatchableError:
      discard client(bad).get("http://localhost/")
