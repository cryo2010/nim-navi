## Unix domain socket transport on the sync backend.
##
## Driven by tests/interop/unixsocket.sh, which starts an AF_UNIX HTTP server that
## echoes the request's Host header and exports NAVI_UDS_PATH. Validates the round
## trip, that the URL host (not the socket path) becomes the Host header, that an
## over-long socket path is rejected, and that a URL with no authority is refused
## before it reaches the socket (#435).
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

  test "a URL with no host should be refused before it reaches the socket":
    # The Unix-socket connect never resolves the URL host, so an empty one used to
    # travel all the way into the handshake, where "no host" meant no SNI and no
    # certificate identity check: any chain-valid certificate was accepted (#435).
    # A configured `unixSocket` is exactly the reachable case, so prove the refusal
    # here rather than only at `buildRequest`.
    expect ValueError:
      discard client().get("https:///path")
    expect ValueError:
      discard client().get("http:///hello")
    # ... while a URL that does name a host still round-trips over the socket.
    check client().get("http://example.test/").status == 200
