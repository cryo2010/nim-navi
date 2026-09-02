## Strict protocol selection (#193): the HTTP version actually used must be in
## config.http, else ProtocolError -- with the single exemption of the h3 Alt-Svc
## discovery leg for an h3-only config.

import unittest
import navi/core/request
import navi/core/response   # ProtocolError

proc cfg(h: set[HttpVersion]): NaviConfigBase =
  result.http = h

proc allows(h: set[HttpVersion], httpVersion: string): bool =
  ## True when strict selection permits `httpVersion` under `h` (no ProtocolError).
  try:
    enforceProtocol(cfg(h), httpVersion)
    true
  except ProtocolError:
    false

suite "strict protocol selection":
  test "enforceProtocol should raise when the used version is not in config.http":
    check not allows({H2}, "HTTP/1.1")
    check not allows({H1}, "HTTP/2")

  test "enforceProtocol should pass when the used version is allowed":
    check allows({H1, H2}, "HTTP/1.1")
    check allows({H1, H2}, "HTTP/2")
    check allows({H2}, "HTTP/2")

  test "enforceProtocol should treat an empty set as allow-all":
    check allows({}, "HTTP/1.1")
    check allows({}, "HTTP/2")
    check allows({}, "HTTP/3")

  test "enforceProtocol should exempt the h1/h2 bootstrap for an h3-only config":
    check allows({H3}, "HTTP/2")      # Alt-Svc discovery leg
    check allows({H3}, "HTTP/1.1")
    check allows({H3}, "HTTP/3")      # after upgrade

  test "enforceProtocol should still reject an h1 downgrade when h2 or h3 is required":
    # {H2,H3} names its own bootstrap (h2), so h1 is a genuine downgrade, not discovery.
    check not allows({H2, H3}, "HTTP/1.1")

  test "enforceProtocol should accept h3 wherever it is requested":
    check allows({H1, H2, H3}, "HTTP/3")
    check allows({H2, H3}, "HTTP/3")

  test "the build-aware default should list every protocol this build can negotiate":
    when defined(naviHttp3):
      check defaultHttpVersions == {H1, H2, H3}
    else:
      check defaultHttpVersions == {H1, H2}
    # A default client therefore never trips strict selection on h1/h2/h3.
    check allows(defaultHttpVersions, "HTTP/1.1")
    check allows(defaultHttpVersions, "HTTP/2")
