## TLS client conformance against badssl.com.
##
## Network test (nightly / on demand, not a per-PR gate): asserts navi rejects
## invalid server certificates with verification on (the default) and accepts a
## valid one. Run with `nimble badssl`.

import unittest
import navi

const invalid = [
  "https://expired.badssl.com/",
  "https://wrong.host.badssl.com/",
  "https://self-signed.badssl.com/",
  "https://untrusted-root.badssl.com/",
]

suite "badssl TLS client conformance":
  test "rejects invalid certificates when verify is on (default)":
    var cfg = newNaviConfig()
    cfg.maxRetries = 0
    let api = newNavi(cfg)
    for url in invalid:
      var raised = false
      try:
        discard api.get(url)
      except CatchableError:
        raised = true
      checkpoint(url)
      check raised

  test "accepts a valid certificate":
    var cfg = newNaviConfig()
    cfg.maxRetries = 0
    let api = newNavi(cfg)
    check api.get("https://badssl.com/").status == 200

  test "verify = false accepts an invalid certificate":
    var cfg = newNaviConfig()
    cfg.tls.verify = false
    cfg.maxRetries = 0
    let api = newNavi(cfg)
    check api.get("https://self-signed.badssl.com/").status == 200
