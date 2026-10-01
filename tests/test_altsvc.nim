## Unit tests for Alt-Svc parsing and the per-client h3 discovery cache.

import unittest
import std/options
import navi/core/[altsvc, url]

suite "parseAltSvc":
  test "parseAltSvc should read an h3 advertisement with default max-age":
    let a = parseAltSvc("h3=\":443\"")
    check a.h3
    check a.endpoint.host == ""          # ":443" carries no host
    check a.endpoint.port == 443
    check a.maxAge == 86_400             # RFC default when `ma` absent
    check not a.clear

  test "parseAltSvc should read an explicit ma parameter":
    let a = parseAltSvc("h3=\":443\"; ma=3600")
    check a.h3
    check a.maxAge == 3600

  test "parseAltSvc should read a host in the alt-authority":
    let a = parseAltSvc("h3=\"alt.example.com:8443\"; ma=60")
    check a.h3
    check a.endpoint.host == "alt.example.com"
    check a.endpoint.port == 8443

  test "parseAltSvc should parse a bracketed IPv6 alt-authority":
    let a = parseAltSvc("h3=\"[2001:db8::1]:443\"")
    check a.h3
    check a.endpoint.host == "2001:db8::1"
    check a.endpoint.port == 443

  test "parseAltSvc should pick h3 from a list of alternatives":
    let a = parseAltSvc("h2=\":443\"; ma=3600, h3=\":8443\"; ma=120")
    check a.h3
    check a.endpoint.port == 8443
    check a.maxAge == 120

  test "parseAltSvc should ignore draft h3 protocol ids":
    let a = parseAltSvc("h3-29=\":443\", h3-27=\":443\"")
    check not a.h3

  test "parseAltSvc should recognize the clear directive":
    let a = parseAltSvc("clear")
    check a.clear
    check not a.h3

  test "parseAltSvc should not raise on malformed input":
    for bad in ["", "   ", "h3", "h3=", "h3=\":\"", "=;=;", "h3=\"nope\""]:
      let a = parseAltSvc(bad)
      check not a.h3
      check not a.clear

suite "AltSvcCache":
  test "the cache should return a recorded h3 endpoint for its origin":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    let ep = c.h3Endpoint("https", "example.com", 443)
    check ep.isSome
    check ep.get.host == "example.com"   # empty alt host resolves to origin
    check ep.get.port == 443

  test "the cache should keep a distinct alt host and port":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\"edge.example.com:8443\"; ma=3600")
    let ep = c.h3Endpoint("https", "example.com", 443)
    check ep.isSome
    check ep.get.host == "edge.example.com"
    check ep.get.port == 8443

  test "the cache should miss for an unknown origin":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    check c.h3Endpoint("https", "other.com", 443).isNone
    check c.h3Endpoint("https", "example.com", 8443).isNone
    check c.h3Endpoint("http", "example.com", 443).isNone

  test "the cache should be case-insensitive on scheme and host":
    let c = newAltSvcCache()
    c.record("HTTPS", "Example.COM", 443, "h3=\":443\"; ma=3600")
    check c.h3Endpoint("https", "example.com", 443).isSome

  test "the cache should drop an origin on the clear directive":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    c.record("https", "example.com", 443, "clear")
    check c.h3Endpoint("https", "example.com", 443).isNone

  test "the cache should not store an advertisement with non-positive ma":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=0")
    check c.h3Endpoint("https", "example.com", 443).isNone

  test "recording a header without h3 should drop any existing entry":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    c.record("https", "example.com", 443, "h2=\":443\"; ma=3600")
    check c.h3Endpoint("https", "example.com", 443).isNone

  test "a freshly recorded entry should be present":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=1")
    check c.h3Endpoint("https", "example.com", 443).isSome

  test "clear should empty the whole cache":
    let c = newAltSvcCache()
    c.record("https", "a.com", 443, "h3=\":443\"; ma=3600")
    c.record("https", "b.com", 443, "h3=\":443\"; ma=3600")
    c.clear()
    check c.h3Endpoint("https", "a.com", 443).isNone
    check c.h3Endpoint("https", "b.com", 443).isNone

  test "h3Endpoint on a nil cache should be none":
    var c: AltSvcCache = nil
    check c.h3Endpoint("https", "example.com", 443).isNone

suite "alt-svc broken-endpoint backoff":
  test "markBroken should suppress the endpoint for the backoff window":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    check c.h3Endpoint("https", "example.com", 443).isSome
    c.markBroken("https", "example.com", 443)
    check c.h3Endpoint("https", "example.com", 443).isNone

  test "markWorking should clear an active backoff":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    c.markBroken("https", "example.com", 443)
    check c.h3Endpoint("https", "example.com", 443).isNone
    c.markWorking("https", "example.com", 443)
    check c.h3Endpoint("https", "example.com", 443).isSome

  test "re-advertising the same endpoint should not clear the backoff":
    # An origin repeats its Alt-Svc header on every TCP response, so a plain
    # re-record must not put the client straight back on the dead UDP path.
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    c.markBroken("https", "example.com", 443)
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    check c.h3Endpoint("https", "example.com", 443).isNone

  test "advertising a different alt-authority should start clean":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    c.markBroken("https", "example.com", 443)
    c.record("https", "example.com", 443, "h3=\"alt.example.com:8443\"; ma=3600")
    let ep = c.h3Endpoint("https", "example.com", 443)
    check ep.isSome
    check ep.get.host == "alt.example.com"
    check ep.get.port == 8443

  test "the backoff should only affect the origin that failed":
    let c = newAltSvcCache()
    c.record("https", "a.com", 443, "h3=\":443\"; ma=3600")
    c.record("https", "b.com", 443, "h3=\":443\"; ma=3600")
    c.markBroken("https", "a.com", 443)
    check c.h3Endpoint("https", "a.com", 443).isNone
    check c.h3Endpoint("https", "b.com", 443).isSome

  test "markBroken and markWorking on an unknown origin should be no-ops":
    let c = newAltSvcCache()
    c.markBroken("https", "example.com", 443)
    c.markWorking("https", "example.com", 443)
    check c.h3Endpoint("https", "example.com", 443).isNone

  test "markBroken and markWorking on a nil cache should not raise":
    var c: AltSvcCache = nil
    c.markBroken("https", "example.com", 443)
    c.markWorking("https", "example.com", 443)
    check c.h3Endpoint("https", "example.com", 443).isNone

  test "the backoff window should double with consecutive failures":
    check brokenBackoffSecs == 60
    check brokenBackoffMaxSecs == 960
    # 60 << 4 == 960, so the ceiling is reached at the fifth failure and holds.
    check min(brokenBackoffSecs shl 4, brokenBackoffMaxSecs) == brokenBackoffMaxSecs
    check min(brokenBackoffSecs shl 5, brokenBackoffMaxSecs) == brokenBackoffMaxSecs

suite "openH3Tracked":
  # The template binds `QuicError` at the expansion site on purpose (naming it in
  # altsvc.nim would be an import cycle: navi/backend/quic imports altsvc), so the
  # test declares its own stand-in and a fake open expression. That is exactly the
  # contract the three real h3 openers rely on.
  type QuicError = object of CatchableError

  proc openOk(): string = "conn"
  proc openFails(): string = raise newException(QuicError, "no route to UDP")

  test "openH3Tracked should return the connection and clear the backoff":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    c.markBroken("https", "example.com", 443)
    check c.h3Endpoint("https", "example.com", 443).isNone
    let conn = c.openH3Tracked("example.com", 443, openOk())
    check conn == "conn"
    check c.h3Endpoint("https", "example.com", 443).isSome

  test "openH3Tracked should mark broken and re-raise on a QuicError":
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    check c.h3Endpoint("https", "example.com", 443).isSome
    var raised = false
    try:
      discard c.openH3Tracked("example.com", 443, openFails())
    except QuicError:
      raised = true
    check raised                         # the original error reaches the caller
    check c.h3Endpoint("https", "example.com", 443).isNone

  test "openH3Tracked should leave other errors alone":
    # Only QuicError is a connect failure; anything else is not an Alt-Svc signal,
    # so it propagates without touching the backoff (and without marking working).
    let c = newAltSvcCache()
    c.record("https", "example.com", 443, "h3=\":443\"; ma=3600")
    c.markBroken("https", "example.com", 443)
    var raised = false
    try:
      discard c.openH3Tracked("example.com", 443,
                              (proc(): string = raise newException(ValueError, "x"))())
    except ValueError:
      raised = true
    check raised
    check c.h3Endpoint("https", "example.com", 443).isNone   # backoff untouched

  test "openH3Tracked on a nil cache should still yield the connection":
    var c: AltSvcCache = nil
    check c.openH3Tracked("example.com", 443, openOk()) == "conn"

suite "recordFrom scheme gate":
  # RFC 7838 2.1: an alternative service may only be learned from a response that
  # arrived over a secure transport. `recordFrom` is the single gate every transport
  # goes through, so a cleartext response can never name the https origin's h3
  # endpoint (#434).
  test "an Alt-Svc header on a plain-http response should not reach the cache":
    let c = newAltSvcCache()
    c.recordFrom(parseUrl("http://api.example:8443/health"),
                 "h3=\"evil.example:443\"; ma=3600")
    check c.h3Endpoint("https", "api.example", 8443).isNone   # the https origin
    check c.h3Endpoint("http", "api.example", 8443).isNone    # and no http key either

  test "an Alt-Svc header on an https response should be recorded":
    let c = newAltSvcCache()
    c.recordFrom(parseUrl("https://api.example:8443/"), "h3=\":8443\"; ma=3600")
    let ep = c.h3Endpoint("https", "api.example", 8443)
    check ep.isSome
    check ep.get.host == "api.example"
    check ep.get.port == 8443

  test "recordFrom should use the scheme default port when the URL omits it":
    let c = newAltSvcCache()
    c.recordFrom(parseUrl("https://api.example/"), "h3=\":443\"; ma=3600")
    check c.h3Endpoint("https", "api.example", 443).isSome

  test "recordFrom should accept an uppercase https scheme":
    let c = newAltSvcCache()
    c.recordFrom(parseUrl("HTTPS://API.example/"), "h3=\":443\"; ma=3600")
    check c.h3Endpoint("https", "api.example", 443).isSome

  test "a cleartext clear directive should not drop the https entry":
    # The mirror of the poisoning case: an on-path attacker on the http leg must not
    # be able to evict the origin's real advertisement either.
    let c = newAltSvcCache()
    c.recordFrom(parseUrl("https://api.example:8443/"), "h3=\":8443\"; ma=3600")
    c.recordFrom(parseUrl("http://api.example:8443/health"), "clear")
    check c.h3Endpoint("https", "api.example", 8443).isSome

  test "recordFrom on a nil cache should not raise":
    var c: AltSvcCache = nil
    c.recordFrom(parseUrl("https://api.example/"), "h3=\":443\"; ma=3600")
    check c.h3Endpoint("https", "api.example", 443).isNone
