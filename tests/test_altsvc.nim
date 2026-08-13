## Unit tests for Alt-Svc parsing and the per-client h3 discovery cache.

import unittest
import std/options
import navi/core/altsvc

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
