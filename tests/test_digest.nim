## Digest auth: the pure computation (RFC 2617 test vector) and the end-to-end
## 401-challenge/retry flow against an in-process server.

import unittest
import std/[strutils, options]
import navi/core/digest

suite "digest computation":
  test "matches the RFC 2617 section 3.5 example":
    let ch = parseChallenge(
      "Digest realm=\"testrealm@host.com\", qop=\"auth,auth-int\", " &
      "nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\", " &
      "opaque=\"5ccc069c403ebaf9f0171e9517f40e41\"")
    check ch.isSome
    let header = digestAuthHeader("Mufasa", "Circle Of Life", "GET",
      "/dir/index.html", ch.get, cnonce = "0a4f113b")
    # The canonical response hash from the RFC example.
    check "response=\"6629fae49393a05397450978507c4ef1\"" in header
    check "qop=auth" in header
    check "nc=00000001" in header
    check "cnonce=\"0a4f113b\"" in header
    check "opaque=\"5ccc069c403ebaf9f0171e9517f40e41\"" in header

  test "matches the RFC 7616 section 3.9.1 SHA-256 example":
    let ch = parseChallenge(
      "Digest realm=\"http-auth@example.org\", qop=\"auth, auth-int\", " &
      "algorithm=SHA-256, " &
      "nonce=\"7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v\", " &
      "opaque=\"FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS\"")
    check ch.isSome
    let header = digestAuthHeader("Mufasa", "Circle of Life", "GET",
      "/dir/index.html", ch.get,
      cnonce = "f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ")
    # The canonical SHA-256 response hash from the RFC example.
    check ("response=\"753927fa0e85d155564e2e272a28d1802ca10da" &
           "f4496794697cf8db5856cb6c1\"") in header
    check "algorithm=SHA-256" in header
    check "qop=auth" in header

  test "parseChallenge ignores a non-Digest scheme":
    check parseChallenge("Basic realm=\"x\"").isNone

  test "the legacy no-qop form omits qop, nc, and cnonce":
    let ch = parseChallenge("Digest realm=\"r\", nonce=\"n\"")
    let header = digestAuthHeader("u", "p", "GET", "/", ch.get)
    check "qop" notin header
    check "cnonce" notin header
    check "nc=" notin header

# End-to-end flow uses a plain-http in-process server (no TLS needed).
import std/[net, os]
import navi

var chReady: bool

proc serveDigest(port: int) {.thread.} =
    ## Answer the first request with a 401 Digest challenge, then check the
    ## Authorization header on the retry and return 200 when it is present.
    var server = newSocket()
    server.setSockOpt(OptReuseAddr, true)
    server.bindAddr(Port(port), "127.0.0.1")
    server.listen()
    chReady = true

    proc readReq(c: Socket): string =
      while "\r\n\r\n" notin result: result.add c.recv(1)

    block firstRequest:
      var c: Socket
      server.accept(c)
      discard readReq(c)
      let body = "unauthorized"
      c.send("HTTP/1.1 401 Unauthorized\r\n" &
             "WWW-Authenticate: Digest realm=\"navi\", qop=\"auth\", " &
             "nonce=\"abc123\", opaque=\"xyz\"\r\n" &
             "Content-Length: " & $body.len & "\r\nConnection: close\r\n\r\n" & body)
      c.close()

    block retry:
      var c: Socket
      server.accept(c)
      let req = readReq(c).toLowerAscii
      let ok = "authorization: digest" in req and "response=" in req and
               "uri=\"/secret\"" in req
      let body = if ok: "welcome" else: "still unauthorized"
      let status = if ok: "200 OK" else: "401 Unauthorized"
      c.send("HTTP/1.1 " & status & "\r\nContent-Length: " & $body.len &
             "\r\nConnection: close\r\n\r\n" & body)
      c.close()
    server.close()

suite "digest auth end to end":
  test "answers a 401 Digest challenge and retries with credentials":
    const port = 8992
    var th: Thread[int]
    createThread(th, serveDigest, port)
    while not chReady: sleep(5)

    let api = newNavi(NaviOptions(auth: digestAuth("user", "pass")))
    let res = api.get("http://127.0.0.1:" & $port & "/secret")
    check res.status == 200
    check res.body == "welcome"
    joinThread(th)
