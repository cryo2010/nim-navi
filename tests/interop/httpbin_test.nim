## Local httpbin behind Caddy (TLS + HTTP/2): exercises navi's client across the
## full breadth of httpbin's surface -- every request method, request bodies (raw,
## JSON, form), query params and headers, status-code handling, auth (basic,
## bearer, digest), redirects, gzip/deflate/brotli decompression, the cookie jar,
## streaming, and assorted response formats. Driven by httpbin.sh; the target is
## the container (NAVI_HTTPBIN_URL), never the public httpbin.org.

import std/[os, strutils, json]
import navi

let cert = getEnv("NAVI_INTEROP_CERT")
let base = getEnv("NAVI_HTTPBIN_URL")

var passed = 0
var skipped = 0
var failures: seq[string]

template check(name: string, cond: untyped) =
  ## Evaluate a boolean assertion; an exception is a failure, not a crash, so one
  ## broken endpoint does not hide the rest.
  block:
    try:
      if cond: inc passed
      else:
        failures.add name
        echo "FAIL ", name
    except CatchableError as e:
      failures.add name & " [" & e.msg & "]"
      echo "FAIL ", name, "  (", e.msg, ")"

proc baseCfg(): NaviConfig =
  result = newNaviConfig()
  result.tls.caFile = cert
  result.timeout = 20_000
  result.headers["user-agent"] = "navi-httpbin/1.0"

proc api(): Navi = newNavi(baseCfg())

# --- request methods -------------------------------------------------------
check "GET /get echoes query args over h2":
  let r = api().get(base & "/get", params = @[("x", "1"), ("y", "2")])
  r.status == 200 and r.httpVersion == "HTTP/2" and
    r.data["args"]["x"].getStr == "1" and r.data["args"]["y"].getStr == "2"

check "POST /post echoes a raw body":
  api().post(base & "/post", body = "hello body").data["data"].getStr == "hello body"

check "POST /post encodes a JSON body and sets Content-Type":
  let r = api().post(base & "/post", json = %*{"a": 1, "b": "two"})
  r.data["json"]["a"].getInt == 1 and r.data["json"]["b"].getStr == "two" and
    r.data["headers"]["Content-Type"].getStr.contains("application/json")

check "POST /post encodes a form body":
  let r = api().post(base & "/post", form = @[("f", "v"), ("g", "w")])
  r.data["form"]["f"].getStr == "v" and r.data["form"]["g"].getStr == "w"

check "PUT /put echoes a body":
  api().put(base & "/put", body = "pv").data["data"].getStr == "pv"

check "PATCH /patch":
  api().patch(base & "/patch", body = "x").status == 200

check "DELETE /delete":
  api().delete(base & "/delete").status == 200

check "HEAD /get returns no body":
  let r = api().head(base & "/get")
  r.status == 200 and r.body.len == 0

check "OPTIONS /get advertises Allow":
  let r = api().options(base & "/get")
  r.status == 200 and "allow" in r.headers

# --- request data ----------------------------------------------------------
check "a custom request header is delivered":
  var c = baseCfg(); c.headers["x-navi-test"] = "42"
  newNavi(c).get(base & "/headers").data["headers"]["X-Navi-Test"].getStr == "42"

check "/user-agent reflects our User-Agent":
  api().get(base & "/user-agent").data["user-agent"].getStr == "navi-httpbin/1.0"

# --- status codes ----------------------------------------------------------
check "/status/200 is 200":
  api().get(base & "/status/200").status == 200

check "throwHttpErrors=false returns the 404":
  var c = baseCfg(); c.throwHttpErrors = false
  newNavi(c).get(base & "/status/404").status == 404

check "throwHttpErrors=true raises HttpError on 500":
  var raised = false
  try: discard api().get(base & "/status/500")
  except HttpError as e: raised = e.response.status == 500
  raised

check "/status/418 teapot":
  var c = baseCfg(); c.throwHttpErrors = false
  newNavi(c).get(base & "/status/418").status == 418

# --- response inspection ---------------------------------------------------
check "/json parses via res.data":
  api().get(base & "/json").data["slideshow"]["title"].getStr.len > 0

check "/response-headers sets a custom response header":
  api().get(base & "/response-headers", params = @[("X-Navi", "yo")]).headers["X-Navi"] == "yo"

check "/ip returns an origin":
  api().get(base & "/ip").data["origin"].getStr.len > 0

check "/uuid returns a uuid":
  api().get(base & "/uuid").data["uuid"].getStr.len == 36

# --- auth ------------------------------------------------------------------
check "basic auth succeeds":
  var c = baseCfg(); c.auth = basicAuth("user", "pass")
  let r = newNavi(c).get(base & "/basic-auth/user/pass")
  r.status == 200 and r.data["authenticated"].getBool

check "basic auth with a wrong password is rejected (401)":
  var c = baseCfg(); c.auth = basicAuth("user", "wrong"); c.throwHttpErrors = false
  newNavi(c).get(base & "/basic-auth/user/pass").status == 401

check "bearer auth succeeds and echoes the token":
  var c = baseCfg(); c.auth = bearerAuth("tok123")
  newNavi(c).get(base & "/bearer").data["token"].getStr == "tok123"

check "digest auth completes the 401 challenge and retries":
  var c = baseCfg(); c.auth = digestAuth("duser", "dpass")
  let r = newNavi(c).get(base & "/digest-auth/auth/duser/dpass")
  r.status == 200 and r.data["authenticated"].getBool

# --- redirects -------------------------------------------------------------
check "follows relative redirects to completion":
  let r = api().get(base & "/relative-redirect/3")
  r.status == 200 and r.data.hasKey("url")

check "/redirect-to follows an explicit target":
  api().get(base & "/redirect-to",
    params = @[("url", "/get"), ("status_code", "307")]).status == 200

check "maxRedirects=0 returns the 302 unfollowed":
  var c = baseCfg(); c.maxRedirects = 0; c.throwHttpErrors = false
  let r = newNavi(c).get(base & "/relative-redirect/1")
  r.status == 302 and "location" in r.headers

# --- decompression ---------------------------------------------------------
check "gzip response is auto-decompressed":
  api().get(base & "/gzip").data["gzipped"].getBool

check "deflate response is auto-decompressed":
  api().get(base & "/deflate").data["deflated"].getBool

check "decompress=false leaves the gzip body raw":
  # Ask for gzip explicitly (httpbin only compresses when Accept-Encoding invites
  # it) but disable navi's decompression, so the body must come back encoded.
  var c = baseCfg()
  c.decompress = false
  c.headers["accept-encoding"] = "gzip"
  let r = newNavi(c).get(base & "/gzip")
  r.headers.get("content-encoding").contains("gzip") and
    r.body.len >= 2 and r.body[0] == '\x1f' and r.body[1] == '\x8b'

block:  # brotli decoding needs libbrotlidec at runtime; skip cleanly if absent.
  const name = "brotli response is auto-decompressed"
  try:
    if api().get(base & "/brotli").data["brotli"].getBool: inc passed
    else: (failures.add name; echo "FAIL ", name)
  except ValueError as e:
    if "brotli" in e.msg.toLowerAscii:
      inc skipped; echo "SKIP ", name, " (", e.msg, ")"
    else: (failures.add name; echo "FAIL ", name, "  (", e.msg, ")")

# --- cookie jar ------------------------------------------------------------
check "cookie jar stores a Set-Cookie and replays it across the redirect":
  # /cookies/set 302 -> /cookies; the jar replays foo=bar on the followed request.
  api().get(base & "/cookies/set", params = @[("foo", "bar")]).data["cookies"]["foo"].getStr == "bar"

check "cookie jar persists for a later request on the same client":
  let c = api()
  discard c.get(base & "/cookies/set", params = @[("k", "v")])
  c.get(base & "/cookies").data["cookies"]["k"].getStr == "v"

# --- streaming / sized bodies ----------------------------------------------
check "/bytes returns an exact length":
  api().get(base & "/bytes/1024").body.len == 1024

check "stream() delivers the body to a sink and leaves res.body empty":
  var total = 0
  let r = api().stream(GET, base & "/bytes/2048",
    sink = proc(data: openArray[byte]) = total += data.len)
  total == 2048 and r.body.len == 0

check "/stream/5 yields five newline-delimited JSON objects":
  var n = 0
  for ln in api().get(base & "/stream/5").body.splitLines:
    if ln.strip.len > 0: inc n
  n == 5

# --- misc response formats -------------------------------------------------
check "/html is served as text/html":
  api().get(base & "/html").headers["content-type"].contains("text/html")

check "/encoding/utf8 is utf-8":
  let r = api().get(base & "/encoding/utf8")
  r.status == 200 and r.headers["content-type"].toLowerAscii.contains("utf-8") and r.body.len > 0

check "/anything echoes method and JSON":
  let r = api().post(base & "/anything", json = %*{"k": "v"})
  r.data["method"].getStr == "POST" and r.data["json"]["k"].getStr == "v"

# --- summary ---------------------------------------------------------------
echo ""
echo "httpbin interop: ", passed, " passed, ", skipped, " skipped, ", failures.len, " failed"
if failures.len > 0:
  for f in failures: echo "  - ", f
  quit(1)
