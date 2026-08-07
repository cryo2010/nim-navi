## Local httpbin behind Caddy, exercised across navi's three native backends.
## Built three ways by httpbin.sh:
##   nim c ...                -> navi (sync, h2)
##   nim c -d:useAsync ...    -> navi/asyncdispatch (h2)
##   nim c -d:useChronos ...  -> navi/chronos (HTTP/1.1: BearSSL has no client ALPN)
## The assertions are identical for all three: they live in one `checks` template
## and use `await`, which is an identity template on the sync backend so the body
## reads the same. (The js backend differs enough -- fetch runtime, cert trust via
## env, no digest, runtime-owned redirects/decoding -- that it has its own file,
## httpbin_js.nim.)

import std/[os, strutils, json]

when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
  const wantVersion = "HTTP/1.1"
elif defined(useAsync):
  import navi/asyncdispatch
  const backend = "asyncdispatch"
  const wantVersion = "HTTP/2"
else:
  import navi
  template await(x: untyped): untyped = x   # identity: the sync body reads the same
  const backend = "sync"
  const wantVersion = "HTTP/2"

proc baseCfg(): NaviConfig =
  # Read the cert path here rather than from a global so the config helper stays
  # GC-safe under chronos's async macro.
  result = initNaviConfig()
  result.tls.caFile = getEnv("NAVI_INTEROP_CERT")
  result.timeout = 20_000
  result.headers["user-agent"] = "navi-httpbin/1.0"

proc api(): Navi = newNavi(baseCfg())

template runAll() =
  # State and env reads live here (locals of `main`), so the chronos async proc
  # touches no GC'd globals -- its macro would otherwise reject it as not GC-safe.
  var passed = 0
  var skipped = 0
  var failures: seq[string]
  let base = getEnv("NAVI_HTTPBIN_URL")

  # Defined here (not at module scope) so it binds the locals above -- template
  # free identifiers resolve at the definition site, and keeping the state local
  # is what makes the chronos async build GC-safe.
  template check(name: string, cond: untyped) =
    block:
      try:
        if cond: inc passed
        else:
          failures.add name
          echo "FAIL ", name
      except CatchableError as e:
        failures.add name & " [" & e.msg & "]"
        echo "FAIL ", name, "  (", e.msg, ")"

  # --- request methods -----------------------------------------------------
  check "GET /get echoes query args over " & wantVersion:
    let r = await api().get(base & "/get", params = @[("x", "1"), ("y", "2")])
    r.status == 200 and r.httpVersion == wantVersion and
      r.data["args"]["x"].getStr == "1" and r.data["args"]["y"].getStr == "2"

  check "POST /post echoes a raw body":
    (await api().post(base & "/post", body = "hello body")).data["data"].getStr == "hello body"

  check "POST /post encodes a JSON body and sets Content-Type":
    let r = await api().post(base & "/post", json = %*{"a": 1, "b": "two"})
    r.data["json"]["a"].getInt == 1 and r.data["json"]["b"].getStr == "two" and
      r.data["headers"]["Content-Type"].getStr.contains("application/json")

  check "POST /post encodes a form body":
    let r = await api().post(base & "/post", form = @[("f", "v"), ("g", "w")])
    r.data["form"]["f"].getStr == "v" and r.data["form"]["g"].getStr == "w"

  check "PUT /put echoes a body":
    (await api().put(base & "/put", body = "pv")).data["data"].getStr == "pv"

  check "PATCH /patch":
    (await api().patch(base & "/patch", body = "x")).status == 200

  check "DELETE /delete":
    (await api().delete(base & "/delete")).status == 200

  check "HEAD /get returns no body":
    let r = await api().head(base & "/get")
    r.status == 200 and r.body.len == 0

  check "OPTIONS /get advertises Allow":
    let r = await api().options(base & "/get")
    r.status == 200 and "allow" in r.headers

  # --- request data --------------------------------------------------------
  check "a custom request header is delivered":
    var c = baseCfg(); c.headers["x-navi-test"] = "42"
    (await newNavi(c).get(base & "/headers")).data["headers"]["X-Navi-Test"].getStr == "42"

  check "/user-agent reflects our User-Agent":
    (await api().get(base & "/user-agent")).data["user-agent"].getStr == "navi-httpbin/1.0"

  # --- status codes --------------------------------------------------------
  check "/status/200 is 200":
    (await api().get(base & "/status/200")).status == 200

  check "throwHttpErrors=false returns the 404":
    var c = baseCfg(); c.throwHttpErrors = false
    (await newNavi(c).get(base & "/status/404")).status == 404

  check "throwHttpErrors=true raises HttpError on 500":
    var raised = false
    try: discard await api().get(base & "/status/500")
    except HttpError as e: raised = e.response.status == 500
    raised

  check "/status/418 teapot":
    var c = baseCfg(); c.throwHttpErrors = false
    (await newNavi(c).get(base & "/status/418")).status == 418

  # --- response inspection -------------------------------------------------
  check "/json parses via res.data":
    (await api().get(base & "/json")).data["slideshow"]["title"].getStr.len > 0

  check "/response-headers sets a custom response header":
    (await api().get(base & "/response-headers", params = @[("X-Navi", "yo")])).headers["X-Navi"] == "yo"

  check "/ip returns an origin":
    (await api().get(base & "/ip")).data["origin"].getStr.len > 0

  check "/uuid returns a uuid":
    (await api().get(base & "/uuid")).data["uuid"].getStr.len == 36

  # --- auth ----------------------------------------------------------------
  check "basic auth succeeds":
    var c = baseCfg(); c.auth = basicAuth("user", "pass")
    let r = await newNavi(c).get(base & "/basic-auth/user/pass")
    r.status == 200 and r.data["authenticated"].getBool

  check "basic auth with a wrong password is rejected (401)":
    var c = baseCfg(); c.auth = basicAuth("user", "wrong"); c.throwHttpErrors = false
    (await newNavi(c).get(base & "/basic-auth/user/pass")).status == 401

  check "bearer auth succeeds and echoes the token":
    var c = baseCfg(); c.auth = bearerAuth("tok123")
    (await newNavi(c).get(base & "/bearer")).data["token"].getStr == "tok123"

  check "digest auth completes the 401 challenge and retries":
    var c = baseCfg(); c.auth = digestAuth("duser", "dpass")
    let r = await newNavi(c).get(base & "/digest-auth/auth/duser/dpass")
    r.status == 200 and r.data["authenticated"].getBool

  # --- redirects -----------------------------------------------------------
  check "follows relative redirects to completion":
    let r = await api().get(base & "/relative-redirect/3")
    r.status == 200 and r.data.hasKey("url")

  check "/redirect-to follows an explicit target":
    (await api().get(base & "/redirect-to",
      params = @[("url", "/get"), ("status_code", "307")])).status == 200

  check "maxRedirects=0 returns the 302 unfollowed":
    var c = baseCfg(); c.maxRedirects = 0; c.throwHttpErrors = false
    let r = await newNavi(c).get(base & "/relative-redirect/1")
    r.status == 302 and "location" in r.headers

  # --- decompression -------------------------------------------------------
  check "gzip response is auto-decompressed":
    (await api().get(base & "/gzip")).data["gzipped"].getBool

  check "deflate response is auto-decompressed":
    (await api().get(base & "/deflate")).data["deflated"].getBool

  check "decompress=false leaves the gzip body raw":
    # Ask for gzip explicitly (httpbin only compresses when Accept-Encoding invites
    # it) but disable navi's decompression, so the body must come back encoded.
    var c = baseCfg(); c.decompress = false; c.headers["accept-encoding"] = "gzip"
    let r = await newNavi(c).get(base & "/gzip")
    r.headers.get("content-encoding").contains("gzip") and
      r.body.len >= 2 and r.body[0] == '\x1f' and r.body[1] == '\x8b'

  block:  # brotli decoding needs libbrotlidec at runtime; skip cleanly if absent.
    const bname = "brotli response is auto-decompressed"
    try:
      if (await api().get(base & "/brotli")).data["brotli"].getBool: inc passed
      else: (failures.add bname; echo "FAIL ", bname)
    except ValueError as e:
      if "brotli" in e.msg.toLowerAscii: (inc skipped; echo "SKIP ", bname, " (", e.msg, ")")
      else: (failures.add bname; echo "FAIL ", bname, "  (", e.msg, ")")

  # --- cookie jar ----------------------------------------------------------
  check "cookie jar stores a Set-Cookie and replays it across the redirect":
    # /cookies/set 302 -> /cookies; the jar replays foo=bar on the followed request.
    (await api().get(base & "/cookies/set", params = @[("foo", "bar")])).data["cookies"]["foo"].getStr == "bar"

  check "cookie jar persists for a later request on the same client":
    let c = api()
    discard await c.get(base & "/cookies/set", params = @[("k", "v")])
    (await c.get(base & "/cookies")).data["cookies"]["k"].getStr == "v"

  # --- streaming / sized bodies --------------------------------------------
  check "/bytes returns an exact length":
    (await api().get(base & "/bytes/1024")).body.len == 1024

  check "stream() delivers the body to a sink and leaves res.body empty":
    var total = 0
    let r = await api().stream(GET, base & "/bytes/2048",
      sink = proc(data: openArray[byte]) = total += data.len)
    total == 2048 and r.body.len == 0

  check "a bodyStream upload is streamed and arrives intact over " & wantVersion:
    # A pull-based producer over each native backend (chunked on h1, DATA frames
    # on h2); httpbin echoes the received body, which must equal what we produced.
    let chunk = repeat("s", 10_000)
    var left = 3                          # 3 x 10k, produced across several calls
    let r = await api().request(POST, base & "/post", bodyStream = proc(): string =
      if left == 0: return ""
      dec left
      chunk)
    r.data["data"].getStr == repeat("s", 30_000)

  check "/stream/5 yields five newline-delimited JSON objects":
    var n = 0
    for ln in (await api().get(base & "/stream/5")).body.splitLines:
      if ln.strip.len > 0: inc n
    n == 5

  # --- misc response formats -----------------------------------------------
  check "/html is served as text/html":
    (await api().get(base & "/html")).headers["content-type"].contains("text/html")

  check "/encoding/utf8 is utf-8":
    let r = await api().get(base & "/encoding/utf8")
    r.status == 200 and r.headers["content-type"].toLowerAscii.contains("utf-8") and r.body.len > 0

  check "/anything echoes method and JSON":
    let r = await api().post(base & "/anything", json = %*{"k": "v"})
    r.data["method"].getStr == "POST" and r.data["json"]["k"].getStr == "v"

  # --- summary -------------------------------------------------------------
  echo ""
  echo "httpbin interop [", backend, "]: ", passed, " passed, ", skipped,
    " skipped, ", failures.len, " failed"
  if failures.len > 0:
    for f in failures: echo "  - ", f
    quit(1)

when defined(useChronos) or defined(useAsync):
  proc main() {.async.} = runAll()
  waitFor main()
else:
  proc main() = runAll()
  main()
