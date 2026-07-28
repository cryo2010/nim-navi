## Local httpbin behind Caddy, exercised on the navi/js backend under Node.
## Compiled with `nim js` and run by httpbin.sh; the self-signed cert is trusted
## via NODE_EXTRA_CA_CERTS (fetch ignores cfg.tls.caFile). The port is fixed by
## the compose file, so the base URL is a constant (getEnv is awkward under js).
##
## Covers the subset of httpbin functionality the js runtime supports: every
## method, raw/JSON/form bodies, params and headers, status handling, basic and
## bearer auth, a followed redirect, the (Node-only) cookie jar, runtime gzip, and
## sized/streamed bodies. Digest auth, maxRedirects=0, and decompress=false are
## native-only concerns handled by the runtime here, so they live in
## httpbin_test.nim instead.

import std/[strutils, json]
import navi/js

const base = "https://127.0.0.1:8447"

proc baseCfg(): NaviConfig =
  result = newNaviConfig()
  result.timeout = 20_000
  result.headers["user-agent"] = "navi-httpbin/1.0"

proc api(): Navi = newNavi(baseCfg())

proc main() {.async.} =
  var passed = 0
  var failures: seq[string]

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
  check "GET /get echoes query args":
    let r = await api().get(base & "/get", params = @[("x", "1"), ("y", "2")])
    r.status == 200 and r.data["args"]["x"].getStr == "1" and
      r.data["args"]["y"].getStr == "2"

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

  # --- response inspection -------------------------------------------------
  check "/json parses via res.data":
    (await api().get(base & "/json")).data["slideshow"]["title"].getStr.len > 0

  check "/response-headers sets a custom response header":
    (await api().get(base & "/response-headers", params = @[("X-Navi", "yo")])).headers["X-Navi"] == "yo"

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

  # --- redirects (runtime-followed) ----------------------------------------
  check "follows a redirect to completion":
    let r = await api().get(base & "/relative-redirect/3")
    r.status == 200 and r.data.hasKey("url")

  # --- cookie jar (kept off-browser under Node) ----------------------------
  check "cookie jar stores a Set-Cookie and replays it":
    # /cookies/set redirects, and on js the runtime follows the redirect itself,
    # hiding the intermediate Set-Cookie from navi. Set it via a plain 200 instead
    # (/response-headers reflects params as response headers).
    let c = api()
    discard await c.get(base & "/response-headers", params = @[("Set-Cookie", "k=v")])
    (await c.get(base & "/cookies")).data["cookies"]["k"].getStr == "v"

  # --- decompression (runtime-owned on js) ---------------------------------
  check "gzip response is decompressed by the runtime":
    (await api().get(base & "/gzip")).data["gzipped"].getBool

  # --- sized / streamed bodies ---------------------------------------------
  check "/base64 decodes to exact text":
    # navi/js's buffered body is a string, so a random /bytes payload would be
    # mangled by text decoding; use a deterministic ASCII endpoint for the
    # buffered path. Exact binary length is covered by the stream() sink below.
    (await api().get(base & "/base64/aGVsbG8=")).body == "hello"

  check "stream() delivers the exact byte count to a sink":
    var total = 0
    let r = await api().stream(GET, base & "/bytes/2048",
      sink = proc(data: openArray[byte]) = total += data.len)
    total == 2048 and r.body.len == 0

  # --- misc response formats -----------------------------------------------
  check "/html is served as text/html":
    (await api().get(base & "/html")).headers["content-type"].contains("text/html")

  check "/anything echoes method and JSON":
    let r = await api().post(base & "/anything", json = %*{"k": "v"})
    r.data["method"].getStr == "POST" and r.data["json"]["k"].getStr == "v"

  echo ""
  echo "httpbin interop [js]: ", passed, " passed, ", failures.len, " failed"
  if failures.len > 0:
    for f in failures: echo "  - ", f
    quit(1)

discard main()
