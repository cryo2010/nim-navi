## stressRequests, navi/js backend (Node). `clients` x `concurrency` workers rotate
## every verb x payload against /echo across the server pool, tally status codes,
## and verify each echo per content-type kind (json by parsed-tree equality, form by
## decoded pairs, text byte-exact). The js cell is the js-safe subset of the shared
## catalog: binary payloads are excluded (TextEncoder mangles raw bytes) and there is
## no request-body compression (the js runtime owns its codec). The runner trusts the
## self-signed cert via NODE_EXTRA_CA_CERTS.

import std/[strutils, json]
import navi/js
import ../common/[harness_js, payloads]

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"
    await ctx.next()

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let payloads = filterPayloads(stressPayloads(), cfg.contentTypes, jsSafe = true)
  if payloads.len == 0:
    # NAVI_CONTENT_TYPES=octet leaves nothing js-safe (binary is excluded on js), so
    # skip cleanly like a native gap cell instead of `mod 0`-crashing in the worker.
    echo "[requests ", cfg.proto, " js] skipped: no js-safe payloads in NAVI_CONTENT_TYPES=",
      cfg.contentTypes
    return
  let counter = newJsCounter()
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients:
    var c = initNaviConfig()
    c.middleware = @[stampMw()]
    apis.add newNavi(c)

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  let label = "[requests " & cfg.proto & " js]"
  let timer = setIntervalJs(proc () = counter.report(label, start),
                            cfg.reportSeconds * 1000)

  proc fail(msg: string) =
    echo label, " FAIL: ", msg
    jsExit(1)

  proc verifyEcho(p: Payload, v: HttpVerb, res: Response) =
    ## Per-kind verification (no checkVersion: js can't pin/read the negotiated version).
    let base = res.headers.get("content-type").split(';', 1)[0].strip().toLowerAscii()
    if base != p.contentType:
      fail(p.name & " " & $v & ": echoed content-type '" &
        res.headers.get("content-type") & "' (want base " & p.contentType & ")")
    case p.kind
    of pkJson:
      var got, want: JsonNode
      try: got = parseJson(res.body)
      except CatchableError as e:
        fail(p.name & ": echoed body is not JSON (" & e.msg & ")"); return
      want = parseJson(p.jsonSrc)
      if got != want: fail(p.name & ": JSON tree mismatch")
      if p.reserialized and res.body == $want:
        fail(p.name & ": server byte-echoed (canonical echo == sent bytes)")
    of pkForm:
      if not checkFormEcho(res.body, p.form):
        fail(p.name & ": form pairs mismatch (echoed '" & res.body & "')")
    of pkText, pkBinary:
      if res.body != p.text:
        fail(p.name & ": body mismatch (expected " & $p.text.len &
          " got " & $res.body.len & ")")

  proc worker(api: Navi, i: int) {.async.} =
    var n = i
    while nowMs() < deadline:
      let v = verbs[n mod verbs.len]
      let p = payloads[(n div verbs.len) mod payloads.len]
      let bodied = v in {POST, PUT, PATCH}
      inc n
      var h = initHeaders()
      # No x-want-encoding: the js cell leaves response compression to the runtime's
      # own codec (unchanged from the pre-catalog js client), so it never asks the
      # server to br/zstd-encode a body undici might not decode.
      try:
        var res: Response
        if not bodied:
          res = await api.request(v, pool.pick() & "/echo", headers = h)
        else:
          case p.kind
          of pkJson:
            res = await api.request(v, pool.pick() & "/echo", headers = h,
                                    body = parseJson(p.jsonSrc))
          of pkForm:
            res = await api.request(v, pool.pick() & "/echo", headers = h, form = p.form)
          of pkText, pkBinary:
            h["content-type"] = p.contentType
            res = await api.request(v, pool.pick() & "/echo", headers = h, body = p.text)
        if res.status != 200:
          fail(p.name & " " & $v & " -> status " & $res.status)
        if res.headers.get("x-echo-method") != $v:
          fail(p.name & " " & $v & " echoed method '" & res.headers.get("x-echo-method") & "'")
        if res.headers.get("x-echo-stress") != "1":
          fail(p.name & " " & $v & " middleware header not echoed")
        if bodied: verifyEcho(p, v, res)
        elif res.body.len != 0:
          fail(p.name & " " & $v & " bodiless verb returned a body")
        counter.tally(res.status)
      except CatchableError as e:
        counter.note()
        fail(p.name & " " & $v & " -> " & $e.name & ": " & e.msg)

  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add worker(api, i)
  for f in futs: await f
  clearIntervalJs(timer)

  if counter.ops == 0:
    echo label, " FAIL: no request completed"
    jsExit(1)
  counter.report(label, start)
  echo "== requests js ", cfg.proto, " passed (", counter.ops, " ops) =="

discard main()
