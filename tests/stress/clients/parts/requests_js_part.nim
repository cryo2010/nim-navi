# The verified /echo request loop for the navi/js backend, shared by
# clients/requests_js.nim and the requests slice of clients/mixed_js.nim.
# `include`d (not imported) AFTER the including file's `import navi/js`, like the
# native parts, so Navi/Response/HttpVerb/Future are in scope. Not a standalone
# module. Needs std/[strutils, json] and ../common/[harness_js, payloads] from
# the including file.
#
# The js cell runs the js-safe subset of the shared payload catalog (binary is
# excluded -- TextEncoder mangles raw bytes) with no request-body compression
# (the runtime owns its codec) and no checkVersion (js cannot pin or read the
# negotiated version). Everything else is the native contract: status 200, the
# echoed method and middleware header, and the body per content-type kind.

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"
    await ctx.next()

proc mkJsClient(): Navi =
  var c = initNaviConfig()
  c.middleware = @[stampMw()]
  newNavi(c)

proc verifyEcho(label: string, p: Payload, v: HttpVerb, res: Response) =
  ## Per-kind verification (no checkVersion: js can't pin/read the negotiated
  ## version). JSON/form compare parsed values; text stays byte-exact.
  let base = res.headers.get("content-type").split(';', 1)[0].strip().toLowerAscii()
  if base != p.contentType:
    jsFail(label, p.name & " " & $v & ": echoed content-type '" &
      res.headers.get("content-type") & "' (want base " & p.contentType & ")")
  case p.kind
  of pkJson:
    var got, want: JsonNode
    try: got = parseJson(res.body)
    except CatchableError as e:
      jsFail(label, p.name & ": echoed body is not JSON (" & e.msg & ")"); return
    want = parseJson(p.jsonSrc)
    if got != want: jsFail(label, p.name & ": JSON tree mismatch")
    if p.reserialized and res.body == $want:
      jsFail(label, p.name & ": server byte-echoed (canonical echo == sent bytes)")
  of pkForm:
    if not checkFormEcho(res.body, p.form):
      jsFail(label, p.name & ": form pairs mismatch (echoed '" & res.body & "')")
  of pkText, pkBinary:
    if res.body != p.text:
      jsFail(label, p.name & ": body mismatch (expected " & $p.text.len &
        " got " & $res.body.len & ")")

proc requestsWorker(api: Navi, label: string, pool: JsPool,
                    payloads: seq[Payload], counter: JsCounter,
                    deadline: float, i: int) {.async.} =
  var n = i
  while nowMs() < deadline:
    let v = verbs[n mod verbs.len]
    let p = payloads[(n div verbs.len) mod payloads.len]
    let bodied = v in {POST, PUT, PATCH}
    inc n
    var h = initHeaders()
    # No x-want-encoding: the js cell leaves response compression to the
    # runtime's own codec, so it never asks the server to br/zstd-encode a body
    # undici might not decode.
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
        jsFail(label, p.name & " " & $v & " -> status " & $res.status)
      if res.headers.get("x-echo-method") != $v:
        jsFail(label, p.name & " " & $v & " echoed method '" &
          res.headers.get("x-echo-method") & "'")
      if res.headers.get("x-echo-stress") != "1":
        jsFail(label, p.name & " " & $v & " middleware header not echoed")
      if bodied: verifyEcho(label, p, v, res)
      elif res.body.len != 0:
        jsFail(label, p.name & " " & $v & " bodiless verb returned a body")
      counter.tally(res.status)
    except CatchableError as e:
      counter.note()
      jsFail(label, p.name & " " & $v & " -> " & $e.name & ": " & e.msg)
