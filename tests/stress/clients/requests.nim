## stressRequests, async backends (one source, built twice):
##   nim c -d:ssl ...                -> navi/asyncdispatch
##   nim c -d:ssl -d:useChronos ...  -> navi/chronos
##
## A buffered request/response soak: `clients` navi clients, each fanning out
## `concurrency` workers that loop every verb against `/echo` across the server pool
## until the deadline. A middleware stamps x-stress (exercises the chain). Each
## response is fully verified -- status is 200, x-echo-method matches the verb,
## x-echo-stress is echoed, and the body matches per content-type kind: octet/text
## byte-exact (decompressed), json by parsed-tree equality, form by decoded-pair
## equality (so a server byte-echo cannot make json/form pass). Bodies rotate
## through octet/text/json/form (see common/payloads) via a two-index verb x payload
## cross product, restricted by NAVI_CONTENT_TYPES. A verification miss or transport
## error FAILS HARD. The protocol is pinned via config.http, so a silent downgrade
## also fails.

import std/[times, strutils, json]
import ../zlibcodec
import ../common/[config, reporter, servers, payloads]

when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]
const allPayloads = stressPayloads()   # compile-time: gcsafe const for the chronos build

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"
    await ctx.next()

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)     # pin the cell's protocol (strict: no downgrade)
  c.tls.caFile = cfg.cert
  c.middleware = @[stampMw()]
  newNavi(c)

proc failHard(cfg: Config, msg: string) =
  {.cast(gcsafe).}:
    stderr.writeLine cfg.label & " FAIL: " & msg
  quit(1)

proc baseType(ct: string): string =
  ## The media type before any `;` parameter (Starlette appends `; charset=utf-8`
  ## to text/* responses), lowercased and trimmed, for the content-type mirror check.
  ct.split(';', 1)[0].strip().toLowerAscii()

proc verifyEcho(cfg: Config, p: Payload, v: HttpVerb, res: Response) =
  ## Per-kind verification of a bodied /echo response. JSON/form compare parsed
  ## values (sidestepping cross-language formatting); text/binary stay byte-exact.
  ## Every failure names p.name so a soak failure identifies the payload shape.
  if baseType(res.headers.get("content-type")) != p.contentType:
    cfg.failHard(p.name & ": echoed content-type '" &
      res.headers.get("content-type") & "' (want base " & p.contentType & ")")
  case p.kind
  of pkJson:
    var got, want: JsonNode
    try: got = parseJson(res.body)
    except CatchableError as e:
      cfg.failHard(p.name & ": echoed body is not JSON (" & e.msg & ")"); return
    want = parseJson(p.jsonSrc)
    if got != want:
      cfg.failHard(p.name & ": JSON tree mismatch")
    # -- Prove the server parsed rather than byte-echoed: for an unsorted-key doc the
    # -- sorted-key canonical echo cannot equal the bytes navi put on the wire.
    if p.reserialized and res.body == $want:
      cfg.failHard(p.name & ": server byte-echoed (canonical echo == sent bytes)")
  of pkForm:
    if not checkFormEcho(res.body, p.form):
      cfg.failHard(p.name & ": form pairs mismatch (echoed '" & res.body & "')")
  of pkText, pkBinary:
    if res.body != p.text:
      cfg.failHard(p.name & ": body mismatch (expected " & $p.text.len &
        " bytes, got " & $res.body.len & ")")

proc worker(api: Navi, cfg: Config, pool: ptr ServerPool, payloads: seq[Payload],
            counter: StatusCounter, deadline: float, i: int) {.async.} =
  var n = i
  while epochTime() < deadline:
    # Two-index rotation: every payload crosses every bodied verb (the old
    # phase-locked `[n mod 7]` pairing shipped body k only with verb k, so the big
    # bodies always landed on bodiless verbs and never went on the wire).
    let v = verbs[n mod verbs.len]
    let p = payloads[(n div verbs.len) mod payloads.len]
    let bodied = v in {POST, PUT, PATCH}
    inc n
    let url = pool[].pick() & "/echo"
    var h = initHeaders()
    if n mod 11 == 0:                    # occasionally a big header value -> HPACK path
      h["x-big"] = repeat("H", 8192)
    try:
      var res: Response
      if not bodied:
        res = await api.request(v, url, headers = h)
      else:
        if cfg.respCompression != "none":
          h["x-want-encoding"] = cfg.respCompression
        case p.kind
        of pkJson:
          # Typed JsonNode: navi sets application/json and puts uncompressed JSON on
          # the wire (a content-encoding would misdescribe the bytes). Parse per
          # request from the source string -- deterministic, and keeps the catalog const.
          res = await api.request(v, url, headers = h, body = parseJson(p.jsonSrc))
        of pkForm:
          # Typed form: navi sets application/x-www-form-urlencoded, uncompressed.
          res = await api.request(v, url, headers = h, form = p.form)
        of pkText, pkBinary:
          var wire = p.text
          h["content-type"] = p.contentType
          if cfg.reqCompression != "none" and p.text.len > 0:
            wire = zcompress(p.text, cfg.reqCompression)
            h["content-encoding"] = cfg.reqCompression
          res = await api.request(v, url, headers = h, body = wire)
      cfg.checkVersion(res.httpVersion)  # hard-fail on a silent protocol downgrade
      if res.status != 200:
        cfg.failHard(p.name & " " & $v & " " & url & " -> status " & $res.status)
      if res.headers.get("x-echo-method") != $v:
        cfg.failHard(p.name & " " & $v & ": echoed method '" &
          res.headers.get("x-echo-method") & "'")
      if res.headers.get("x-echo-stress") != "1":
        cfg.failHard(p.name & " " & $v & ": middleware header not echoed")
      if bodied: cfg.verifyEcho(p, v, res)
      elif res.body.len != 0:
        cfg.failHard(p.name & " " & $v & ": bodiless verb returned a body")
      counter.tally(res.status)
    except CatchableError as e:
      counter.fail()
      # A surfaced transport error means navi could not handle the request (a
      # replayable failure is retried internally and never reaches here), so treat
      # it as a bug to investigate, not soak noise to tally.
      cfg.failHard(p.name & " " & $v & " " & url & " -> " & $e.name & ": " & e.msg)

proc featureChecks(cfg: Config, base: string) {.async.} =
  ## Once-per-cell checks of paths the /echo soak never hits: redirect following,
  ## error-status handling, the cookie jar, and Basic auth -- all under the pinned
  ## protocol. Closes the redirect/status/auth/cookie coverage gaps.
  var ec = initNaviConfig()
  ec.http = httpVersions(cfg.proto)
  ec.tls.caFile = cfg.cert
  ec.throwHttpErrors = false            # inspect 4xx/5xx instead of raising
  let api = newNavi(ec)
  block:
    let r = await api.request(GET, base & "/redirect/3")   # 3 hops -> 200
    if r.status != 200 or r.body != "redirect-done":
      cfg.failHard("redirect: status " & $r.status & " body '" & r.body & "'")
  for code in [404, 503]:
    if (await api.request(GET, base & "/status/" & $code)).status != code:
      cfg.failHard("status " & $code & " not surfaced")
  discard await api.request(GET, base & "/setcookie")       # jar records the cookie
  if (await api.request(GET, base & "/needs-cookie")).status != 200:
    cfg.failHard("cookie jar not carried back to the origin")
  var ac = initNaviConfig()
  ac.http = httpVersions(cfg.proto)
  ac.tls.caFile = cfg.cert
  ac.auth = basicAuth("stress", "secret")
  if (await newNavi(ac).request(GET, base & "/needs-auth")).status != 200:
    cfg.failHard("basic auth rejected")

proc reporterLoop(cfg: Config, counter: StatusCounter,
                  start, deadline: float) {.async.} =
  var last = start
  while epochTime() < deadline:
    await sleep(1000)                   # 1s granularity: stop within ~1s of the deadline
    if epochTime() - last >= cfg.reportSeconds.float:
      last = epochTime()
      report(cfg.label, counter, epochTime() - start)

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  let payloads = filterPayloads(allPayloads, cfg.contentTypes, jsSafe = false)
  let counter = newStatusCounter()
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  # Warm up each client's per-origin protocol so the measured phase runs pinned from
  # the first request: h3 is discovered via an initial Alt-Svc round-trip, so hit
  # each origin until the expected version is negotiated. After this, any downgrade
  # during the soak fails hard (checkVersion), catching a silent fallback.
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for api in apis:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if (await api.request(GET, base & "/echo")).httpVersion == expect: break
          except CatchableError: break

  await featureChecks(cfg, pool.all()[0])   # redirect/status/auth/cookie coverage

  let start = epochTime()
  let deadline = start + cfg.seconds
  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add worker(api, cfg, addr pool, payloads, counter, deadline, i)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f

  if counter.ops == 0: cfg.failHard("no request completed")   # a cell must do work
  report(cfg.label & " final", counter, epochTime() - start)
  echo "== requests ", backend, " ", cfg.proto, " passed (", counter.ops, " ops) =="

waitFor main()
