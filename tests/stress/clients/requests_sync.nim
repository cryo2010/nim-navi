## stressRequests, sync backend (`import navi`). The sync client is single
## in-flight by nature: it loops the clients round-robin, firing GET/POST/PUT at
## /echo across the server pool until the deadline. Same consume-and-discard +
## per-report cadence as the async client, minus the fan-out (documented: sync has
## no concurrency).

import std/[times, strutils, json]
import ../zlibcodec
import ../common/[config, reporter, servers, payloads, leakcheck]
import navi
include ../common/httpset
include ../common/chaos

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]
const allPayloads = stressPayloads()

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) =
    ctx.req.headers["x-stress"] = "1"
    ctx.next()

proc failHard(cfg: Config, msg: string) =
  stderr.writeLine cfg.label & " FAIL: " & msg
  quit(1)

proc baseType(ct: string): string =
  ## The media type before any `;` parameter, lowercased/trimmed, for the mirror check.
  ct.split(';', 1)[0].strip().toLowerAscii()

proc verifyEcho(cfg: Config, p: Payload, v: HttpVerb, res: Response) =
  ## Per-kind verification: json by parsed-tree equality, form by decoded pairs,
  ## text/binary byte-exact. Every failure names p.name.
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
    if p.reserialized and res.body == $want:
      cfg.failHard(p.name & ": server byte-echoed (canonical echo == sent bytes)")
  of pkForm:
    if not checkFormEcho(res.body, p.form):
      cfg.failHard(p.name & ": form pairs mismatch (echoed '" & res.body & "')")
  of pkText, pkBinary:
    if res.body != p.text:
      cfg.failHard(p.name & ": body mismatch (expected " & $p.text.len &
        " bytes, got " & $res.body.len & ")")

proc featureChecks(cfg: Config, base: string) =
  ## Once-per-cell checks of paths the /echo soak never hits: redirect following,
  ## error-status handling, the cookie jar, and Basic auth -- under the pinned protocol.
  var ec = initNaviConfig()
  ec.http = httpVersions(cfg.proto)
  ec.tls.caFile = cfg.cert
  ec.throwHttpErrors = false
  let api = newNavi(ec)
  block:
    let r = api.request(GET, base & "/redirect/3")
    if r.status != 200 or r.body != "redirect-done":
      cfg.failHard("redirect: status " & $r.status & " body '" & r.body & "'")
  for code in [404, 503]:
    if api.request(GET, base & "/status/" & $code).status != code:
      cfg.failHard("status " & $code & " not surfaced")
  discard api.request(GET, base & "/setcookie")
  if api.request(GET, base & "/needs-cookie").status != 200:
    cfg.failHard("cookie jar not carried back to the origin")
  var ac = initNaviConfig()
  ac.http = httpVersions(cfg.proto)
  ac.tls.caFile = cfg.cert
  ac.auth = basicAuth("stress", "secret")
  if newNavi(ac).request(GET, base & "/needs-auth").status != 200:
    cfg.failHard("basic auth rejected")

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  c.middleware = @[stampMw()]
  newNavi(c)

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  let notice = cfg.chaosSkipNotice
  if notice.len > 0: echo cfg.label, " ", notice
  var leakBase = sampleBaseline(cfg.chaos)   # before ANY Navi is constructed
  var pool = initServerPool(cfg)
  let payloads = filterPayloads(allPayloads, cfg.contentTypes, jsSafe = false)
  let counter = newStatusCounter()
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  # Warm up per-origin protocol discovery (h3 needs an Alt-Svc round-trip) so the
  # measured phase is pinned from the first request; then a downgrade fails hard.
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for api in apis:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if api.request(GET, base & "/echo").httpVersion == expect: break
          except CatchableError: break

  featureChecks(cfg, pool.all()[0])   # redirect/status/auth/cookie coverage

  let start = epochTime()
  let deadline = start + cfg.seconds
  # sync interleaves one chaos request per M verified requests (no fan-out). M ~=
  # NAVI_CHAOS_CONC so the chaos:verified ratio roughly matches the async cells.
  var sc = syncChaosStart(cfg, leakBase)   # no-op when chaos is off
  let chaosEvery = max(1, cfg.chaos.conc)
  var lastReport = start
  var n = 0
  while epochTime() < deadline:
    for api in apis:
      # Two-index rotation: every payload crosses every bodied verb (the old
      # phase-locked pairing shipped body k only with verb k, so big bodies always
      # landed on bodiless verbs and never went on the wire).
      let v = verbs[n mod verbs.len]
      let p = payloads[(n div verbs.len) mod payloads.len]
      let bodied = v in {POST, PUT, PATCH}
      inc n
      let url = pool.pick() & "/echo"
      var h = initHeaders()
      if n mod 11 == 0: h["x-big"] = repeat("H", 8192)   # exercise the HPACK path
      # Under chaos, tag bodied requests with an Idempotency-Key so navi replays a
      # load-induced keep-alive race (see the async client for the rationale).
      # Byte-identical no-op when chaos is off.
      if bodied and cfg.chaos.enabled:
        h["idempotency-key"] = "stress-" & $n
      try:
        var res: Response
        if not bodied:
          res = api.request(v, url, headers = h)
        else:
          if cfg.respCompression != "none":
            h["x-want-encoding"] = cfg.respCompression
          case p.kind
          of pkJson:
            # Typed JsonNode -> application/json, uncompressed (a content-encoding
            # would misdescribe the plain bytes). Parse per request from the source.
            res = api.request(v, url, headers = h, body = parseJson(p.jsonSrc))
          of pkForm:
            res = api.request(v, url, headers = h, form = p.form)
          of pkText, pkBinary:
            var wire = p.text
            h["content-type"] = p.contentType
            if cfg.reqCompression != "none" and p.text.len > 0:
              wire = zcompress(p.text, cfg.reqCompression)
              h["content-encoding"] = cfg.reqCompression
            res = api.request(v, url, headers = h, body = wire)
        cfg.checkVersion(res.httpVersion)   # hard-fail on a silent protocol downgrade
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
        cfg.failHard(p.name & " " & $v & " " & url & " -> " & $e.name & ": " & e.msg)
      if sc.active and n mod chaosEvery == 0: syncChaosStep(sc)   # interleave chaos
    if epochTime() - lastReport >= cfg.reportSeconds.float:
      lastReport = epochTime()
      report(cfg.label, counter, epochTime() - start)
      syncChaosReport(sc)

  if counter.ops == 0: cfg.failHard("no request completed")
  report(cfg.label & " final", counter, epochTime() - start)
  syncChaosFinish(sc, apis)
  echo "== requests sync ", cfg.proto, " passed (", counter.ops, " ops) =="

main()
