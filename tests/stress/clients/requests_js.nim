## stressRequests, navi/js backend (Node). `clients` x `concurrency` workers rotate
## every verb x payload against /echo across the server pool, tally status codes,
## and verify each echo per content-type kind (json by parsed-tree equality, form by
## decoded pairs, text byte-exact). The js cell is the js-safe subset of the shared
## catalog: binary payloads are excluded (TextEncoder mangles raw bytes) and there is
## no request-body compression (the js runtime owns its codec). The runner trusts the
## self-signed cert via NODE_EXTRA_CA_CERTS.
##
## The verified loop itself lives in parts/requests_js_part, shared with
## clients/mixed_js.nim.

import std/[strutils, json]
import navi/js
import ../common/[harness_js, payloads]
include parts/requests_js_part   # the verified /echo loop (shared with mixed_js.nim)

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
  for _ in 0 ..< cfg.clients: apis.add mkJsClient()

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  let label = "[requests " & cfg.proto & " js]"
  let timer = setIntervalJs(proc () = counter.report(label, start),
                            cfg.reportSeconds * 1000)

  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add requestsWorker(api, label, pool, payloads, counter, deadline, i)
  for f in futs: await f
  clearIntervalJs(timer)

  if counter.ops == 0: jsFail(label, "no request completed")
  let elapsed = (nowMs() - start) / 1000.0   # the measured phase: the ops/s divisor
  counter.report(label, start, final = true)
  echo "== requests js ", cfg.proto, " passed (", counter.ops, " ops, ",
    fmtRate(counter.ops, elapsed), " ops/s) =="

discard main()
