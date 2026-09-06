## SSE reconnect interop (navi/asyncdispatch over HTTP/2).
##
## /events drops the connection after 3 events per request, so receiving all 10 in
## order -- no gaps or duplicates -- requires transparent reconnection with
## Last-Event-ID resume. /stall instead goes silent after each 3-event batch with
## the h2 stream still open (issue #229's wedge): with all timeouts off the read
## would park forever, so recovering here proves the SSE idle timeout catches the
## wedge and reconnects. Driven by docker-compose against the FastAPI SSE server.
import std/os
import navi/asyncdispatch

const Total = 10

proc collect(api: Navi, path: string, idleTimeoutMs = 45_000, retryMs = 3000):
    Future[tuple[datas, ids: seq[string]]] {.async.} =
  let s = await api.sse(getEnv("BASE") & path, idleTimeoutMs = idleTimeoutMs,
                        retryMs = retryMs)
  s.each(ev):
    result.datas.add ev.data
    result.ids.add ev.id
    if result.datas.len >= Total: break    # break works: `each` is a real loop
  await s.close()                          # tears down the SSE client's mux too

proc check(label: string, r: tuple[datas, ids: seq[string]]) =
  doAssert r.datas.len == Total, label & ": expected " & $Total & " events, got " & $r.datas.len
  for i in 0 ..< Total:
    doAssert r.ids[i] == $(i + 1), label & ": id gap/dup at index " & $i & ": got '" & r.ids[i] & "'"
    doAssert r.datas[i] == "event-" & $(i + 1), label & ": data mismatch at index " & $i

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false                   # server uses a self-signed cert
  let api = newNavi(cfg)

  check("reconnect", await api.collect("/events"))
  echo "ok: ", Total, " SSE events across clean reconnects (Last-Event-ID resumed)"

  # #229: the stream goes silent (no END_STREAM) after each batch; a short idle
  # timeout must catch the wedged read and reconnect instead of hanging forever.
  check("wedge", await api.collect("/stall", idleTimeoutMs = 700, retryMs = 100))
  echo "ok: ", Total, " SSE events recovered from a wedged read via the idle timeout"

  await api.close()

waitFor main()
