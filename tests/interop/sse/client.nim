## SSE reconnect interop (navi/asyncdispatch over HTTP/2).
##
## The server drops the connection after 3 events per request, so receiving all 10
## events in order -- with no gaps or duplicates -- requires transparent
## reconnection with Last-Event-ID resume. That is exactly what this asserts, over
## the h2 mux. Driven by docker-compose against the FastAPI SSE server.
import std/os
import navi/asyncdispatch

const Total = 10

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false                   # server uses a self-signed cert
  let api = newNavi(cfg)
  let s = await api.sse(getEnv("BASE") & "/events")
  var datas, ids: seq[string]
  s.each(ev):
    datas.add ev.data
    ids.add ev.id
    if datas.len >= Total: break           # break works: `each` is a real loop
  await s.close()                          # tears down the SSE client's mux too

  doAssert datas.len == Total, "expected " & $Total & " events, got " & $datas.len
  for i in 0 ..< Total:
    doAssert ids[i] == $(i + 1), "id gap/dup at index " & $i & ": got '" & ids[i] & "'"
    doAssert datas[i] == "event-" & $(i + 1), "data mismatch at index " & $i
  echo "ok: ", Total,
    " SSE events received in order across reconnects (Last-Event-ID resumed)"
  await api.close()

waitFor main()
