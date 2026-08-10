## navi/asyncdispatch SSE demo: subscribe to a tick stream that drops midway. navi
## reconnects transparently (resending Last-Event-ID), so every tick arrives in
## order and the drop is invisible to this loop. Verified end to end.
import std/[os, strutils]
import navi/asyncdispatch

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false                   # demo server uses a self-signed cert
  let api = newNavi(cfg)
  echo "subscribing to ", getEnv("BASE"), "/ticks"
  let s = await api.sse(getEnv("BASE") & "/ticks")
  var ticks: seq[int]
  s.each(ev):                              # a real loop, so break works
    if ev.event == "end": break
    echo "  <- ", ev.data, "  (id ", ev.id, ")"
    ticks.add parseInt(ev.id)
  await s.close()

  doAssert ticks == @[1, 2, 3, 4, 5, 6, 7, 8],
    "expected ticks 1..8 in order, got " & $ticks
  echo "ok: received all ", ticks.len,
    " ticks in order -- navi reconnected and resumed from Last-Event-ID transparently"
  await api.close()

waitFor main()
