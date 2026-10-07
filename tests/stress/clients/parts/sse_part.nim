# The verified SSE consume loop, shared by clients/sse.nim and the sse slice of
# clients/mixed.nim. `include`d (not imported) AFTER the including file's
# `import navi[/backend]` + `include ../common/httpset`, like common/httpset and
# common/chaos, because SseStream/Future come from that backend. Not a standalone
# module. Needs std/[times, strutils] and ../common/[config, reporter] from the
# including file.
#
# Each event tallies and is discarded; the server's periodic drop exercises
# navi's transparent reconnect, and a gap or duplicate in the monotonic event ids
# across that reconnect is a Last-Event-ID resume bug and hard-fails. The worker
# breaks inside the `each` body and closes its own stream afterwards, never
# mid-read (closing under a parked h2 read orphans the read's future and crashes
# the dispatcher at teardown). The VersionGate is the h3 allowance: an h3 stream
# legitimately starts on h2 until the Alt-Svc reconnect.

proc sseWorker(cfg: Config, s: SseStream, counter: StatusCounter,
            gate: ptr VersionGate, deadline: float) {.async.} =
  var lastId = 0
  try:
    s.each(ev):
      counter.tally(200)
      gate[].sample(s.httpVersion)        # track the negotiated version (h3 after upgrade)
      # Verify Last-Event-ID resume: the server numbers events monotonically and
      # resumes at last+1 after each periodic drop, so a gap or duplicate id (across
      # a reconnect) is a resume bug, not soak noise.
      if ev.id.len > 0:
        let id = try: parseInt(ev.id) except ValueError: -1
        if id < 0:
          cfg.failHard("non-numeric SSE id '" & ev.id & "'")
        if lastId != 0 and id != lastId + 1:
          cfg.failHard("SSE id discontinuity: expected " & $(lastId + 1) &
            ", got " & $id & " (Last-Event-ID resume broken)")
        lastId = id
      if epochTime() >= deadline: break   # self-terminate: events flow continuously
  except CatchableError:
    counter.fail()
  try: await s.close()                     # closed here, not mid-read: clean teardown
  except CatchableError: discard
