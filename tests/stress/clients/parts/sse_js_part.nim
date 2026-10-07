# The verified SSE consume loop for the navi/js backend, shared by
# clients/sse_js.nim and the sse slice of clients/mixed_js.nim. `include`d (not
# imported) AFTER the including file's `import navi/js`, like the native parts.
# Not a standalone module. Needs std/strutils and ../common/harness_js from the
# including file.
#
# Each event tallies and is discarded; the server's periodic drop exercises
# navi's reconnect, and a gap or duplicate in the monotonic event ids across
# that reconnect is a Last-Event-ID resume bug and hard-fails. The worker breaks
# inside the `each` body and closes its own stream afterwards, never mid-read,
# mirroring the native client.

proc sseWorker(label: string, s: SseStream, counter: JsCounter,
               deadline: float) {.async.} =
  var lastId = 0
  try:
    s.each(ev):
      counter.tally(200)
      if ev.id.len > 0:              # verify Last-Event-ID resume continuity
        let id = try: parseInt(ev.id) except ValueError: -1
        if id < 0 or (lastId != 0 and id != lastId + 1):
          jsFail(label, "SSE id discontinuity: expected " & $(lastId + 1) &
            ", got " & ev.id & " (Last-Event-ID resume broken)")
        lastId = id
      if nowMs() >= deadline: break
  except CatchableError:
    counter.note()
  s.close()                          # closed after the loop, not mid-read
