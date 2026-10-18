## Cooperative event-loop fairness for the async backends (issue #462).
##
## Both async runtimes resume an `await` on an already-finished future in place,
## without going back to the event loop. A connection whose peer keeps the socket
## readable (a download or an SSE feed arriving faster than it is consumed), or
## whose send buffer keeps accepting writes (an upload), therefore completes read
## after read, write after write, synchronously: the coroutine driving it never
## parks, and nothing else on the loop runs until the kernel buffer finally runs
## dry. In the mixed stress cell four such h1 transfers held the loop for 165 ms on
## average per turn (a 10 ms sleep took 175 ms), which is what collapsed the
## buffered `/echo` requests sharing that loop to about 1/50 of their solo rate.
##
## Where the in-place completions come from: asyncdispatch's TLS path calls
## SSL_read/SSL_write on the fd and only parks on WANT_READ/WANT_WRITE (its plaintext
## `recv`/`send` always park on the selector). On chronos it is the stream
## transport: `fastWrite` (every chronos 4.x) writes straight into a writable socket,
## and chronos 4.4's `fastRead` reads straight off a readable one once the transport
## has paused reading, so a read completes in place even though the transport's own
## buffer is only 4 KiB. That covers the TLS pump and the plaintext reader alike.
## chronos 4.0 and 4.2 (also allowed by `chronos >= 4.0.0`) have no `fastRead`: there
## a read parks whenever that buffer is empty and only the write side runs in place.
##
## The bound is per I/O call: the slice is checked when a read starts or a write
## ends, so a single call that does a lot of work (one `sendAll` of a large in-memory body,
## encrypted in one SSL_write) still runs to completion. HTTP/3 does not go through
## `recvSome`/`sendAll` and is not covered.
##
## The fix is a time slice shared by every connection on the thread. `sliceSpent`
## reports that the loop has gone `naviFairSliceUs` without regaining control,
## and the transport's read/write entry points then yield once (`yieldSlice` in
## each backend) before doing more I/O. Any real suspension resets the slice via
## `sliceRestart`: a coroutine resuming from a parked future proves the loop just
## ran. A read yields BEFORE it starts, never between taking bytes off the
## transport and handing them back, so a cancellation landing on the yield
## (chronos) cannot lose data. A write yields AFTER it completes: issuing it first,
## synchronously, keeps concurrent un-awaited writes on one connection in call order
## (a WebSocket's automatic pong or Close reply behind a user frame), and a cancel on
## that trailing yield finds the bytes already written. A caller that commits
## protocol state before it writes (the h2 mux records a stream as on the wire) runs
## the yield itself (`fairYield`) ahead of that commit and passes
## `sendAll(..., fair = false)`.
##
## Cycle-free: standard library only, so both backends import it.

import std/[monotimes, times]

const naviFairSliceUs* {.intdefine.} = 2000
  ## How long, in microseconds, connection I/O may run back to back without the
  ## event loop getting a turn. Small enough that a buffered request queued behind
  ## a few saturated streams waits single-digit milliseconds per loop round; large
  ## enough that the extra poll it costs is noise next to the bytes moved in the
  ## slice. `-d:naviFairSliceUs=0` yields at every I/O call that follows a
  ## synchronous completion.

var sliceStart {.threadvar.}: MonoTime
  ## When the event loop last had control, as far as this thread's connections can
  ## tell. One event loop per thread, so one slice per thread.

proc sliceRestart*() {.inline.} =
  ## The loop just ran: a coroutine resumed from a real suspension, or a fairness
  ## yield completed. Start a fresh slice.
  sliceStart = getMonoTime()

proc sliceSpent*(): bool {.inline.} =
  ## Whether connection I/O has held the loop for a whole slice.
  (getMonoTime() - sliceStart).inMicroseconds >= naviFairSliceUs
