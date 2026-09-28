## Sans-io Server-Sent Events (`text/event-stream`) parser, following the WHATWG
## EventSource "interpreting an event stream" rules.
##
## Fed decoded UTF-8 text incrementally (bytes in, events out); it owns no sockets
## and does no reconnection. The entries drive it over a streaming response and
## surface the events. Line splitting is on ASCII newlines, so feeding raw UTF-8
## is safe even when a multi-byte character straddles a chunk boundary.
##
## The `id` on an event is the persistent "last event id" (it survives across
## dispatches until a new `id:` changes it), which is what a reconnect resends as
## `Last-Event-ID`. Per the spec an `id:` line only fills a buffer; that buffer is
## promoted to the persistent id when the event is dispatched (the blank line), so a
## partial event cut off before its blank line never advances the resume point.
## `reset` clears the per-connection parse state but keeps that id and the retry,
## for use across a reconnect.

import std/[deques, strutils, options]   # strutils for `delete(string, HSlice)`

const maxSseEventBytes* = 16 * 1024 * 1024
  ## A single event's accumulated data (or one unterminated line) may not exceed
  ## this. SSE streams run with `maxResponseBytes` off (they are long-lived), so
  ## without this bound a server that never sends a newline, or an endless run of
  ## `data:` lines, would grow the parser's buffers without limit (memory DoS).

type
  SseEvent* = object
    event*: string     ## event type; "message" if the stream did not set one
    data*: string      ## payload (the `data:` lines joined with "\n")
    id*: string        ## the persistent last event id in effect at dispatch
    retry*: int        ## reconnect delay in ms in effect, or -1 if never set

  SseParser* = object
    buf: string              ## text past the last processed line terminator
    scanned: int             ## bytes of `buf` already scanned for a terminator, so
                             ## a long unterminated line is not rescanned per feed
                             ## (bounds streamed input to O(n), not O(n^2))
    ready: Deque[SseEvent]   ## dispatched events awaiting `next`
    evType: string           ## current event's type buffer
    data: string             ## current event's data, `data:` lines joined with "\n"
    hasData: bool            ## whether any `data:` line was seen (fires even if empty)
    dataBytes: int           ## running size of `data`, to bound one event
    idBuf: string            ## last event id buffer: what `id:` fills, promoted to
                             ## `lastId` only at dispatch
    lastId: string           ## persistent last event id (survives dispatch)
    retry: int               ## last `retry:` value seen, or -1
    atStart: bool            ## until the first byte, to strip a leading BOM

proc initSseParser*(lastEventId = ""): SseParser =
  ## A fresh parser. `lastEventId` seeds the resume id (for a stream opened with a
  ## caller-supplied Last-Event-ID).
  SseParser(retry: -1, atStart: true, idBuf: lastEventId, lastId: lastEventId)

proc addRange(dst: var string, src: string, first, n: int) {.inline.} =
  ## Append `src[first ..< first + n]` to `dst` without materializing the slice as
  ## its own string first. A Nim string slice is an allocation plus a byte-at-a-time
  ## loop, and every byte of an SSE stream passes through here, so the temporary is
  ## pure overhead (the same shape as the h1 parser's `addRange`).
  if n <= 0: return
  let start = dst.len
  dst.setLen(start + n)
  when defined(js):                           # no copyMem (and no pointers) on js
    for i in 0 ..< n: dst[start + i] = src[first + i]
  else:
    copyMem(addr dst[start], unsafeAddr src[first], n)

proc setRange(dst: var string, src: string, first, n: int) {.inline.} =
  ## `dst = src[first ..< first + n]`, keeping `dst`'s capacity.
  dst.setLen(0)
  dst.addRange(src, first, n)

proc fieldIs(buf: string, first, last: int, name: string): bool {.inline.} =
  ## Whether `buf[first ..< last]` is exactly `name`, compared in place so a field
  ## name is never copied out of the parse buffer just to be matched.
  if last - first != name.len: return false
  for i in 0 ..< name.len:
    if buf[first + i] != name[i]: return false
  true

proc dispatch(p: var SseParser) =
  ## End of an event (a blank line). Fire it only if it accumulated data; either
  ## way reset the per-event buffers. The last-id buffer persists across events.
  ##
  ## Promoting the id buffer is the first step, ahead of the no-data early return:
  ## an `id:` line with no `data:` still moves the resume point (so a server can
  ## checkpoint without sending a payload), while an event still being received when
  ## the connection drops never reaches here and so cannot advance it -- which is
  ## what keeps a reconnect from skipping the events it never saw.
  ##
  ## The per-event buffers are moved into the event, not copied: an event at the
  ## 16 MiB cap would otherwise be copied once more on its way out.
  p.lastId = p.idBuf
  if not p.hasData:
    p.evType.setLen(0)
    return
  var ev = SseEvent(id: p.lastId, retry: p.retry)
  ev.event = if p.evType.len == 0: "message" else: move(p.evType)
  ev.data = move(p.data)
  p.ready.addLast(move(ev))
  p.evType.setLen(0)
  p.data.setLen(0)
  p.hasData = false
  p.dataBytes = 0

proc processLine(p: var SseParser, first, last: int) =
  ## Handle the line `p.buf[first ..< last]` (terminator excluded). The line is
  ## addressed in place: the colon and the optional single leading space are located
  ## in the buffer, the field name is matched without being copied, and only the
  ## value is copied, once, straight into the field it belongs to.
  if last <= first:
    p.dispatch()
    return
  if p.buf[first] == ':': return             # comment line: ignored (keep-alive)
  var c = first
  while c < last and p.buf[c] != ':': inc c  # only the first colon splits
  var vs = if c < last: c + 1 else: last     # no colon: the whole line is the field
  if vs < last and p.buf[vs] == ' ': inc vs  # strip one leading space
  let n = last - vs
  if p.buf.fieldIs(first, c, "data"):
    if p.hasData: p.data.add '\n'            # separator between successive data lines
    p.data.addRange(p.buf, vs, n)
    p.hasData = true
    p.dataBytes += n + 1                     # +1 for the "\n" join separator
    if p.dataBytes > maxSseEventBytes:
      raise newException(ValueError,
        "navi: SSE event exceeds the " & $maxSseEventBytes & "-byte limit")
  elif p.buf.fieldIs(first, c, "event"):
    p.evType.setRange(p.buf, vs, n)
  elif p.buf.fieldIs(first, c, "id"):
    for i in vs ..< last:
      if p.buf[i] == '\0': return            # an id containing NUL is ignored
    p.idBuf.setRange(p.buf, vs, n)
  elif p.buf.fieldIs(first, c, "retry"):
    # All-digit but possibly out of int range; ignore an unparseable value rather
    # than crash the stream. Accumulated in place, with the range check `parseInt`
    # would make, so a handful of digits costs no slice.
    if n <= 0: return
    var v = 0
    for i in vs ..< last:
      let d = ord(p.buf[i]) - ord('0')
      if d < 0 or d > 9: return              # not all digits: ignored
      if v > (int.high - d) div 10: return   # out of int range: ignored
      v = v * 10 + d
    p.retry = v
  else: discard                              # unknown field: ignored

proc feed*(p: var SseParser, text: string) =
  ## Feed a chunk of decoded text. Complete events become available via `next`.
  p.buf.add text
  if p.atStart:
    # A leading UTF-8 BOM (EF BB BF) must be stripped, but the first feed may carry
    # fewer than 3 bytes: wait until enough have accumulated to decide, rather than
    # clearing `atStart` early and leaving the BOM in the stream (which would break
    # the first event's field-name match). Nothing is scanned until then anyway --
    # a 1-2 byte prefix can't complete a line.
    if p.buf.len < 3: return
    if p.buf[0] == '\xEF' and p.buf[1] == '\xBB' and p.buf[2] == '\xBF':
      p.buf.delete(0 ..< 3)                   # strip a single leading UTF-8 BOM
    p.atStart = false
  var i = p.scanned                           # resume; earlier bytes had no terminator
  var lineStart = 0
  while i < p.buf.len:
    case p.buf[i]
    of '\n':
      p.processLine(lineStart, i)
      inc i; lineStart = i
    of '\r':
      if i == p.buf.len - 1: break            # maybe a \r\n split across feeds: wait
      p.processLine(lineStart, i)
      if p.buf[i + 1] == '\n': inc i          # consume the \n of a \r\n terminator
      inc i; lineStart = i
    else: inc i
  if lineStart > 0:
    # Drop the consumed prefix in place (no realloc): setLen(0) when a feed ends
    # exactly on a terminator (the steady state), else memmove the small tail down.
    if lineStart >= p.buf.len: p.buf.setLen(0)
    else: p.buf.delete(0 ..< lineStart)
    p.scanned = 0                             # the small tail is rescanned next feed
  else:
    p.scanned = i                             # no terminator yet; don't rescan this run
  if p.buf.len > maxSseEventBytes:            # a single line with no terminator
    raise newException(ValueError,
      "navi: SSE line exceeds the " & $maxSseEventBytes & "-byte limit")

proc next*(p: var SseParser): Option[SseEvent] =
  ## The next dispatched event, or none if none are ready yet.
  if p.ready.len > 0: some(p.ready.popFirst()) else: none(SseEvent)

proc retryMs*(p: SseParser): int = p.retry
  ## The most recent `retry:` value (ms), or -1 if the stream never sent one.

proc lastEventId*(p: SseParser): string = p.lastId
  ## The persistent last event id, to resend as Last-Event-ID on reconnect.

# --- reconnect delay policy (shared by every client's reconnect loop) ---

const defaultSseMinRetryMs* = 100
  ## Default floor, in milliseconds, under every SSE reconnect delay. Without one a
  ## server that sends `retry: 0`, or that answers 200 and closes with no events,
  ## turns the reconnect loop into a busy loop hammering it (#291). The floor is
  ## itself capped by `maxRetryMs`, so lowering the ceiling still lowers the floor.

proc sseRetryDelay*(retryMs, minRetryMs, maxRetryMs: int): int =
  ## `retryMs` clamped into the floor/ceiling the stream was opened with. Used for
  ## the configured base, for a `retry:` the server sent, and for the delay actually
  ## slept, so no path can produce a sub-floor (or over-ceiling) reconnect.
  let hi = max(maxRetryMs, 0)
  let lo = max(min(minRetryMs, hi), 0)
  result = max(min(retryMs, hi), lo)

proc sseBackoff*(retryMs, baseRetryMs, minRetryMs, maxRetryMs: int): int =
  ## The next delay after a connect that failed, or that closed without delivering
  ## a single event: double the current delay (never below the base or the floor),
  ## saturating at the ceiling. The halfway test also keeps the doubling from
  ## overflowing on an extreme ceiling.
  let hi = max(maxRetryMs, 0)
  let lo = max(min(minRetryMs, hi), 0)
  let cur = max(max(retryMs, baseRetryMs), lo)
  result = if cur >= hi div 2: hi else: max(min(cur * 2, hi), lo)

proc reset*(p: var SseParser) =
  ## Drop per-connection parse state before a reconnect, keeping the resume id and
  ## retry. A partially-received event at disconnect is discarded (not dispatched),
  ## per the spec.
  p.buf.setLen(0)
  p.scanned = 0
  p.evType.setLen(0)
  p.data.setLen(0)
  p.hasData = false
  p.dataBytes = 0
  p.idBuf = p.lastId        # roll back an id from the discarded partial event
  p.ready.clear()
  p.atStart = true
