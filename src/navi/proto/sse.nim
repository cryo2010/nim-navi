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
## `Last-Event-ID`. `reset` clears the per-connection parse state but keeps that id
## and the retry, for use across a reconnect.

import std/[deques, strutils, options]

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
    lastId: string           ## persistent last event id (survives dispatch)
    retry: int               ## last `retry:` value seen, or -1
    atStart: bool            ## until the first byte, to strip a leading BOM

proc initSseParser*(lastEventId = ""): SseParser =
  ## A fresh parser. `lastEventId` seeds the resume id (for a stream opened with a
  ## caller-supplied Last-Event-ID).
  SseParser(retry: -1, atStart: true, lastId: lastEventId)

proc dispatch(p: var SseParser) =
  ## End of an event (a blank line). Fire it only if it accumulated data; either
  ## way reset the per-event buffers. The last-id buffer persists across events.
  if not p.hasData:
    p.evType.setLen(0)
    return
  p.ready.addLast SseEvent(
    event: (if p.evType.len == 0: "message" else: p.evType),
    data: p.data, id: p.lastId, retry: p.retry)
  p.evType.setLen(0)
  p.data.setLen(0)                            # reuse the buffer's capacity next event
  p.hasData = false
  p.dataBytes = 0

proc processLine(p: var SseParser, line: string) =
  if line.len == 0:
    p.dispatch()
    return
  if line[0] == ':': return                 # comment line: ignored (keep-alive)
  let c = line.find(':')
  let field = if c < 0: line else: line[0 ..< c]
  var val = if c < 0: "" else: line[c + 1 .. ^1]
  if val.len > 0 and val[0] == ' ': val = val[1 .. ^1]   # strip one leading space
  case field
  of "event": p.evType = val
  of "data":
    if p.hasData: p.data.add '\n'            # separator between successive data lines
    p.data.add val
    p.hasData = true
    p.dataBytes += val.len + 1               # +1 for the "\n" join separator
    if p.dataBytes > maxSseEventBytes:
      raise newException(ValueError,
        "navi: SSE event exceeds the " & $maxSseEventBytes & "-byte limit")
  of "id":
    if '\0' notin val: p.lastId = val        # an id containing NUL is ignored
  of "retry":
    # All-digit but possibly out of int range; ignore an unparseable value rather
    # than crash the stream.
    if val.len > 0 and val.allCharsInSet({'0' .. '9'}):
      try: p.retry = parseInt(val)
      except ValueError: discard
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
      p.buf = p.buf[3 .. ^1]                  # strip a single leading UTF-8 BOM
    p.atStart = false
  var i = p.scanned                           # resume; earlier bytes had no terminator
  var lineStart = 0
  while i < p.buf.len:
    case p.buf[i]
    of '\n':
      p.processLine(p.buf[lineStart ..< i])
      inc i; lineStart = i
    of '\r':
      if i == p.buf.len - 1: break            # maybe a \r\n split across feeds: wait
      p.processLine(p.buf[lineStart ..< i])
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
  p.ready.clear()
  p.atStart = true
