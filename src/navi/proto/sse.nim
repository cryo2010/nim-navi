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

type
  SseEvent* = object
    event*: string     ## event type; "message" if the stream did not set one
    data*: string      ## payload (the `data:` lines joined with "\n")
    id*: string        ## the persistent last event id in effect at dispatch
    retry*: int        ## reconnect delay in ms in effect, or -1 if never set

  SseParser* = object
    buf: string              ## text past the last processed line terminator
    ready: Deque[SseEvent]   ## dispatched events awaiting `next`
    evType: string           ## current event's type buffer
    dataLines: seq[string]   ## current event's data buffer
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
  if p.dataLines.len == 0:
    p.evType.setLen(0)
    return
  p.ready.addLast SseEvent(
    event: (if p.evType.len == 0: "message" else: p.evType),
    data: p.dataLines.join("\n"), id: p.lastId, retry: p.retry)
  p.evType.setLen(0)
  p.dataLines.setLen(0)

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
  of "data":  p.dataLines.add val
  of "id":
    if '\0' notin val: p.lastId = val        # an id containing NUL is ignored
  of "retry":
    if val.len > 0 and val.allCharsInSet({'0' .. '9'}): p.retry = parseInt(val)
  else: discard                              # unknown field: ignored

proc feed*(p: var SseParser, text: string) =
  ## Feed a chunk of decoded text. Complete events become available via `next`.
  var s = text
  if p.atStart and s.len >= 3 and s[0] == '\xEF' and s[1] == '\xBB' and s[2] == '\xBF':
    s = s[3 .. ^1]                            # strip a single leading UTF-8 BOM
  p.atStart = false
  p.buf.add s
  var i = 0
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
    p.buf = p.buf[lineStart .. ^1]            # keep only the unterminated tail

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
  p.evType.setLen(0)
  p.dataLines.setLen(0)
  p.ready.clear()
  p.atStart = true
