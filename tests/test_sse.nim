## Sans-io Server-Sent Events parser: fed text, events out. No sockets.
import unittest
import std/[options, strutils]
import navi/proto/sse

proc drain(p: var SseParser): seq[SseEvent] =
  while true:
    let e = p.next()
    if e.isNone: break
    result.add e.get

suite "sse parser":
  test "a data line should dispatch a default message event on the blank line":
    var p = initSseParser()
    p.feed("data: hello\n\n")
    let ev = p.drain()
    check ev.len == 1
    check ev[0].event == "message"
    check ev[0].data == "hello"
    check ev[0].id == ""

  test "an event field should set the event type":
    var p = initSseParser()
    p.feed("event: ping\ndata: x\n\n")
    let ev = p.drain()
    check ev.len == 1 and ev[0].event == "ping" and ev[0].data == "x"

  test "multiple data lines should join with a newline":
    var p = initSseParser()
    p.feed("data: a\ndata: b\ndata: c\n\n")
    check p.drain()[0].data == "a\nb\nc"

  test "only the first colon should split field from value":
    var p = initSseParser()
    p.feed("data: a: b\n\n")
    check p.drain()[0].data == "a: b"

  test "exactly one leading space should be stripped from the value":
    var p = initSseParser()
    p.feed("data:  two-spaces\n\n")   # first space is the delimiter; one kept
    check p.drain()[0].data == " two-spaces"

  test "a comment line should be ignored":
    var p = initSseParser()
    p.feed(": keep-alive\ndata: x\n\n")
    let ev = p.drain()
    check ev.len == 1 and ev[0].data == "x"

  test "a blank event with no data should not dispatch":
    var p = initSseParser()
    p.feed(": just a comment\n\n")
    check p.drain().len == 0

  test "the id should persist across events until changed":
    var p = initSseParser()
    p.feed("data: 1\nid: 42\n\n")
    p.feed("data: 2\n\n")             # no id here; should still report 42
    p.feed("data: 3\nid: 99\n\n")
    let ev = p.drain()
    check ev.len == 3
    check ev[0].id == "42" and ev[1].id == "42" and ev[2].id == "99"
    check p.lastEventId() == "99"

  test "an id containing a NUL should be ignored":
    var p = initSseParser()
    p.feed("data: x\nid: a\x00b\n\n")
    check p.drain()[0].id == ""

  test "a retry field should set the reconnect time and ride on the event":
    var p = initSseParser()
    p.feed("retry: 5000\ndata: x\n\n")
    check p.retryMs() == 5000
    check p.drain()[0].retry == 5000

  test "a non-integer retry should be ignored":
    var p = initSseParser()
    p.feed("retry: soon\ndata: x\n\n")
    check p.retryMs() == -1

  test "CRLF line endings should parse the same as LF":
    var p = initSseParser()
    p.feed("event: e\r\ndata: x\r\n\r\n")
    let ev = p.drain()
    check ev.len == 1 and ev[0].event == "e" and ev[0].data == "x"

  test "a bare CR should terminate a line":
    # A trailing lone CR is held (it may begin a CRLF split across feeds); the next
    # feed disambiguates it, so events separated by bare CR still parse.
    var p = initSseParser()
    p.feed("data: x\r\r")
    p.feed("data: y\n\n")
    let ev = p.drain()
    check ev.len == 2 and ev[0].data == "x" and ev[1].data == "y"

  test "an event split across two feeds should reassemble":
    var p = initSseParser()
    p.feed("data: hel")
    check p.drain().len == 0          # nothing complete yet
    p.feed("lo\n\n")
    check p.drain()[0].data == "hello"

  test "a CRLF split across two feeds should not double-terminate":
    var p = initSseParser()
    p.feed("data: x\r")               # trailing CR is ambiguous, held back
    check p.drain().len == 0
    p.feed("\n\n")                    # the LF completes the CRLF, then a blank line
    let ev = p.drain()
    check ev.len == 1 and ev[0].data == "x"

  test "a leading UTF-8 BOM should be stripped once":
    var p = initSseParser()
    p.feed("\xEF\xBB\xBFdata: x\n\n")
    check p.drain()[0].data == "x"

  test "a UTF-8 BOM split across small first feeds is still stripped (#244)":
    # The BOM (3 bytes) can arrive one or two bytes at a time. The parser must not
    # clear its start state until enough bytes are present to decide, or the BOM
    # bytes leak into the first field name and it fails to match.
    var p = initSseParser()
    p.feed("\xEF")            # 1 byte: cannot decide yet
    p.feed("\xBB")            # 2 bytes: still cannot decide
    p.feed("\xBFdata: x\n\n") # BOM now complete, then the event
    check p.drain()[0].data == "x"

  test "a two-then-rest BOM split is stripped":
    var p = initSseParser()
    p.feed("\xEF\xBB")
    p.feed("\xBFdata: y\n\n")
    check p.drain()[0].data == "y"

  test "a short first feed that is not a BOM is preserved (#244)":
    # The other half of the fragmented-start rule: holding back on a 1-2 byte first
    # feed must not eat those bytes when they turn out not to be a BOM. A stream that
    # opens with a one-byte read still parses its first field.
    var p = initSseParser()
    p.feed("d")                        # 1 byte: cannot decide yet
    p.feed("a")                        # 2 bytes: still cannot decide
    p.feed("ta: x\n\n")
    check p.drain()[0].data == "x"

  test "a first feed that shares a prefix with the BOM is preserved (#244)":
    # EF BB followed by anything but BF is not a BOM, so both bytes stay in the
    # stream -- here as the start of a (harmlessly ignored) unknown field name,
    # which proves they were not dropped: the real event that follows still fires.
    var p = initSseParser()
    p.feed("\xEF\xBB")
    p.feed("field: v\ndata: z\n\n")
    let ev = p.drain()
    check ev.len == 1
    check ev[0].data == "z"
    check ev[0].event == "message"     # "\xEF\xBBfield" is not "event": still default

  test "a data field with no value should contribute an empty line":
    var p = initSseParser()
    p.feed("data\ndata: y\n\n")       # "data" alone -> empty string in the buffer
    check p.drain()[0].data == "\ny"

  test "an id promoted by dispatch should survive a reconnect (#290)":
    # `id:` only fills a buffer; the persistent resume id moves at dispatch. A
    # partial event still being received when the connection drops must not
    # advance it, or the reconnect resumes past events that were never delivered.
    var p = initSseParser()
    p.feed("data: 1\nid: 7\n\n")
    discard p.drain()
    p.feed("id: 9\ndata: partial")        # no blank line: never dispatched
    p.reset()                             # the drop
    check p.lastEventId() == "7"          # not the un-dispatched 9

  test "an id line with no data should still move the resume id (#290)":
    # Promotion happens before the empty-data early return, so a server can
    # checkpoint with a bare `id:` event that dispatches nothing.
    var p = initSseParser()
    p.feed("id: 5\n\n")
    check p.drain().len == 0              # nothing dispatched: no data
    check p.lastEventId() == "5"

  test "reset should drop a partial event but keep the resume id":
    var p = initSseParser()
    p.feed("data: 1\nid: 7\n\n")
    discard p.drain()
    p.feed("data: partial-no-blank-line")   # never dispatched
    p.reset()
    check p.lastEventId() == "7"
    check p.drain().len == 0
    p.feed("data: after\n\n")
    check p.drain()[0].id == "7"      # id persisted across the reconnect

  test "mixed CR / LF / CRLF endings and leading spaces parse in place (#410)":
    # The line's value is located in the parse buffer and copied once, so the
    # terminator handling and the single-leading-space rule are pinned together:
    # no space, one (the delimiter, stripped), two (one kept), an empty value, and
    # a line with no colon at all.
    var p = initSseParser()
    p.feed("data:none\r" &            # bare CR terminator, no space after the colon
           "data: one\n" &            # LF, one space: that is the delimiter
           "data:  two\r\n" &         # CRLF, two spaces: only the first is stripped
           "data:\n" &                # a colon and nothing else: empty value
           "data\r\n" &               # no colon at all: empty value
           "\r\n")                    # blank line dispatches
    let ev = p.drain()
    check ev.len == 1
    check ev[0].event == "message"
    check ev[0].data == "none\none\n two\n\n"

  test "a multi-megabyte event round-trips byte-identically (#410)":
    # 4 MiB of data lines, whole and then fed in 64 KiB chunks so the buffer
    # compaction between feeds is exercised on the same payload.
    const lineLen = 4096
    const lines = 1024                              # 4 MiB of payload
    var expected = newStringOfCap(lines * (lineLen + 1))
    var wire = newStringOfCap(lines * (lineLen + 8))
    for i in 0 ..< lines:
      let v = repeat(chr(ord('a') + (i mod 26)), lineLen)
      if i > 0: expected.add '\n'
      expected.add v
      wire.add "data: "
      wire.add v
      wire.add '\n'
    wire.add '\n'

    var whole = initSseParser()
    whole.feed(wire)
    let one = whole.drain()
    check one.len == 1
    check one[0].data.len == expected.len
    check one[0].data == expected

    var chunked = initSseParser()
    var off = 0
    while off < wire.len:
      let n = min(65536, wire.len - off)
      chunked.feed(wire[off ..< off + n])
      off += n
    let two = chunked.drain()
    check two.len == 1
    check two[0].data == expected

  test "moving the event fields out on dispatch leaks nothing into the next (#410)":
    # The per-event buffers are moved into the event, so the reset that follows has
    # to leave the parser exactly as a fresh event expects: default type, empty
    # data, and the persistent id still standing.
    var p = initSseParser()
    p.feed("event: a\nid: 1\ndata: first\n\n")
    p.feed("data: second\n\n")          # no event, no id: default type, id persists
    p.feed("event: b\nid: 2\ndata: third\n\n")
    p.feed("id: 3\n\n")                 # id-only checkpoint: dispatches nothing
    p.feed("data: fourth\n\n")
    let ev = p.drain()
    check ev.len == 4
    check (ev[0].event, ev[0].data, ev[0].id) == ("a", "first", "1")
    check (ev[1].event, ev[1].data, ev[1].id) == ("message", "second", "1")
    check (ev[2].event, ev[2].data, ev[2].id) == ("b", "third", "2")
    check (ev[3].event, ev[3].data, ev[3].id) == ("message", "fourth", "3")
    check p.lastEventId() == "3"

  test "a retry value that is not all digits is ignored (#410)":
    var p = initSseParser()
    p.feed("retry: 12x\ndata: x\n\n")
    check p.retryMs() == -1
    p.feed("retry:\ndata: y\n\n")        # empty value
    check p.retryMs() == -1
    p.feed("retry: 250\ndata: z\n\n")
    check p.retryMs() == 250

suite "sse parser DoS hardening":
  test "an out-of-range retry value is ignored, not a crash":
    var p = initSseParser()
    p.feed("retry: 999999999999999999999\ndata: x\n\n")   # digits but overflows int
    check p.retryMs() == -1                                # ignored, no exception
    check p.drain()[0].data == "x"

  test "an oversized single event fails instead of buffering unbounded":
    var p = initSseParser()
    var raised = false
    try:
      # many data: lines with no dispatching blank line
      let line = "data: " & repeat("x", 100_000) & "\n"
      for _ in 0 ..< 200: p.feed(line)      # ~20 MB > maxSseEventBytes
    except ValueError:
      raised = true
    check raised

  test "an endless unterminated line fails instead of buffering unbounded":
    var p = initSseParser()
    var raised = false
    try:
      for _ in 0 ..< 200: p.feed(repeat("x", 100_000))   # no newline ever
    except ValueError:
      raised = true
    check raised

suite "sse reconnect delay policy (#291)":
  # The floor and the empty-connect backoff are shared by every client's reconnect
  # loop, so the arithmetic is pinned here; the socket-level behaviour lives in
  # test_sse_retry*.nim.
  test "a retry: 0 is lifted to the floor instead of reconnecting instantly":
    check sseRetryDelay(0, defaultSseMinRetryMs, 30_000) == 100

  test "the floor is itself capped by the ceiling":
    check sseRetryDelay(0, 500, 200) == 200          # a floor cannot exceed the max
    check sseRetryDelay(50, 0, 30_000) == 50         # floor off: the value stands

  test "a delay between the floor and the ceiling is left alone, above it is capped":
    check sseRetryDelay(3000, 100, 30_000) == 3000
    check sseRetryDelay(60_000, 100, 30_000) == 30_000

  test "a nonsense (negative) delay lands on the floor, never below zero":
    check sseRetryDelay(-5, 100, 30_000) == 100
    check sseRetryDelay(-5, 0, 30_000) == 0

  test "the backoff doubles and saturates at the ceiling":
    check sseBackoff(100, 100, 100, 30_000) == 200
    check sseBackoff(200, 100, 100, 30_000) == 400
    check sseBackoff(20_000, 100, 100, 30_000) == 30_000
    check sseBackoff(30_000, 100, 100, 30_000) == 30_000

  test "the backoff starts from the floor even when the base is zero":
    check sseBackoff(0, 0, 100, 30_000) == 200       # a retry: 0 server still backs off
    check sseBackoff(100, 100, 100, 100) == 100      # floor == ceiling: nowhere to go

  test "the backoff cannot overflow on an extreme ceiling":
    check sseBackoff(int.high div 2 + 10, 0, 0, int.high) == int.high
