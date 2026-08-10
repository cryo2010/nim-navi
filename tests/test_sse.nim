## Sans-io Server-Sent Events parser: fed text, events out. No sockets.
import unittest
import std/options
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

  test "a data field with no value should contribute an empty line":
    var p = initSseParser()
    p.feed("data\ndata: y\n\n")       # "data" alone -> empty string in the buffer
    check p.drain()[0].data == "\ny"

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
