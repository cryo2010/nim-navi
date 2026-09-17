## navi/js response-sink runtime checks, run under Node against a small HTTP server
## (see js_sink.sh). nim check -b:js is insufficient (a prior js bug shipped that
## way), so this exercises the sink end to end under Node:
##   1. chunked delivery: a bool sink receives the whole body, res.body is empty.
##   2. bool-stop truncation: returning false stops early; res.bodyTruncated is set
##      and the request still resolves.
##   3. void form: a void sink always continues.
##   4. delivery rule: a 503-then-200 (retry) delivers only the final 200 body.
##   5. throw + non-2xx: a 404 never calls the sink and raises HttpError.

import navi/js

const base = "http://127.0.0.1:9522"

proc bytesToStr(b: seq[byte]): string =
  result = newString(b.len)
  for i in 0 ..< b.len: result[i] = char(b[i])

proc chunkedDelivery(): Future[void] {.async.} =
  let api = newNavi()
  var got = ""
  let sink = proc(data: seq[byte]): Future[bool] {.async.} =
    got.add bytesToStr(data); return true
  let res = await api.get(base & "/body", sink = sink)
  doAssert res.status == 200, "status " & $res.status
  doAssert res.body == "", "body should be empty, got: " & res.body
  doAssert got == "Hello, chunked world!", "delivered: " & got
  doAssert not res.bodyTruncated
  echo "OK: chunked delivery"

proc earlyStop(): Future[void] {.async.} =
  let api = newNavi()
  var chunks = 0
  let sink = proc(data: seq[byte]): Future[bool] {.async.} =
    inc chunks; return false                 # stop after the first chunk
  let res = await api.get(base & "/body", sink = sink)
  doAssert res.status == 200
  doAssert res.bodyTruncated, "expected bodyTruncated"
  doAssert res.body == ""
  doAssert chunks == 1, "chunks: " & $chunks
  echo "OK: early stop truncation, request resolved"

proc voidForm(): Future[void] {.async.} =
  let api = newNavi()
  var got = ""
  let sink = proc(data: seq[byte]): Future[void] {.async.} =
    got.add bytesToStr(data)
  let res = await api.get(base & "/body", sink = sink)
  doAssert res.status == 200
  doAssert got == "Hello, chunked world!", "delivered: " & got
  echo "OK: void form delivers the whole body"

proc retryDeliversFinalOnly(): Future[void] {.async.} =
  let api = newNavi()
  var got = ""
  let sink = proc(data: seq[byte]): Future[bool] {.async.} =
    got.add bytesToStr(data); return true
  let res = await api.get(base & "/retry", sink = sink)
  doAssert res.status == 200, "status " & $res.status
  doAssert got == "recovered", "delivered: " & got   # never the 503 body
  echo "OK: retry delivers the final body only"

proc throwNeverCallsSink(): Future[void] {.async.} =
  let api = newNavi()
  var called = false
  let sink = proc(data: seq[byte]): Future[bool] {.async.} =
    called = true; return true
  var raised = false
  try:
    discard await api.get(base & "/notfound", sink = sink)
  except HttpError as e:
    raised = true
    doAssert e.response.body.len > 0, "HttpError should carry the body"
  doAssert raised, "expected HttpError"
  doAssert not called, "sink must not be called for a thrown non-2xx"
  echo "OK: throw + non-2xx never calls the sink"

proc main() {.async.} =
  await chunkedDelivery()
  await earlyStop()
  await voidForm()
  await retryDeliversFinalOnly()
  await throwNeverCallsSink()
  echo "ALL OK: navi/js response sink"

discard main()
