## navi/js keep-alive race replay + fetch error cause, under Node. Driven by
## tests/interop/js_replay.sh, which runs the server this talks to.
import std/strutils
import navi/js

const base = "http://127.0.0.1:9534"
var fails = 0

proc check(name: string, ok: bool, detail = "") =
  echo (if ok: "OK: " else: "FAIL: "), name, (if detail.len > 0: " [" & detail & "]" else: "")
  if not ok: inc fails

proc hdrs(id: string, key = ""): Headers =
  result = initHeaders()
  result["x-id"] = id
  if key.len > 0: result["idempotency-key"] = key

proc hits(api: Navi, id: string): Future[int] {.async.} =
  let r = await api.request(GET, base & "/hits", headers = hdrs(id))
  return parseInt(r.body)

proc main() {.async.} =
  let api = newNavi(initNaviConfig())

  let r1 = await api.request(POST, base & "/reset", headers = hdrs("keyed", "k1"), body = "hi")
  check("keyed POST is replayed after a pre-response socket drop",
        r1.status == 200 and r1.body == "ok 2 hi", r1.body)

  try:
    discard await api.request(POST, base & "/reset", headers = hdrs("unkeyed"), body = "hi")
    check("unkeyed POST is not replayed", false, "request succeeded")
  except KeepAliveRaceError as e:
    check("unkeyed POST raises KeepAliveRaceError carrying the cause",
          "UND_ERR_SOCKET" in e.msg or "ECONNRESET" in e.msg, e.msg)
  except CatchableError as e:
    check("unkeyed POST raises KeepAliveRaceError", false, $e.name & ": " & e.msg)
  check("unkeyed POST was sent once", (await api.hits("unkeyed")) == 1)

  let r3 = await api.request(GET, base & "/reset", headers = hdrs("get"))
  check("GET is replayed", r3.status == 200 and r3.body == "ok 2 ", r3.body)

  try:
    discard await api.request(POST, base & "/always", headers = hdrs("always", "k4"), body = "x")
    check("always-dropping keyed POST raises", false, "request succeeded")
  except CatchableError:
    # Default retry.limit 2: three attempts, each with one immediate replay.
    let n = await api.hits("always")
    check("always-dropping keyed POST is bounded", n == 6, $n & " sends")

  try:
    discard await api.request(GET, base & "/midbody", headers = hdrs("mid"))
    check("mid-body drop raises", false, "request succeeded")
  except CatchableError as e:
    check("mid-body drop is a catchable IOError carrying the cause",
          e of IOError and "body read failed" in e.msg and "(" in e.msg, e.msg)

  try:
    discard await api.request(POST, "http://127.0.0.1:9535/x", body = "x")
    check("refused connect raises", false, "request succeeded")
  except KeepAliveRaceError as e:
    check("refused connect is not a keep-alive race", false, e.msg)
  except CatchableError as e:
    check("refused connect is a plain IOError carrying the cause",
          "ECONNREFUSED" in e.msg, e.msg)

  if fails > 0: quit(1)
  echo "ALL OK: navi/js keep-alive race replay"

discard main()
