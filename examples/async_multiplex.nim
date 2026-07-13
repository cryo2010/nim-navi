## Manual smoke test: concurrent async requests multiplexed over one HTTP/2
## connection (ky-style Promise.all). Needs the network.
##
##   nim c -r examples/async_multiplex.nim

import std/asyncdispatch
import navi/asyncdispatch
import navi/backend/asyncdispatch as be

proc main() {.async.} =
  let api = newNavi()
  let responses = await all(@[
    api.get("https://nghttp2.org/"),
    api.get("https://nghttp2.org/httpbin/get"),
    api.get("https://nghttp2.org/httpbin/user-agent"),
    api.get("https://nghttp2.org/httpbin/ip"),
  ])
  for r in responses:
    echo r.httpVersion, " ", r.status, "  (", r.body.len, " bytes)"
  echo "TCP connections opened: ", be.openedConnections
  doAssert be.openedConnections == 1, "all should multiplex over one connection"
  echo "ok"

waitFor main()
