## Manual smoke test: fetch several URLs concurrently, multiplexed over one
## HTTP/2 connection. Not in the deterministic suite (needs the network).
##
##   nim c -r examples/parallel.nim

import navi

let api = newNavi()
let res = api.parallel(@[
  "https://nghttp2.org/",
  "https://nghttp2.org/httpbin/get",
  "https://nghttp2.org/httpbin/user-agent",
])
for i, r in res:
  echo r.httpVersion, " ", r.status, "  (", r.body.len, " bytes)"
doAssert res.len == 3
for r in res:
  doAssert r.ok
echo "ok"
