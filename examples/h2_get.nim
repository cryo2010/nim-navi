## Manual smoke test: a real HTTP/2 request (sync backend, ALPN-negotiated).
## Not in the deterministic suite because it depends on the network.
##
##   nim c -r examples/h2_get.nim

import navi

let api = newNavi()
let res = api.get("https://nghttp2.org/")
echo "version: ", res.httpVersion
echo "status:  ", res.status
echo "bytes:   ", res.body.len
doAssert res.httpVersion == "HTTP/2", "expected ALPN to negotiate h2"
doAssert res.ok
echo "h2 ok"
