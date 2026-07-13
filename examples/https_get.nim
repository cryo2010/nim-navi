## Manual smoke test: a real HTTPS GET. Not part of the deterministic suite
## because it depends on the network.
##
##   nim c -r examples/https_get.nim

import navi

let api = newNavi()
let res = api.get("https://example.com/")
echo "status: ", res.status
echo "server: ", res.headers.get("server")
echo "bytes:  ", res.body.len
doAssert res.ok, "expected a 2xx response"
echo "ok"
