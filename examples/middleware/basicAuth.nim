## Basic-auth middleware: sets `Authorization: Basic <base64(user:pass)>` on every
## request. The local server echoes back the Authorization header it received, so
## we can confirm the encoded credentials.
##
##   nim c -r examples/middleware/basicAuth.nim

import std/base64
import navi
import navi/mw
import ./server

var state: ServerState
startServer(port = 9705, state = addr state)

var cfg = initNaviConfig()
cfg.middleware = @[mw.basic("alice", "s3cr3t-pass")]
let api = newNavi(cfg)

let res = api.get("http://127.0.0.1:9705/protected")
echo "server saw -> ", res.body

let expected = "Basic " & encode("alice:s3cr3t-pass")
doAssert res.body == expected
echo "ok"
