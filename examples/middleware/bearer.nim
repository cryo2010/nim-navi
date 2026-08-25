## Bearer-auth middleware: sets `Authorization: Bearer <token>` on every request.
## The local server echoes back whatever Authorization header it received, so we
## can confirm the middleware attached it.
##
##   nim c -r examples/middleware/bearer.nim

import navi
import navi/mw
import ./server

var state: ServerState
startServer(port = 9704, state = addr state)

var cfg = initNaviConfig()
cfg.middleware = @[mw.bearer("s3cr3t-token")]
let api = newNavi(cfg)

let res = api.get("http://127.0.0.1:9704/protected")
echo "server saw -> ", res.body

doAssert res.body == "Bearer s3cr3t-token"
echo "ok"
