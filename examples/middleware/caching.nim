## Response-caching middleware: a fresh GET is served from the in-memory cache
## without a second network request. The local server tags its response with
## `Cache-Control: max-age=60`, so `mw.cache()` stores it and short-circuits the
## repeat -- proven by the server's request counter staying at 1.
##
##   nim c -r examples/middleware/caching.nim

import navi
import navi/mw
import ./server

var state: ServerState
startServer(port = 9701, state = addr state, maxAge = 60)

var cfg = initNaviConfig()
cfg.middleware = @[mw.cache()]
let api = newNavi(cfg)

let url = "http://127.0.0.1:9701/data"
let first = api.get(url)    # miss: fetched from the server and stored
let second = api.get(url)   # fresh hit: served from cache, no request sent

echo "first:  ", first.status, "  body=", first.body
echo "second: ", second.status, "  body=", second.body, "  (from cache)"
echo "requests the server actually received: ", state.count.load

doAssert second.body == first.body
doAssert state.count.load == 1, "the second GET should have been a cache hit"
echo "ok"
