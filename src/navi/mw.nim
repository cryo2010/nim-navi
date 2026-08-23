## Batteries-included middleware for the synchronous client.
##
##   import navi
##   import navi/mw
##   let api = newNavi(initNaviConfig())   # then add mw.cache / mw.rateLimit / ...
##
## The middleware import mirrors the client import: `navi/asyncdispatch/mw`,
## `navi/chronos/mw`, and `navi/js/mw` provide the same factories for the async
## clients (the async trio shares one implementation).

import navi
include navi/private/mw_sync
