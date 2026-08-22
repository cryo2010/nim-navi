## Batteries-included middleware for the asyncdispatch client (`import
## navi/asyncdispatch/mw`). Shares its implementation with the chronos and js
## clients via `private/mw_async`. Under `nim js` this entry falls back to the js
## client, matching `navi/asyncdispatch` itself.

when defined(js):
  import navi/js
  import navi/backend/js          # sleep (not re-exported by navi/js)
else:
  import navi/asyncdispatch
  import std/asyncdispatch         # Future / newFuture for the concurrency limiter
include navi/private/mw_async
