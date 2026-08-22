## Batteries-included middleware for the chronos client (`import
## navi/chronos/mw`). Shares its implementation with the asyncdispatch and js
## clients via `private/mw_async`. Under `nim js` this entry falls back to the js
## client, matching `navi/chronos` itself.

when defined(js):
  import navi/js
  import navi/backend/js          # sleep (not re-exported by navi/js)
else:
  import navi/chronos
  import pkg/chronos               # Future / newFuture for the concurrency limiter
include navi/private/mw_async
