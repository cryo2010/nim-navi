## Batteries-included middleware for the JavaScript (`nim js`) client (`import
## navi/js/mw`). Shares its implementation with the asyncdispatch and chronos
## clients via `private/mw_async`. The `concurrencyLimit` limiter is omitted here (the
## fetch runtime manages request concurrency).

import navi/js
import navi/backend/js             # sleep (not re-exported by navi/js)
include navi/private/mw_async
