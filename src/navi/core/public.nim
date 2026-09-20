## Umbrella of the public, backend-agnostic surface. Entry modules re-export
## this so users get the full type/API set from a single import.

import ./headers, ./url, ./request, ./response, ./cancel
# Import the cookie module under an alias so a bare `cookies` module symbol is not
# in scope: it would otherwise collide with the `Navi.cookies` inspection accessor
# and mis-resolve `client.cookies` under the nim js async macro (the C backends
# tolerate the clash, js does not). Its exported symbols re-export unchanged. #374
import ./cookies as cookiejar
import ../backend/api
export headers, url, request, response, api, cancel, cookiejar
