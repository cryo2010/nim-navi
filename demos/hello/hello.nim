## navi/js demo: GET the /hello route of a FastAPI server.
##
## With the bundled server, via Docker (from the repo root):
##
##   docker compose -f demos/hello/docker-compose.yml up --build
##
## Standalone (needs Node 18+ for global fetch and a server on :8080):
##
##   nim js -r demos/hello/hello.nim
##
## The target comes from $HELLO_URL (default http://localhost:8080/hello); that
## is how docker-compose points the client at the `server` service.

import navi/js

proc getEnv(key, fallback: cstring): cstring {.importjs: "(process.env[#] ?? #)".}

# A middleware that echoes the outgoing URL, then proceeds. Middleware are
# nimcall procs that take a Context (no capture), so this is a top-level proc.
proc echoUrl(ctx: Context): Future[void] {.async, nimcall.} =
  echo "-> ", ctx.request.url
  await ctx.next()

proc main() {.async.} =
  let url = $getEnv("HELLO_URL", "http://localhost:8080/hello")
  let api = newNavi(NaviOptions(middleware: @[Middleware(echoUrl)]))

  try:
    let res = await api.get(url)
    echo "status: ", res.status
    echo "body:   ", res.body
  except HttpError as e:
    echo "non-2xx: ", e.response.status, " ", e.response.body
  except CatchableError as e:
    echo "request failed (is the server up?): ", e.msg

discard main()
