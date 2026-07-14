## navi/js demo: GET http://localhost:8080/hello
##
## Runs on the JavaScript backend over the runtime's fetch (Node 18+ ships a
## global fetch). Start a server on :8080 first, for example:
##
##   node -e 'require("http").createServer((_,s)=>s.end("hello\n")).listen(8080)'
##
## then build and run:
##
##   nim js -r demos/hello.nim

import navi/js

proc main() {.async.} =
  let api = newNavi()
  try:
    let res = await api.get("http://localhost:8080/hello")
    echo "status: ", res.status
    echo "body:   ", res.body
  except HttpError as e:
    echo "non-2xx: ", e.response.status, " ", e.response.body
  except CatchableError as e:
    echo "request failed (is a server listening on :8080?): ", e.msg

discard main()
