## Verb sugar shared by every entry module via `include`.
##
## Not a standalone module: it is `include`d after the including entry module
## defines `Navi` and `request`. The `auto` return type adapts to the backend
## (Response for the sync entry, Future[Response] for the async ones), so this
## one definition serves all three.

proc get*(client: Navi, target: string, headers = initHeaders()): auto =
  client.request(GET, target, headers)

proc head*(client: Navi, target: string, headers = initHeaders()): auto =
  client.request(HEAD, target, headers)

proc delete*(client: Navi, target: string, headers = initHeaders()): auto =
  client.request(DELETE, target, headers)

proc options*(client: Navi, target: string, headers = initHeaders()): auto =
  client.request(OPTIONS, target, headers)

proc post*(client: Navi, target: string, body = "", json: JsonNode = nil,
           form: seq[(string, string)] = @[], headers = initHeaders()): auto =
  client.request(POST, target, headers, body, json, form)

proc put*(client: Navi, target: string, body = "", json: JsonNode = nil,
          form: seq[(string, string)] = @[], headers = initHeaders()): auto =
  client.request(PUT, target, headers, body, json, form)

proc patch*(client: Navi, target: string, body = "", json: JsonNode = nil,
            form: seq[(string, string)] = @[], headers = initHeaders()): auto =
  client.request(PATCH, target, headers, body, json, form)
