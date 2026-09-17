## Verb sugar shared by every entry module via `include`.
##
## Not a standalone module: it is `include`d after the including entry module
## defines `Navi` and `request`. The `auto` return type adapts to the backend
## (Response for the sync entry, Future[Response] for the async ones), so this
## one definition serves all three.
##
## `params` is generic so a verb accepts any query form -- a seq/array of pairs
## (`@[...]`, `@{...}`, `{...}`) or a `Table` / `OrderedTable` -- normalized by
## `toQuery`. The typed default keeps the no-params call unambiguous.
##
## `sink` is a defaulted generic param (`S = default(GatedBodySink)`; a typed nil
## CONVERSION would emit `null.bind(null)` on the js backend and crash): omitting it
## dispatches to the buffered `request`; passing a `GatedBodySink` (bool),
## `BodySink` (void), or a plain `{.async.}` proc (which converts implicitly)
## dispatches to the gated `request` overload, which streams the FINAL response
## body to the sink instead of buffering it. On js the element type is `seq[byte]`.

proc get*[P, S](client: Navi, target: string, headers = initHeaders(),
             params: P = seq[(string, string)].default, cancel: CancelToken = nil,
             sink: S = default(GatedBodySink)): auto =
  client.request(GET, target, headers, "", sink,
                 params = toQuery(params), cancel = cancel)

proc head*[P, S](client: Navi, target: string, headers = initHeaders(),
              params: P = seq[(string, string)].default, cancel: CancelToken = nil,
              sink: S = default(GatedBodySink)): auto =
  client.request(HEAD, target, headers, "", sink,
                 params = toQuery(params), cancel = cancel)

proc delete*[P, S](client: Navi, target: string, headers = initHeaders(),
                params: P = seq[(string, string)].default, cancel: CancelToken = nil,
                sink: S = default(GatedBodySink)): auto =
  client.request(DELETE, target, headers, "", sink,
                 params = toQuery(params), cancel = cancel)

proc options*[P, S](client: Navi, target: string, headers = initHeaders(),
                 params: P = seq[(string, string)].default, cancel: CancelToken = nil,
                 sink: S = default(GatedBodySink)): auto =
  client.request(OPTIONS, target, headers, "", sink,
                 params = toQuery(params), cancel = cancel)

proc post*[P, B, S](client: Navi, target: string, body: B = "",
                 form: seq[(string, string)] = @[],
                 headers = initHeaders(), params: P = seq[(string, string)].default,
                 cancel: CancelToken = nil, sink: S = default(GatedBodySink)): auto =
  client.request(POST, target, headers, body, sink, form,
                 params = toQuery(params), cancel = cancel)

proc put*[P, B, S](client: Navi, target: string, body: B = "",
                form: seq[(string, string)] = @[],
                headers = initHeaders(), params: P = seq[(string, string)].default,
                cancel: CancelToken = nil, sink: S = default(GatedBodySink)): auto =
  client.request(PUT, target, headers, body, sink, form,
                 params = toQuery(params), cancel = cancel)

proc patch*[P, B, S](client: Navi, target: string, body: B = "",
                  form: seq[(string, string)] = @[],
                  headers = initHeaders(), params: P = seq[(string, string)].default,
                  cancel: CancelToken = nil, sink: S = default(GatedBodySink)): auto =
  client.request(PATCH, target, headers, body, sink, form,
                 params = toQuery(params), cancel = cancel)
