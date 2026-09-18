## Streaming-download verb sugar shared by every entry module via `include`.
##
## Not a standalone module: it is `include`d after the including entry module
## defines `Navi` and the full-control `stream(client, verb, target, ...)`. The
## `auto` return type adapts to the backend (StreamResponse for the sync entry,
## Future[StreamResponse] for the async/js ones), so this one definition serves
## all three -- exactly as private/verbs.nim does for the buffered verbs.
##
## `StreamClient` is a zero-cost view over `Navi` returned by the no-arg
## `stream(client)` overload, giving streaming downloads the same two-layer shape
## as the rest of navi: `api.stream.get(url)` is sugar over the full-control
## `api.stream(GET, url)`, just as `api.get(url)` is sugar over `api.request(GET, url)`.

type
  StreamClient* = distinct Navi
    ## A zero-cost view over `Navi` whose verb procs open streaming downloads.
    ## Obtained from `api.stream` (the no-arg overload) and consumed immediately
    ## as `api.stream.get(url)`; see the verb procs below.

proc stream*(client: Navi): StreamClient {.inline.} =
  ## The streaming-download namespace view: `api.stream.get(url)` (and the six
  ## other verbs) open a streaming download, sugar over `api.stream(GET, url)`.
  ## Distinct in arity from the full-control `stream(client, verb, target, ...)`,
  ## so overload resolution picks this only for the bare `api.stream` form.
  StreamClient(client)

proc get*(sc: StreamClient, target: string, headers = initHeaders(),
          params: seq[(string, string)] = @[], cancel: CancelToken = nil): auto =
  ## Open a streaming GET download. Sugar over `stream(client, GET, target, ...)`;
  ## see that proc for the full semantics (no throw on non-2xx, redirects/digest).
  stream(Navi(sc), GET, target, headers, params, cancel)

proc head*(sc: StreamClient, target: string, headers = initHeaders(),
           params: seq[(string, string)] = @[], cancel: CancelToken = nil): auto =
  ## Open a streaming HEAD download. Sugar over `stream(client, HEAD, target, ...)`.
  stream(Navi(sc), HEAD, target, headers, params, cancel)

proc delete*(sc: StreamClient, target: string, headers = initHeaders(),
             params: seq[(string, string)] = @[], cancel: CancelToken = nil): auto =
  ## Open a streaming DELETE download. Sugar over `stream(client, DELETE, target, ...)`.
  stream(Navi(sc), DELETE, target, headers, params, cancel)

proc options*(sc: StreamClient, target: string, headers = initHeaders(),
              params: seq[(string, string)] = @[], cancel: CancelToken = nil): auto =
  ## Open a streaming OPTIONS download. Sugar over `stream(client, OPTIONS, target, ...)`.
  stream(Navi(sc), OPTIONS, target, headers, params, cancel)

proc post*(sc: StreamClient, target: string, headers = initHeaders(),
           params: seq[(string, string)] = @[], cancel: CancelToken = nil): auto =
  ## Open a streaming POST download. Sugar over `stream(client, POST, target, ...)`.
  stream(Navi(sc), POST, target, headers, params, cancel)

proc put*(sc: StreamClient, target: string, headers = initHeaders(),
          params: seq[(string, string)] = @[], cancel: CancelToken = nil): auto =
  ## Open a streaming PUT download. Sugar over `stream(client, PUT, target, ...)`.
  stream(Navi(sc), PUT, target, headers, params, cancel)

proc patch*(sc: StreamClient, target: string, headers = initHeaders(),
            params: seq[(string, string)] = @[], cancel: CancelToken = nil): auto =
  ## Open a streaming PATCH download. Sugar over `stream(client, PATCH, target, ...)`.
  stream(Navi(sc), PATCH, target, headers, params, cancel)
