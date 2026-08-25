## Shared local HTTP/1.1 server for the middleware examples (std/asynchttpserver),
## run on a background thread so each example stays one self-contained script. It:
##  - counts requests it actually receives, so a cache example can prove a hit
##    skipped the network;
##  - tags GET responses with `Cache-Control: max-age=<maxAge>` (or `no-store`
##    when `maxAge` is 0), so the cache middleware can decide to store them;
##  - echoes any `Authorization` header back verbatim in the body, so the
##    bearer/basic examples can confirm exactly what the client sent;
##  - can delay each response (`delayMs`), so rate-limit / concurrency pacing is
##    observable, and tracks the peak number of concurrent in-flight requests.
##
## Counters live in a `ServerState` the example owns and shares by pointer; they
## are `Atomic` so the example's thread can read them while the server thread
## writes. Not an example itself -- imported by the others.

import std/[asynchttpserver, asyncdispatch, atomics, os]
export atomics   # so examples can read the counters (`state.count.load`)

type
  ServerState* = object
    count*: Atomic[int]      ## total requests received
    inFlight*: Atomic[int]   ## requests currently being served
    peak*: Atomic[int]       ## high-water mark of `inFlight`

  ServerArgs = object
    port: int
    maxAge: int              ## Cache-Control max-age on GET (0 => no-store)
    delayMs: int             ## artificial per-request delay
    state: ptr ServerState

proc runServer(args: ServerArgs) {.thread.} =
  proc handle(req: Request) {.async, gcsafe.} =
    discard args.state.count.fetchAdd(1)
    let now = args.state.inFlight.fetchAdd(1) + 1
    if now > args.state.peak.load: args.state.peak.store(now)   # single writer
    if args.delayMs > 0: await sleepAsync(args.delayMs)

    let auth = $req.headers.getOrDefault("authorization")
    let body = if auth.len > 0: auth else: "payload"   # echoed back
    var headers = newHttpHeaders()
    headers["Cache-Control"] =
      if args.maxAge > 0: "max-age=" & $args.maxAge else: "no-store"
    await req.respond(Http200, body, headers)

    discard args.state.inFlight.fetchSub(1)

  let server = newAsyncHttpServer()
  waitFor server.serve(Port(args.port), handle)

var serverThread: Thread[ServerArgs]   # owned here so callers never name the type

proc startServer*(port: int, state: ptr ServerState, maxAge = 0, delayMs = 0) =
  ## Launch the server on a background thread and pause briefly so it is bound
  ## before the first request. `state` receives the request counters. The thread
  ## runs until the process exits (the examples are short-lived scripts).
  createThread(serverThread, runServer,
    ServerArgs(port: port, maxAge: maxAge, delayMs: delayMs, state: state))
  sleep(200)
