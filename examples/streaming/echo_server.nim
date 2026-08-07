## Minimal local HTTP/1.1 server for the streaming examples (std/asynchttpserver),
## run on a background thread so each example stays a single self-contained script.
## GET returns a fixed payload; POST echoes the request body back verbatim -- so a
## client can hash the response against what it sent and prove the round-trip.
import std/[asynchttpserver, asyncdispatch]

type ServerArgs* = object
  port*: int
  payload*: string          ## returned as the body of any GET

proc runEchoServer(args: ServerArgs) {.thread.} =
  proc handle(req: Request) {.async, gcsafe.} =
    case req.reqMethod
    of HttpGet:  await req.respond(Http200, args.payload)
    of HttpPost: await req.respond(Http200, req.body)     # echo the upload verbatim
    else:        await req.respond(Http405, "")
  let server = newAsyncHttpServer()
  waitFor server.serve(Port(args.port), handle)

proc startEchoServer*(th: var Thread[ServerArgs], port: int, payload = "") =
  ## Launch the echo server on a background thread. Call `sleep` briefly after to
  ## let it bind before the first request.
  createThread(th, runEchoServer, ServerArgs(port: port, payload: payload))

proc sampleContent*(bytes: int): string =
  ## Deterministic, varied bytes (a small LCG) so the examples need no external
  ## file or randomness, yet the hash check is meaningful.
  result = newStringOfCap(bytes)
  var x = 0x12345678'u32
  for _ in 0 ..< bytes:
    x = x * 1664525'u32 + 1013904223'u32
    result.add char(x shr 24)
