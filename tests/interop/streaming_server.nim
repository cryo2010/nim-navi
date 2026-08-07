## Cleartext HTTP/1.1 server for the http/1.1 file-streaming CI checks (the http/2
## checks use nghttpd). GET /download streams the payload named by NAVI_STREAM_FILE;
## POST /echo returns the request body verbatim so the client can hash-match its
## upload. asynchttpserver decodes chunked request bodies into `req.body`.
import std/[asynchttpserver, asyncdispatch, os, strutils]

proc main() {.async.} =
  # Captured as closure locals so the handler stays gcsafe (no global access).
  let payload = readFile(getEnv("NAVI_STREAM_FILE"))
  let port = parseInt(getEnv("NAVI_STREAM_PORT"))

  proc handle(req: Request) {.async, gcsafe.} =
    case req.reqMethod
    of HttpGet:
      if req.url.path == "/download":
        await req.respond(Http200, payload,
          newHttpHeaders({"content-type": "application/octet-stream"}))
      else:
        await req.respond(Http404, "")
    of HttpPost:
      await req.respond(Http200, req.body)        # echo the upload verbatim
    else:
      await req.respond(Http405, "")

  let server = newAsyncHttpServer()
  await server.serve(Port(port), handle)

waitFor main()
