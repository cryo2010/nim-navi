## Fuzz target: the HTTP/1.1 response parser (untrusted bytes from a server).
import navi/proto/h1
include ./fuzzlib

fuzzMain:
  var p = initH1Parser()
  try:
    p.feed(input)
    p.eof()
    discard p.finished
  except CatchableError:
    discard  # rejecting malformed input cleanly is correct
