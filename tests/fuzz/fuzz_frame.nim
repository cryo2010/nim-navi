## Fuzz target: the HTTP/2 frame decoder (untrusted frames from a peer).
import navi/proto/h2/frame
include ./fuzzlib

fuzzMain:
  var d: FrameDecoder
  var frame: Frame
  try:
    d.feed(input)
    while d.next(frame): discard
  except CatchableError:
    discard  # rejecting malformed input cleanly is correct
