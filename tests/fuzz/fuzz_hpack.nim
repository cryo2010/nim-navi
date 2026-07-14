## Fuzz target: the HPACK decoder (untrusted header blocks from a peer).
import navi/proto/h2/hpack
include ./fuzzlib

fuzzMain:
  var dec = initHpackDecoder()
  try:
    discard dec.decode(input)
  except CatchableError:
    discard  # malformed input rejected cleanly is the correct behavior
