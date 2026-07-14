## Fuzz target: the HPACK Huffman decoder (untrusted encoded strings).
import navi/proto/h2/huffman
include ./fuzzlib

fuzzMain:
  try:
    discard huffmanDecode(input)
  except CatchableError:
    discard  # rejecting malformed input cleanly is correct
