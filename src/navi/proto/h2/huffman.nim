## HPACK Huffman coding (RFC 7541 Appendix B).
##
## Placeholder: the canonical Huffman table and decoder land in the next step.
## Until then, decoding a Huffman-coded string raises rather than returning
## garbage.

proc huffmanDecode*(data: string): string =
  raise newException(ValueError, "hpack: Huffman decoding not yet implemented")
