## HPACK Huffman coding (RFC 7541 Appendix B).
##
## Decoding walks a prefix-tree built from the canonical code table; encoding
## packs codes MSB-first and pads the final byte with 1-bits (the EOS prefix).

type HuffCode = tuple[code: uint32, bits: int]

const codes: array[0 .. 256, HuffCode] = [
  (0x1ff8'u32, 13), (0x7fffd8'u32, 23), (0xfffffe2'u32, 28), (0xfffffe3'u32, 28),
  (0xfffffe4'u32, 28), (0xfffffe5'u32, 28), (0xfffffe6'u32, 28), (0xfffffe7'u32, 28),
  (0xfffffe8'u32, 28), (0xffffea'u32, 24), (0x3ffffffc'u32, 30), (0xfffffe9'u32, 28),
  (0xfffffea'u32, 28), (0x3ffffffd'u32, 30), (0xfffffeb'u32, 28), (0xfffffec'u32, 28),
  (0xfffffed'u32, 28), (0xfffffee'u32, 28), (0xfffffef'u32, 28), (0xffffff0'u32, 28),
  (0xffffff1'u32, 28), (0xffffff2'u32, 28), (0x3ffffffe'u32, 30), (0xffffff3'u32, 28),
  (0xffffff4'u32, 28), (0xffffff5'u32, 28), (0xffffff6'u32, 28), (0xffffff7'u32, 28),
  (0xffffff8'u32, 28), (0xffffff9'u32, 28), (0xffffffa'u32, 28), (0xffffffb'u32, 28),
  (0x14'u32, 6), (0x3f8'u32, 10), (0x3f9'u32, 10), (0xffa'u32, 12),
  (0x1ff9'u32, 13), (0x15'u32, 6), (0xf8'u32, 8), (0x7fa'u32, 11),
  (0x3fa'u32, 10), (0x3fb'u32, 10), (0xf9'u32, 8), (0x7fb'u32, 11),
  (0xfa'u32, 8), (0x16'u32, 6), (0x17'u32, 6), (0x18'u32, 6),
  (0x0'u32, 5), (0x1'u32, 5), (0x2'u32, 5), (0x19'u32, 6),
  (0x1a'u32, 6), (0x1b'u32, 6), (0x1c'u32, 6), (0x1d'u32, 6),
  (0x1e'u32, 6), (0x1f'u32, 6), (0x5c'u32, 7), (0xfb'u32, 8),
  (0x7ffc'u32, 15), (0x20'u32, 6), (0xffb'u32, 12), (0x3fc'u32, 10),
  (0x1ffa'u32, 13), (0x21'u32, 6), (0x5d'u32, 7), (0x5e'u32, 7),
  (0x5f'u32, 7), (0x60'u32, 7), (0x61'u32, 7), (0x62'u32, 7),
  (0x63'u32, 7), (0x64'u32, 7), (0x65'u32, 7), (0x66'u32, 7),
  (0x67'u32, 7), (0x68'u32, 7), (0x69'u32, 7), (0x6a'u32, 7),
  (0x6b'u32, 7), (0x6c'u32, 7), (0x6d'u32, 7), (0x6e'u32, 7),
  (0x6f'u32, 7), (0x70'u32, 7), (0x71'u32, 7), (0x72'u32, 7),
  (0xfc'u32, 8), (0x73'u32, 7), (0xfd'u32, 8), (0x1ffb'u32, 13),
  (0x7fff0'u32, 19), (0x1ffc'u32, 13), (0x3ffc'u32, 14), (0x22'u32, 6),
  (0x7ffd'u32, 15), (0x3'u32, 5), (0x23'u32, 6), (0x4'u32, 5),
  (0x24'u32, 6), (0x5'u32, 5), (0x25'u32, 6), (0x26'u32, 6),
  (0x27'u32, 6), (0x6'u32, 5), (0x74'u32, 7), (0x75'u32, 7),
  (0x28'u32, 6), (0x29'u32, 6), (0x2a'u32, 6), (0x7'u32, 5),
  (0x2b'u32, 6), (0x76'u32, 7), (0x2c'u32, 6), (0x8'u32, 5),
  (0x9'u32, 5), (0x2d'u32, 6), (0x77'u32, 7), (0x78'u32, 7),
  (0x79'u32, 7), (0x7a'u32, 7), (0x7b'u32, 7), (0x7ffe'u32, 15),
  (0x7fc'u32, 11), (0x3ffd'u32, 14), (0x1ffd'u32, 13), (0xffffffc'u32, 28),
  (0xfffe6'u32, 20), (0x3fffd2'u32, 22), (0xfffe7'u32, 20), (0xfffe8'u32, 20),
  (0x3fffd3'u32, 22), (0x3fffd4'u32, 22), (0x3fffd5'u32, 22), (0x7fffd9'u32, 23),
  (0x3fffd6'u32, 22), (0x7fffda'u32, 23), (0x7fffdb'u32, 23), (0x7fffdc'u32, 23),
  (0x7fffdd'u32, 23), (0x7fffde'u32, 23), (0xffffeb'u32, 24), (0x7fffdf'u32, 23),
  (0xffffec'u32, 24), (0xffffed'u32, 24), (0x3fffd7'u32, 22), (0x7fffe0'u32, 23),
  (0xffffee'u32, 24), (0x7fffe1'u32, 23), (0x7fffe2'u32, 23), (0x7fffe3'u32, 23),
  (0x7fffe4'u32, 23), (0x1fffdc'u32, 21), (0x3fffd8'u32, 22), (0x7fffe5'u32, 23),
  (0x3fffd9'u32, 22), (0x7fffe6'u32, 23), (0x7fffe7'u32, 23), (0xffffef'u32, 24),
  (0x3fffda'u32, 22), (0x1fffdd'u32, 21), (0xfffe9'u32, 20), (0x3fffdb'u32, 22),
  (0x3fffdc'u32, 22), (0x7fffe8'u32, 23), (0x7fffe9'u32, 23), (0x1fffde'u32, 21),
  (0x7fffea'u32, 23), (0x3fffdd'u32, 22), (0x3fffde'u32, 22), (0xfffff0'u32, 24),
  (0x1fffdf'u32, 21), (0x3fffdf'u32, 22), (0x7fffeb'u32, 23), (0x7fffec'u32, 23),
  (0x1fffe0'u32, 21), (0x1fffe1'u32, 21), (0x3fffe0'u32, 22), (0x1fffe2'u32, 21),
  (0x7fffed'u32, 23), (0x3fffe1'u32, 22), (0x7fffee'u32, 23), (0x7fffef'u32, 23),
  (0xfffea'u32, 20), (0x3fffe2'u32, 22), (0x3fffe3'u32, 22), (0x3fffe4'u32, 22),
  (0x7ffff0'u32, 23), (0x3fffe5'u32, 22), (0x3fffe6'u32, 22), (0x7ffff1'u32, 23),
  (0x3ffffe0'u32, 26), (0x3ffffe1'u32, 26), (0xfffeb'u32, 20), (0x7fff1'u32, 19),
  (0x3fffe7'u32, 22), (0x7ffff2'u32, 23), (0x3fffe8'u32, 22), (0x1ffffec'u32, 25),
  (0x3ffffe2'u32, 26), (0x3ffffe3'u32, 26), (0x3ffffe4'u32, 26), (0x7ffffde'u32, 27),
  (0x7ffffdf'u32, 27), (0x3ffffe5'u32, 26), (0xfffff1'u32, 24), (0x1ffffed'u32, 25),
  (0x7fff2'u32, 19), (0x1fffe3'u32, 21), (0x3ffffe6'u32, 26), (0x7ffffe0'u32, 27),
  (0x7ffffe1'u32, 27), (0x3ffffe7'u32, 26), (0x7ffffe2'u32, 27), (0xfffff2'u32, 24),
  (0x1fffe4'u32, 21), (0x1fffe5'u32, 21), (0x3ffffe8'u32, 26), (0x3ffffe9'u32, 26),
  (0xffffffd'u32, 28), (0x7ffffe3'u32, 27), (0x7ffffe4'u32, 27), (0x7ffffe5'u32, 27),
  (0xfffec'u32, 20), (0xfffff3'u32, 24), (0xfffed'u32, 20), (0x1fffe6'u32, 21),
  (0x3fffe9'u32, 22), (0x1fffe7'u32, 21), (0x1fffe8'u32, 21), (0x7ffff3'u32, 23),
  (0x3fffea'u32, 22), (0x3fffeb'u32, 22), (0x1ffffee'u32, 25), (0x1ffffef'u32, 25),
  (0xfffff4'u32, 24), (0xfffff5'u32, 24), (0x3ffffea'u32, 26), (0x7ffff4'u32, 23),
  (0x3ffffeb'u32, 26), (0x7ffffe6'u32, 27), (0x3ffffec'u32, 26), (0x3ffffed'u32, 26),
  (0x7ffffe7'u32, 27), (0x7ffffe8'u32, 27), (0x7ffffe9'u32, 27), (0x7ffffea'u32, 27),
  (0x7ffffeb'u32, 27), (0xffffffe'u32, 28), (0x7ffffec'u32, 27), (0x7ffffed'u32, 27),
  (0x7ffffee'u32, 27), (0x7ffffef'u32, 27), (0x7fffff0'u32, 27), (0x3ffffee'u32, 26),
  (0x3fffffff'u32, 30)]

const eosSymbol = 256

type Node = ref object
  sym: int          ## 0..255 for a leaf, -1 for an internal node
  child: array[2, Node]

proc buildTree(): Node =
  result = Node(sym: -1)
  for sym in 0 .. 255:
    let (code, bits) = codes[sym]
    var node = result
    for k in countdown(bits - 1, 0):
      let bit = int((code shr k) and 1)
      if node.child[bit] == nil:
        node.child[bit] = Node(sym: -1)
      node = node.child[bit]
    node.sym = sym

let decodeTree = buildTree()

proc huffmanDecode*(data: string): string {.gcsafe.} =
  # `decodeTree` is built once and never mutated, so reading it from a gcsafe
  # (chronos async) context is safe.
  {.cast(gcsafe).}:
    var node = decodeTree
    for ch in data:
      let b = uint8(ch)
      for k in countdown(7, 0):
        let bit = int((b shr k) and 1)
        node = node.child[bit]
        if node == nil:
          raise newException(ValueError, "hpack: invalid Huffman code")
        if node.sym >= 0:
          result.add char(node.sym)
          node = decodeTree
    # A dangling partial path is the 1-bit EOS padding; anything else is malformed.
    if node != decodeTree and node.child[1] == nil and node.child[0] != nil:
      raise newException(ValueError, "hpack: truncated Huffman code")

proc huffmanEncode*(s: string): string =
  var acc: uint64 = 0
  var nbits = 0
  for ch in s:
    let (code, bits) = codes[uint8(ch)]
    acc = (acc shl bits) or code
    nbits += bits
    while nbits >= 8:
      nbits -= 8
      result.add char(uint8((acc shr nbits) and 0xff))
  if nbits > 0:
    let pad = 8 - nbits
    result.add char(uint8(((acc shl pad) or ((1'u64 shl pad) - 1)) and 0xff))

proc huffmanLength*(s: string): int =
  ## Encoded length in bytes, for deciding whether Huffman helps.
  var bits = 0
  for ch in s: bits += codes[uint8(ch)].bits
  (bits + 7) div 8

const eos {.used.} = eosSymbol
