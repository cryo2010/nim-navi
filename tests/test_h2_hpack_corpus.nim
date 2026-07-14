## HPACK decoder conformance against the http2jp/hpack-test-case corpus.
##
## Each story is a sequence of HPACK header blocks that share one dynamic table;
## decoding several encoders' output cross-checks navi's decoder against
## independent implementations. Corpus vendored under tests/fixtures/hpack
## (MIT; see that directory's LICENSE and README).

import unittest
import std/[os, json, strutils, algorithm, sequtils]
import navi/proto/h2/hpack

proc hexToStr(hex: string): string =
  for i in countup(0, hex.len - 2, 2):
    result.add char(parseHexInt(hex[i .. i + 1]))

proc expectedPairs(headers: JsonNode): seq[HeaderPair] =
  ## Each header is a single-key object {name: value}; order is significant.
  for h in headers.getElems:
    for name, value in h.pairs:
      result.add (name, value.getStr)

const corpusDir = currentSourcePath.parentDir / "fixtures" / "hpack"

suite "hpack decoder conformance (http2jp/hpack-test-case)":
  test "the vendored corpus is present":
    check dirExists(corpusDir)
    check toSeq(walkDirs(corpusDir / "*")).len > 0

  for encoderDir in walkDirs(corpusDir / "*"):
    let encoder = encoderDir.lastPathPart
    var stories = toSeq(walkFiles(encoderDir / "*.json"))
    stories.sort()
    for storyPath in stories:
      test encoder & "/" & storyPath.extractFilename & " decodes to expected headers":
        # Cases share one dynamic table and are applied in seqno order.
        let cases = sorted(parseFile(storyPath)["cases"].getElems,
          proc(a, b: JsonNode): int = cmp(a["seqno"].getInt, b["seqno"].getInt))
        var dec = initHpackDecoder()
        for c in cases:
          check dec.decode(hexToStr(c["wire"].getStr)) == expectedPairs(c["headers"])
