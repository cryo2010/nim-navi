## Deterministic content-type catalog for the requests soak, shared by every client
## variant (sync, asyncdispatch, chronos, and `nim js`). No navi import (mirrors
## `config.nim`) so all four backends build against it.
##
## The catalog rotates four body kinds through `/echo`: octet (raw binary), text
## (raw utf-8), json (a `JsonNode` on the wire, so navi's `application/json` encoder
## runs), and form (a `@[(k,v)]` seq, so navi's urlencoded encoder runs). JSON and
## form are verified *semantically* -- the server parses and canonically
## re-serializes, and the client compares parsed trees / decoded pairs -- so a
## byte-echo can never make the JSON/form paths pass. Octet/text stay byte-exact.
##
## Chronos gcsafety demands `const allPayloads = stressPayloads()`, so `Payload` is
## all value types and JSON docs are stored as *source strings* (parsed per request
## at the send site, never a non-const `JsonNode` global). The filtered seq is
## passed to workers as a parameter, never read from a mutable global.
##
## Request compression (`NAVI_REQ_COMPRESSION`) applies to octet/text only: a typed
## `JsonNode`/`form` body cannot carry a `content-encoding` without misdescribing
## the plain bytes on the wire, so json/form always ship uncompressed. Response
## compression (`NAVI_RESP_COMPRESSION` via `x-want-encoding`) applies to all kinds.

import std/[strutils, uri, algorithm]

type
  PayloadKind* = enum pkBinary, pkText, pkJson, pkForm

  Payload* = object
    kind*: PayloadKind
    name*: string                   ## for failure messages, e.g. "json-nested"
    contentType*: string            ## what the client sends / expects echoed
    text*: string                   ## raw body (pkBinary, pkText)
    jsonSrc*: string                ## deterministic JSON source text (pkJson)
    form*: seq[(string, string)]    ## field pairs (pkForm), always non-empty
    jsSafe*: bool                   ## false for binary (TextEncoder mangles it)
    reserialized*: bool             ## true when canonical echo MUST differ byte-wise
                                    ## from what was sent (unsorted-key JSON docs)

proc buildJsonLarge(): string =
  ## A ~300 KiB JSON array of varied objects, built at compile time. Big enough that
  ## a JSON body finally crosses the 64 KiB initial flow-control window and actually
  ## ships (today's large bodies were phase-locked onto bodiless verbs and never
  ## did). Keys are unsorted per object so the canonical echo differs byte-wise.
  ## Values stay round-trippable across Python and Nim: ints below 2^53, only dyadic
  ## floats (0.5, 3.25, -12.75), and ascii strings.
  var parts: seq[string]
  for i in 0 ..< 3300:
    # -- unsorted keys ("z","m","a","count","ratio","ok","tag") so a sorted-key
    # -- canonical echo can never be a byte-echo of what was sent.
    parts.add "{\"z\":" & $i &
      ",\"m\":\"item-" & $i & "\"" &
      ",\"a\":[" & $(i mod 7) & "," & $(i mod 13) & "]" &
      ",\"count\":" & $(i * 131 + 17) &
      ",\"ratio\":" & (if i mod 2 == 0: "0.5" else: "3.25") &
      ",\"ok\":" & (if i mod 3 == 0: "true" else: "false") &
      ",\"tag\":\"tag-" & $(i mod 97) & "\"}"
  result = "[" & parts.join(",") & "]"

proc buildJsonArray(): string =
  ## An array of ~512 small objects, a few KiB past the 16 KiB serialized boundary,
  ## so a JSON body crosses the h2 DATA-frame boundary as JSON. Unsorted keys.
  var parts: seq[string]
  for i in 0 ..< 512:
    parts.add "{\"id\":" & $i & ",\"name\":\"n" & $i &
      "\",\"active\":" & (if i mod 2 == 0: "true" else: "false") & "}"
  result = "[" & parts.join(",") & "]"

proc buildFormLarge(): seq[(string, string)] =
  ## ~100 non-empty fields (navi treats an empty `form` seq as "no body").
  for i in 0 ..< 100:
    result.add ("field" & $i, "value-" & $(i * 7 + 3))

proc stressPayloads*(): seq[Payload] =
  ## The full deterministic catalog. Callable at compile time
  ## (`const allPayloads = stressPayloads()`) for the chronos gcsafe path.

  # -- Binary: the existing 7 shapes (empty, tiny, small binary with \x00..\xff,
  # -- either side of the 16 KiB DATA-frame boundary, past the 64 KiB flow-control
  # -- window, and a highly-compressible 256 KiB body). jsSafe=false: TextEncoder
  # -- mangles a raw binary body, so the js cell excludes these.
  let binShapes = [
    ("octet-empty", ""),
    ("octet-tiny", "payload"),
    ("octet-small", "\x00\x01\x02\xfd\xfe\xff binary"),
    ("octet-16383", repeat("a", 16383)),
    ("octet-16385", repeat("b", 16385)),
    ("octet-65536", repeat("c", 65536)),
    ("octet-262144", repeat("z", 262144))]
  for (nm, s) in binShapes:
    result.add Payload(kind: pkBinary, name: nm,
      contentType: "application/octet-stream", text: s,
      jsSafe: false, reserialized: false)

  # -- Text: tiny ascii, a utf-8 multibyte body (accents + CJK + emoji), and a
  # -- ~32 KiB repeated-sentence body so the js text path still crosses the 16 KiB
  # -- and (with the large octet excluded) contributes a big body.
  result.add Payload(kind: pkText, name: "text-ascii",
    contentType: "text/plain", text: "the quick brown fox jumps over the lazy dog",
    jsSafe: true, reserialized: false)
  result.add Payload(kind: pkText, name: "text-unicode",
    contentType: "text/plain",
    text: "café naïve 你好世界 emoji😀 αβγ",
    jsSafe: true, reserialized: false)
  result.add Payload(kind: pkText, name: "text-large",
    contentType: "text/plain",
    text: repeat("the quick brown fox jumps over the lazy dog. ", 768),
    jsSafe: true, reserialized: false)

  # -- JSON: sent as a JsonNode (navi sets application/json). reserialized=true
  # -- wherever key order is intentionally non-sorted, so the server's sorted-key
  # -- canonical echo provably differs byte-wise from what navi put on the wire.
  result.add Payload(kind: pkJson, name: "json-small",
    contentType: "application/json",
    jsonSrc: """{"b":1,"a":"two","ok":true,"z":null}""",
    jsSafe: true, reserialized: true)
  result.add Payload(kind: pkJson, name: "json-nested",
    contentType: "application/json",
    jsonSrc: """{"z":{"y":[1,2,{"inner":true,"deep":{"k":"v","n":42}}],"x":"end"},""" &
             """"m":{"list":[{"a":1},{"a":2}],"flag":false},"a":"first"}""",
    jsSafe: true, reserialized: true)
  result.add Payload(kind: pkJson, name: "json-array",
    contentType: "application/json", jsonSrc: buildJsonArray(),
    jsSafe: true, reserialized: false)
  result.add Payload(kind: pkJson, name: "json-unicode",
    contentType: "application/json",
    jsonSrc: """{"emoji":"😀","cjk":"你好","rtl":"שלום",""" &
             """"quote":"a\"b","back":"a\\b","nl":"a\nb","tab":"a\tb","esc":"©®"}""",
    jsSafe: true, reserialized: true)
  result.add Payload(kind: pkJson, name: "json-numbers",
    contentType: "application/json",
    jsonSrc: """{"big":9007199254740991,"neg":-4503599627370496,"zero":0,""" &
             """"half":0.5,"quarter":3.25,"negf":-12.75}""",
    jsSafe: true, reserialized: true)
  result.add Payload(kind: pkJson, name: "json-large",
    contentType: "application/json", jsonSrc: buildJsonLarge(),
    jsSafe: true, reserialized: true)

  # -- Form: urlencoded, all non-empty. Values exercise navi's percent-encoding
  # -- against Python's parse_qsl decoding; repeated keys and ~100 fields too.
  result.add Payload(kind: pkForm, name: "form-simple",
    contentType: "application/x-www-form-urlencoded",
    form: @[("name", "navi"), ("lang", "nim"), ("ok", "yes")],
    jsSafe: true, reserialized: false)
  result.add Payload(kind: pkForm, name: "form-escapes",
    contentType: "application/x-www-form-urlencoded",
    form: @[("spaced", "a b c"), ("amp", "x&y"), ("eq", "k=v"),
            ("plus", "a+b"), ("pct", "100%"), ("uni", "café 你好")],
    jsSafe: true, reserialized: false)
  result.add Payload(kind: pkForm, name: "form-repeat",
    contentType: "application/x-www-form-urlencoded",
    form: @[("tag", "a"), ("tag", "b"), ("tag", "c")],
    jsSafe: true, reserialized: false)
  result.add Payload(kind: pkForm, name: "form-large",
    contentType: "application/x-www-form-urlencoded",
    form: buildFormLarge(), jsSafe: true, reserialized: false)

const kindTokens = [("octet", pkBinary), ("text", pkText),
                    ("json", pkJson), ("form", pkForm)]

proc parseKind(tok: string): (bool, PayloadKind) =
  for (name, k) in kindTokens:
    if name == tok: return (true, k)
  (false, pkBinary)

proc validContentTypes*(kindsCsv: string): (bool, string) =
  ## Validate the NAVI_CONTENT_TYPES token list; returns (ok, badToken). An empty
  ## csv is valid (defaulting is the caller's job). Callers hard-fail at startup on
  ## a bad token so a typo cannot silently narrow (or empty) the rotation.
  for raw in kindsCsv.split(','):
    let tok = raw.strip()
    if tok.len == 0: continue
    if not parseKind(tok)[0]: return (false, tok)
  (true, "")

proc filterPayloads*(all: seq[Payload], kindsCsv: string, jsSafe: bool): seq[Payload] =
  ## Apply the NAVI_CONTENT_TYPES knob (a comma list of {octet,text,json,form}) and
  ## the js binary exclusion. An empty/all-blank csv keeps every kind. Unknown
  ## tokens are ignored here (validate at startup via `validContentTypes`).
  var want: set[PayloadKind]
  for raw in kindsCsv.split(','):
    let tok = raw.strip()
    if tok.len == 0: continue
    let (ok, k) = parseKind(tok)
    if ok: want.incl k
  if want == {}: want = {pkBinary, pkText, pkJson, pkForm}
  for p in all:
    if p.kind notin want: continue
    if jsSafe and not p.jsSafe: continue
    result.add p

proc checkFormEcho*(echoed: string, sent: seq[(string, string)]): bool =
  ## Compare the server's canonical (sorted, urlencoded) form echo against what was
  ## sent, by decoding both to pairs and sorting. `std/uri.decodeQuery` decodes both
  ## `+` and `%20` as space, so Python's `+`-for-space re-encode is verified through
  ## rather than tripped over.
  var got: seq[(string, string)]
  for k, v in echoed.decodeQuery: got.add (k, v)
  var wantPairs = sent
  got.sort()
  wantPairs.sort()
  got == wantPairs
