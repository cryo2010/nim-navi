## HTTP Digest access authentication (RFC 7616 / RFC 2617).
##
## Pure: no I/O. The engine sends a request, and if the server answers 401 with a
## `WWW-Authenticate: Digest ...` challenge, it calls `digestAuthHeader` to build
## the `Authorization: Digest ...` response and retries once.
##
## The MD5, MD5-sess, SHA-256, and SHA-256-sess algorithms are supported (the
## hash follows the challenge's `algorithm`; an absent one means MD5). The `qop`
## values `auth` and the legacy no-qop form are supported; `auth-int` (which
## hashes the request body) is not.

import std/[strutils, random, times, tables, options]
import checksums/md5, checksums/sha2
export options

type
  DigestChallenge* = object
    realm*, nonce*, opaque*, algorithm*, qop*: string

proc parseChallenge*(header: string): Option[DigestChallenge]
  {.raises: [].} =
  ## Parse a `WWW-Authenticate` header value. Returns none unless it is a Digest
  ## challenge carrying at least a realm and nonce (what we need to answer).
  let h = header.strip()
  if not h.toLowerAscii.startsWith("digest"):
    return none(DigestChallenge)
  var params = initTable[string, string]()
  var i = "digest".len            # past the scheme name
  while i < h.len:
    while i < h.len and (h[i] in {' ', ',', '\t'}): inc i
    let eq = h.find('=', i)
    if eq < 0: break
    let key = h[i ..< eq].strip().toLowerAscii
    var j = eq + 1
    var value: string
    if j < h.len and h[j] == '"':          # quoted string (may contain commas)
      inc j
      let start = j
      while j < h.len and h[j] != '"': inc j
      value = h[start ..< j]
      inc j                                # past closing quote
    else:
      let start = j
      while j < h.len and h[j] != ',': inc j
      value = h[start ..< j].strip()
    params[key] = value
    i = j
  if "realm" notin params or "nonce" notin params:
    return none(DigestChallenge)
  some(DigestChallenge(
    realm: params.getOrDefault("realm"),
    nonce: params.getOrDefault("nonce"),
    opaque: params.getOrDefault("opaque"),
    algorithm: params.getOrDefault("algorithm"),
    qop: params.getOrDefault("qop")))

proc md5hex(s: string): string = $toMD5(s)

proc sha256hex(s: string): string =
  var h = initSha_256()
  h.update(s)
  $h.digest()

proc pickQop(offered: string): string =
  ## Choose `auth` when the server offers it (possibly among a list); return ""
  ## for the legacy no-qop challenge. `auth-int` alone is unsupported.
  for candidate in offered.split(','):
    if candidate.strip().toLowerAscii == "auth":
      return "auth"
  ""

proc newCnonce(): string =
  var rng = initRand(getTime().toUnix xor getTime().nanosecond)
  for _ in 0 ..< 16:
    result.add("0123456789abcdef"[rng.rand(15)])

proc digestAuthHeader*(user, pass, httpMethod, uri: string,
                       ch: DigestChallenge, cnonce = ""): string =
  ## Build the `Authorization: Digest ...` value answering `ch`. `uri` is the
  ## request-target (path plus query); `httpMethod` is the verb. `cnonce` is
  ## generated when empty; pass one only to make the output deterministic (tests).
  ##
  ## Returns "" when the challenge's `algorithm` is not one we implement (only
  ## MD5/MD5-sess and SHA-256/SHA-256-sess, or absent = MD5). Answering an
  ## unsupported algorithm with an MD5 digest while echoing that algorithm would
  ## be a self-contradictory header the server rejects, so the caller should skip
  ## the retry instead.
  let algo = ch.algorithm.toLowerAscii
  let sessAlg = algo.endsWith("-sess")
  let base = if sessAlg: algo[0 ..< algo.len - "-sess".len] else: algo
  if base notin ["", "md5", "sha-256"]:
    return ""
  let useSha256 = base == "sha-256"
  template h(s: string): string =
    (if useSha256: sha256hex(s) else: md5hex(s))

  var ha1 = h(user & ":" & ch.realm & ":" & pass)
  let qop = pickQop(ch.qop)
  let cnonce = if cnonce.len > 0: cnonce else: newCnonce()
  const nc = "00000001"
  if sessAlg:
    ha1 = h(ha1 & ":" & ch.nonce & ":" & cnonce)
  let ha2 = h(httpMethod & ":" & uri)
  let response =
    if qop.len > 0:
      h(ha1 & ":" & ch.nonce & ":" & nc & ":" & cnonce & ":" & qop & ":" & ha2)
    else:
      h(ha1 & ":" & ch.nonce & ":" & ha2)

  result = "Digest username=\"" & user & "\", realm=\"" & ch.realm &
           "\", nonce=\"" & ch.nonce & "\", uri=\"" & uri &
           "\", response=\"" & response & "\""
  if qop.len > 0:
    result.add ", qop=" & qop & ", nc=" & nc & ", cnonce=\"" & cnonce & "\""
  if ch.opaque.len > 0:
    result.add ", opaque=\"" & ch.opaque & "\""
  if ch.algorithm.len > 0:
    result.add ", algorithm=" & ch.algorithm
