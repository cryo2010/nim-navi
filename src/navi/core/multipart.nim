## `multipart/form-data` request bodies (RFC 7578).
##
## Pure encoding, no I/O: build a `Multipart` from fields and file parts, then
## `encodeMultipart` turns it into a body string plus the matching Content-Type
## (which carries the generated boundary). `buildRequest` calls this when a
## caller passes `multipart = ...`, so it works uniformly on every backend (the
## body is just bytes with a header).

import std/[random, times, strutils]

type
  MultipartPart* = object
    name*: string          ## form field name
    content*: string       ## the field value or file bytes
    filename*: string      ## set for a file part; empty for a plain field
    contentType*: string   ## part Content-Type; empty omits it (fields) or
                           ## defaults to application/octet-stream (files)

  Multipart* = seq[MultipartPart]

proc field*(name, value: string): MultipartPart =
  ## A plain text form field.
  MultipartPart(name: name, content: value)

proc filePart*(name, filename, content: string,
               contentType = "application/octet-stream"): MultipartPart =
  ## A file upload part. An empty `filename` would make it a plain field, so a
  ## file part must name its file.
  MultipartPart(name: name, content: content, filename: filename,
                contentType: contentType)

proc randomBoundary(rng: var Rand): string =
  ## 20 random hex chars behind a recognizable prefix. Seeded per call from the
  ## clock so it works identically on the native and JavaScript backends (no
  ## `std/oids`, which the JS backend can't compile).
  result = "----naviFormBoundary"
  for _ in 0 ..< 20:
    result.add("0123456789abcdef"[rng.rand(15)])

proc newBoundary(parts: Multipart): string =
  ## A boundary guaranteed not to appear in any part content, so it can never be
  ## mistaken for a delimiter (regenerated on the astronomically rare collision).
  var rng = initRand(getTime().toUnix xor getTime().nanosecond)
  while true:
    result = randomBoundary(rng)
    block searchParts:
      for p in parts:
        if result in p.content:
          break searchParts   # collision: try another
      return

proc escapeName(s: string): string =
  ## RFC 7578 names/filenames: drop CR/LF (header injection) and encode the one
  ## delimiter that would break the quoted string.
  result = newStringOfCap(s.len)
  for c in s:
    case c
    of '\r', '\n': discard
    of '"': result.add("%22")
    else: result.add(c)

proc encodeMultipart*(parts: Multipart): tuple[body, contentType: string] =
  ## Serialize `parts` into a `multipart/form-data` body and return it with the
  ## Content-Type header value (including the generated boundary).
  let boundary = newBoundary(parts)
  var body = ""
  for p in parts:
    body.add("--" & boundary & "\r\n")
    body.add("Content-Disposition: form-data; name=\"" & escapeName(p.name) & "\"")
    if p.filename.len > 0:
      body.add("; filename=\"" & escapeName(p.filename) & "\"")
    body.add("\r\n")
    let ct = if p.contentType.len > 0: p.contentType
             elif p.filename.len > 0: "application/octet-stream"
             else: ""
    if ct.len > 0:
      body.add("Content-Type: " & ct & "\r\n")
    body.add("\r\n")
    body.add(p.content)
    body.add("\r\n")
  body.add("--" & boundary & "--\r\n")
  (body, "multipart/form-data; boundary=" & boundary)
