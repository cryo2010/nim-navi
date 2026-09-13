## Case-insensitive, order-preserving header collection.
##
## HTTP header field names are case-insensitive (RFC 9110 §5.1). We keep
## insertion order for stable serialization and allow repeated fields.

import std/strutils

type
  Headers* = object
    fields: seq[(string, string)]

proc initHeaders*(pairs: openArray[(string, string)] = []): Headers =
  ## Create a header set from name/value pairs.
  for (k, v) in pairs:
    result.fields.add((k, v))

proc len*(h: Headers): int {.inline.} = h.fields.len

proc add*(h: var Headers, name, value: string) =
  ## Append a header, keeping any existing field of the same name.
  h.fields.add((name, value))

proc `[]=`*(h: var Headers, name, value: string) =
  ## Set a header, replacing all existing fields of the same name.
  var written = false
  var i = 0
  while i < h.fields.len:
    if cmpIgnoreCase(h.fields[i][0], name) == 0:
      if written:
        h.fields.delete(i)
        continue
      h.fields[i] = (name, value)
      written = true
    inc i
  if not written:
    h.fields.add((name, value))

proc parseHeaderLine*(line: string): tuple[name, value: string, ok: bool] =
  ## Split one HTTP/1.x field line ("Name: value") at the first colon, stripping
  ## surrounding whitespace from both sides. `ok` is false when there is no colon
  ## in a valid position (the colon must not be the first character, RFC 9112 5),
  ## so the caller can skip a malformed line. Shared by the h1 parser (header and
  ## trailer lines) and the WebSocket handshake validator.
  let colon = line.find(':')
  if colon > 0:
    (line[0 ..< colon].strip(), line[colon + 1 .. ^1].strip(), true)
  else:
    ("", "", false)

proc splitParam*(part: string): tuple[key, value: string, hasValue: bool] =
  ## Split one delimited `key=value` token at its FIRST `=`, returning the raw
  ## (unstripped, unlowered) key and value so each caller can apply its own
  ## trimming and case rules. `hasValue` is false when there is no `=` at all,
  ## in which case `key` is the whole token and `value` is "" (a valueless
  ## attribute such as Set-Cookie `Secure`). A leading `=` yields an empty key.
  ##
  ## Used by the Set-Cookie attribute parser, which pre-splits the list on its
  ## own delimiter (`;`) and then needs the first-`=` cut per token. Splitting
  ## only on the FIRST `=` keeps any `=` in the value intact. This does NOT
  ## tokenize the list itself: the Digest challenge parser needs a single
  ## quote-aware positional pass (a `,` inside a quoted value is not a
  ## delimiter), so it is deliberately not expressed in terms of this. (Alt-Svc
  ## has a similar first-`=` idiom but a stricter empty-key guard, so it keeps
  ## its own split.)
  let eq = part.find('=')
  if eq < 0: (part, "", false)
  else: (part[0 ..< eq], part[eq + 1 .. ^1], true)

proc del*(h: var Headers, name: string) =
  ## Remove all fields matching `name` (case-insensitive).
  var i = 0
  while i < h.fields.len:
    if cmpIgnoreCase(h.fields[i][0], name) == 0:
      h.fields.delete(i)
    else:
      inc i

proc contains*(h: Headers, name: string): bool =
  for (k, _) in h.fields:
    if cmpIgnoreCase(k, name) == 0:
      return true
  false

proc get*(h: Headers, name: string, default = ""): string =
  ## First value for `name`, or `default` if absent.
  for (k, v) in h.fields:
    if cmpIgnoreCase(k, name) == 0:
      return v
  default

proc getAll*(h: Headers, name: string): seq[string] =
  ## Every value for `name`, in order (a field may appear more than once, e.g.
  ## multiple `WWW-Authenticate` challenges).
  for (k, v) in h.fields:
    if cmpIgnoreCase(k, name) == 0:
      result.add v

proc `[]`*(h: Headers, name: string): string =
  ## First value for `name`; raises KeyError if absent.
  for (k, v) in h.fields:
    if cmpIgnoreCase(k, name) == 0:
      return v
  raise newException(KeyError, "no such header: " & name)

iterator pairs*(h: Headers): (string, string) =
  for kv in h.fields:
    yield kv

proc merge*(base: Headers, overrides: Headers): Headers =
  ## Copy `base`, then apply `overrides` with replace semantics.
  result = base
  for (k, v) in overrides.fields:
    result[k] = v
