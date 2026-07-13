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
