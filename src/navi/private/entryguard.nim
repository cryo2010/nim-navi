## Enforces that exactly one navi entry module is imported.
##
## Each entry module (`navi`, `navi/asyncdispatch`, `navi/chronos`) calls
## `claimEntry` at import time. The claim runs at compile time and records the
## chosen backend in a compile-time global; a conflicting second claim aborts
## compilation with a clear message instead of failing later on an ambiguous
## `newNavi` overload.

import std/macros

var claimed {.compileTime.} = ""

macro claimEntry*(name: static string): untyped =
  if claimed.len > 0 and claimed != name:
    error("navi: import only one entry module, but both '" & claimed &
          "' and '" & name & "' were imported. Choose one of " &
          "navi (sync), navi/asyncdispatch, navi/chronos, or navi/js.")
  claimed = name
  newEmptyNode()
