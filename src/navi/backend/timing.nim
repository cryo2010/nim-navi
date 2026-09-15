## Shared connection-level timeout arithmetic, so the precedence rule, the
## remaining-deadline math, and the timeout error wording are defined once and
## can never drift between the sync, asyncdispatch, chronos, and quic backends.
##
## Cycle-free: it depends only on the standard library, so every backend can
## import it alongside its transport code. Callers raise navi's `TimeoutError`
## (from `core/response`) with the messages built here.

import std/[times, monotimes]

proc establishMs*(connectMs, fallbackMs: int, defaultMs = 0): int =
  ## The establishment budget for a connect: `connectMs` wins when set, then
  ## `fallbackMs` (a total deadline caps establishment when no explicit connect
  ## limit is given), then `defaultMs` (a backend-specific floor, e.g. quic's 30s
  ## handshake default; 0 = no floor, meaning unbounded).
  if connectMs > 0: connectMs
  elif fallbackMs > 0: fallbackMs
  else: defaultMs

proc remainingMs*(deadline: MonoTime): int =
  ## Milliseconds left until `deadline` (may be <= 0 once it has lapsed).
  (deadline - getMonoTime()).inMilliseconds.int

proc connectTimeoutMsg*(ms: int): string =
  ## The canonical connect-timeout message. Kept in one place so the wording is
  ## identical across backends (h1/h2 sync + async, and the h3 handshake).
  "navi: connect timed out after " & $ms & " ms"

proc readTimeoutMsg*(ms: int): string =
  ## The canonical per-read (stall) timeout message, shared by every backend.
  "navi: read timed out after " & $ms & " ms"
