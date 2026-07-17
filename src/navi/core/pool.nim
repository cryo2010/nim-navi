## Idle-connection pool for HTTP keep-alive.
##
## Generic over the backend's `Conn` type. These operations are pure (no I/O,
## no await): they only move connections in and out of the idle set, so the
## same code serves every backend. Actually opening and closing connections
## stays with the backend, driven by the engine.
##
## Not thread-safe: intended for a single-threaded sync program or one async
## event loop, matching how a `Navi` client is used.

import std/tables

type
  Pool*[C] = ref object
    idle: Table[string, seq[C]]
    maxIdlePerHost: int

proc newPool*[C](maxIdlePerHost = 8): Pool[C] =
  Pool[C](idle: initTable[string, seq[C]](), maxIdlePerHost: maxIdlePerHost)

proc idleCount*[C](pool: Pool[C], key: string): int =
  ## Number of pooled idle connections for `key` (observability/tests).
  if pool.idle.hasKey(key): pool.idle[key].len else: 0

proc popIdle*[C](pool: Pool[C], key: string): (bool, C) =
  ## Take an idle connection for `key`, if one is available.
  if pool.idle.hasKey(key) and pool.idle[key].len > 0:
    result = (true, pool.idle[key].pop())

proc pushIdle*[C](pool: Pool[C], key: string, conn: C): bool =
  ## Offer a connection back to the pool. Returns false when the per-host idle
  ## cap is reached, signalling the caller to close `conn` instead.
  if not pool.idle.hasKey(key):
    pool.idle[key] = @[]
  if pool.idle[key].len >= pool.maxIdlePerHost:
    return false
  pool.idle[key].add(conn)
  true

iterator drain*[C](pool: Pool[C]): C =
  ## Yield every idle connection and empty the pool, for client shutdown. The
  ## caller closes each one; the backend (not this pure pool) owns closing.
  for conns in pool.idle.values:
    for c in conns: yield c
  pool.idle.clear()
