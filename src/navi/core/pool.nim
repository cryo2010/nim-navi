## Idle-connection pool for HTTP keep-alive.
##
## Generic over the backend's `Conn` type. These operations are pure (no I/O,
## no await): they only move connections in and out of the idle set, so the
## same code serves every backend. Actually opening and closing connections
## stays with the backend, driven by the engine.
##
## Sizing knobs (all optional, 0 = unlimited):
##   * `maxIdlePerHost` -- idle connections kept per origin (default 8).
##   * `maxIdle`        -- idle connections kept across all origins.
##   * `idleTimeoutMs`  -- how long an idle connection may be pooled before it is
##     evicted. Eviction is lazy: `popIdle` skips expired entries and `reapExpired`
##     hands them back so the backend (which owns closing) can close them.
##
## Not thread-safe: intended for a single-threaded sync program or one async
## event loop, matching how a `Navi` client is used.

import std/[tables, monotimes, times]

type
  Entry[C] = tuple[conn: C, since: MonoTime]
  Pool*[C] = ref object
    idle: Table[string, seq[Entry[C]]]
    expired: seq[C]              ## entries evicted by popIdle, awaiting close
    maxIdlePerHost: int
    maxIdle: int                 ## global idle cap; 0 = unlimited
    idleTimeoutMs: int           ## max idle lifetime; 0 = no timeout

proc newPool*[C](maxIdlePerHost = 8, maxIdle = 0, idleTimeoutMs = 0): Pool[C] =
  Pool[C](idle: initTable[string, seq[Entry[C]]](),
          maxIdlePerHost: maxIdlePerHost, maxIdle: maxIdle,
          idleTimeoutMs: idleTimeoutMs)

proc totalIdle[C](pool: Pool[C]): int =
  for conns in pool.idle.values: result += conns.len

proc idleCount*[C](pool: Pool[C], key: string): int =
  ## Number of pooled idle connections for `key` (observability/tests).
  if pool.idle.hasKey(key): pool.idle[key].len else: 0

proc expiredAt[C](pool: Pool[C], e: Entry[C], now: MonoTime): bool =
  pool.idleTimeoutMs > 0 and
    (now - e.since).inMilliseconds >= pool.idleTimeoutMs

proc popIdle*[C](pool: Pool[C], key: string): (bool, C) =
  ## Take a live idle connection for `key`, if one is available. Connections that
  ## have outlived `idleTimeoutMs` are skipped and moved to the reap list (drain it
  ## with `reapExpired` and close them); this never hands out a stale connection.
  if not pool.idle.hasKey(key): return
  let now = getMonoTime()
  while pool.idle[key].len > 0:
    let e = pool.idle[key].pop()
    if pool.expiredAt(e, now):
      pool.expired.add(e.conn)          # too old: evict, let the caller close it
    else:
      return (true, e.conn)

proc pushIdle*[C](pool: Pool[C], key: string, conn: C): bool =
  ## Offer a connection back to the pool. Returns false when the per-host or global
  ## idle cap is reached, signalling the caller to close `conn` instead.
  if not pool.idle.hasKey(key):
    pool.idle[key] = @[]
  if pool.idle[key].len >= pool.maxIdlePerHost:
    return false
  if pool.maxIdle > 0 and pool.totalIdle >= pool.maxIdle:
    return false
  pool.idle[key].add((conn, getMonoTime()))
  true

proc reapExpired*[C](pool: Pool[C]): seq[C] =
  ## Return and clear connections evicted by `popIdle` (idle-timeout expiry) so the
  ## backend can close them. Also sweeps any currently-expired idle entries, so a
  ## caller can reap proactively without a preceding `popIdle`.
  if pool.idleTimeoutMs > 0:
    let now = getMonoTime()
    for conns in pool.idle.mvalues:
      var i = 0
      while i < conns.len:
        if pool.expiredAt(conns[i], now):
          pool.expired.add(conns[i].conn)
          conns.delete(i)
        else:
          inc i
  result = move(pool.expired)
  pool.expired = @[]

iterator drain*[C](pool: Pool[C]): C =
  ## Yield every idle connection and empty the pool, for client shutdown. The
  ## caller closes each one; the backend (not this pure pool) owns closing. Any
  ## not-yet-reaped expired connections are yielded too, so none leak on close.
  for conns in pool.idle.values:
    for e in conns: yield e.conn
  pool.idle.clear()
  for c in pool.expired: yield c
  pool.expired = @[]
