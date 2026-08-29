## Connection pool sizing: per-host cap, global cap, idle-timeout eviction.
import unittest
import std/os
import navi/core/pool

suite "pool per-host and global caps":
  test "the per-host cap should reject a push beyond the limit":
    let p = newPool[int](maxIdlePerHost = 2)
    check p.pushIdle("a", 1)
    check p.pushIdle("a", 2)
    check not p.pushIdle("a", 3)          # over the per-host cap
    check p.idleCount("a") == 2

  test "the per-host cap should be independent across hosts":
    let p = newPool[int](maxIdlePerHost = 1)
    check p.pushIdle("a", 1)
    check p.pushIdle("b", 2)              # a different host has its own budget
    check not p.pushIdle("a", 3)

  test "the global cap should reject a push once total idle is reached":
    let p = newPool[int](maxIdlePerHost = 10, maxIdle = 2)
    check p.pushIdle("a", 1)
    check p.pushIdle("b", 2)
    check not p.pushIdle("c", 3)          # global cap hit even though the host is empty

  test "a popped connection should free room under the global cap":
    let p = newPool[int](maxIdlePerHost = 10, maxIdle = 1)
    check p.pushIdle("a", 1)
    check not p.pushIdle("b", 2)
    let (found, _) = p.popIdle("a")
    check found
    check p.pushIdle("b", 2)              # room again after the pop

suite "pool idle-timeout eviction":
  test "a connection popped before the timeout should be returned":
    let p = newPool[int](idleTimeoutMs = 10_000)
    check p.pushIdle("a", 1)
    let (found, conn) = p.popIdle("a")
    check found and conn == 1

  test "a connection idle past the timeout should be evicted, not returned":
    let p = newPool[int](idleTimeoutMs = 20)
    check p.pushIdle("a", 1)
    sleep(40)
    let (found, _) = p.popIdle("a")
    check not found                       # skipped as expired
    check p.reapExpired() == @[1]         # handed back for closing

  test "reapExpired should sweep expired entries without a preceding pop":
    let p = newPool[int](idleTimeoutMs = 20)
    check p.pushIdle("a", 1)
    check p.pushIdle("a", 2)
    sleep(40)
    let reaped = p.reapExpired()
    check reaped.len == 2
    check p.idleCount("a") == 0

  test "with no timeout an old connection should still be returned":
    let p = newPool[int]()                # idleTimeoutMs = 0 -> no eviction
    check p.pushIdle("a", 1)
    sleep(20)
    let (found, _) = p.popIdle("a")
    check found
    check p.reapExpired().len == 0

  test "drain should yield idle and not-yet-reaped expired connections":
    let p = newPool[int](idleTimeoutMs = 20)
    check p.pushIdle("a", 1)
    sleep(40)
    discard p.popIdle("a")                # moves 1 to the expired list
    check p.pushIdle("b", 2)              # a live entry
    var seen: seq[int]
    for c in p.drain(): seen.add c
    check 1 in seen and 2 in seen
