## A TLS handshake that fails over a Unix socket must not leak the fd, the SSL or an
## unshared SSL_CTX (issue #427). Driven by tests/interop/unixsocket.sh against a
## TLS-over-AF_UNIX server presenting a self-signed cert the system trust store does
## not know, so every attempt fails verification.
##
## The asyncdispatch backend's pkUnix branch used to have no exception handler at
## all: the connected socket stayed registered on the dispatcher, the SSL was never
## freed, and a bare TlsConfig's owned SSL_CTX was never destroyed. A value-type
## `Conn` has no destructor, so nothing downstream could reclaim any of it and each
## retry leaked another set. Linux only: it counts /proc/self/fd.
import std/[asyncdispatch, os, strutils]
import navi/backend/asyncdispatch as be
import navi/backend/api            # TlsConfig / ProxyTarget

proc fdCount(): int =
  for _ in walkDir("/proc/self/fd"): inc result

proc attempt(sock: string): Future[string] {.async.} =
  ## One https connect over the Unix socket; returns the failure message ("" if it
  ## unexpectedly succeeded). A bare TlsConfig means no contextStore, so each attempt
  ## builds an SSL_CTX of its own that only the teardown path can destroy.
  let target = ProxyTarget(kind: pkUnix, host: sock)
  try:
    discard await be.connect("uds.test", 443, true, TlsConfig(), target)
    return ""
  except CatchableError as e:
    return e.msg

proc main() {.async.} =
  let sock = getEnv("NAVI_UDS_TLS_PATH")
  let first = await attempt(sock)
  doAssert first.len > 0, "a self-signed cert should fail verification over a Unix socket"
  echo "OK  unix TLS verification failed as expected: ", first.splitLines[0]

  # Warm up first: the opening attempts also allocate OpenSSL's one-time state and
  # whatever the dispatcher grows on first use, which is not a leak.
  for _ in 0 ..< 5: discard await attempt(sock)
  let before = fdCount()
  for _ in 0 ..< 40: discard await attempt(sock)
  let after = fdCount()
  doAssert after <= before,
    "failed unix TLS connects leaked descriptors: " & $before & " -> " & $after
  echo "OK  40 failed unix TLS connects left the fd count at ", after,
       " (was ", before, ")"

waitFor main()
echo "== unix socket TLS failure teardown (asyncdispatch) passed =="
