## HTTP CONNECT proxy tunnelling on the sync backend.
##
## Driven by tests/interop/http_connect.sh, which starts a TLS origin behind
## three HTTP CONNECT proxies: one that splits the 200 reply across two TCP
## segments, one whose 200 reply is padded past a single 1 KiB read, and one that
## answers 407. The first two must produce a working tunnel (a one-recv reader
## leaves proxy header bytes on the socket and the TLS handshake fails on them);
## the third must surface the proxy status line, not a TLS error.
import unittest
import std/[os, strutils]
import navi

let
  target = getEnv("NAVI_CONNECT_TARGET")   # https://127.0.0.1:port/
  ca = getEnv("NAVI_CONNECT_CA")
  split = getEnv("NAVI_CONNECT_SPLIT")     # http://127.0.0.1:port
  big = getEnv("NAVI_CONNECT_BIG")
  deny = getEnv("NAVI_CONNECT_DENY")

proc client(proxy: string): Navi =
  var cfg = initNaviConfig()
  cfg.proxy = proxy
  cfg.tls.caFile = ca
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  newNavi(cfg)

proc statusVia(proxy: string): int =
  let api = client(proxy)
  defer: api.close()
  api.get(target).status

proc errorVia(proxy: string): string =
  let api = client(proxy)
  defer: api.close()
  try:
    discard api.get(target); ""
  except CatchableError as e:
    e.msg

suite "HTTP CONNECT proxy (sync backend)":
  test "a split CONNECT reply should still tunnel a TLS request":
    check statusVia(split) == 200   # openssl s_server -www answers 200

  test "a CONNECT reply larger than one read should still tunnel a TLS request":
    check statusVia(big) == 200

  test "a 407 CONNECT reply should surface the proxy status line":
    let msg = errorVia(deny)
    check "proxy CONNECT failed" in msg
    check "407" in msg
