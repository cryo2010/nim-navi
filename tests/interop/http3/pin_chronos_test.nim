## Interop test: the chronos h3 opener enforces TlsConfig.pinnedKeys (#419).
## `openConnChronos` runs the same post-handshake pin check the sync opener does,
## before the connection is returned and therefore before it can be pooled or carry
## a request. Built with -d:ssl -d:naviHttp3.
import std/[strutils, os]
import pkg/chronos
import navi/backend/quic_chronos

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  let goodPin = getEnv("NAVI_H3_PIN")
  doAssert ca.len > 0 and goodPin.len > 0, "NAVI_H3_CA / NAVI_H3_PIN must be set"
  let badPin = (if goodPin[0] == 'A': 'B' else: 'A') & goodPin[1 .. ^1]

  # The right pin connects and serves a request.
  let ok = await openConnChronos("localhost", 4433, "localhost",
                               TlsConfig(caFile: ca, pinnedKeys: @[goodPin]))
  let r = await ok.requestOnConn("GET", "/", @[], "")
  doAssert r.status == 200, "pinned request failed: " & $r.status
  await ok.closeConn()
  echo "ok: correct SPKI pin accepted (chronos)"

  # A wrong pin is rejected, chain-valid certificate or not.
  var msg = ""
  try:
    let bad = await openConnChronos("localhost", 4433, "localhost",
                                  TlsConfig(caFile: ca, pinnedKeys: @[badPin]))
    await bad.closeConn()
  except ValueError as e:
    msg = e.msg
  doAssert "does not match any pin" in msg, "unexpected message: " & msg
  echo "ok: wrong SPKI pin rejected (chronos)"

waitFor main()
echo "NAVI HTTP/3 PIN (chronos) OK"
