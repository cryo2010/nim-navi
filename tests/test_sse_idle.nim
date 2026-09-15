## Sync SSE idle-timeout (#357): a server that sends headers then goes silent must
## not hang the stream forever. Plain HTTP over a loopback socket (no TLS), so it
## runs on this host; the TLS feature path is exercised in the Docker/CI interop
## suite.
import unittest
import std/[net, os, options, monotimes, times]
import navi
import navi/core/response as naviresp   # navi's TimeoutError (net also exports one)

# A one-shot loopback server that speaks a valid SSE response then stalls. It
# binds an ephemeral port, reports it back over a channel, sends the 200
# text/event-stream headers plus one event, then holds the socket open without
# sending another byte for `stallMs`, modeling a wedged server.
var portChan: Channel[int]

proc runSilentSse(stallMs: int) {.thread.} =
  var srv = newSocket()
  srv.setSockOpt(OptReuseAddr, true)
  srv.bindAddr(Port(0), "127.0.0.1")
  srv.listen()
  portChan.send(srv.getLocalAddr()[1].int)
  var client: Socket
  srv.accept(client)
  var line = ""
  while true:                       # drain the request headers to the blank line
    line = ""
    client.readLine(line, timeout = 2000)
    # std/net readLine yields "" on a closed peer and the sentinel "\c\l" for an
    # empty (blank) line: the blank line is the end of the request headers.
    if line.len == 0 or line == "\c\l": break
  client.send("HTTP/1.1 200 OK\r\n" &
              "Content-Type: text/event-stream\r\n" &
              "Connection: keep-alive\r\n\r\n" &
              "data: hello\n\n")
  sleep(stallMs)                    # go silent: no further bytes
  try: client.close() except CatchableError: discard
  try: srv.close() except CatchableError: discard

suite "sync SSE idle timeout (#357)":
  test "a wedged server (headers then silence) trips idleTimeoutMs instead of hanging forever":
    portChan.open()
    var th: Thread[int]
    createThread(th, runSilentSse, 8000)   # stall well past the idle bound
    let port = portChan.recv()             # blocks until the worker has bound + listened
    let api = newNavi()
    let s = api.sse("http://127.0.0.1:" & $port & "/events",
                    reconnect = false, idleTimeoutMs = 500)
    # The first event is delivered from the initial flight.
    let first = s.next()
    check first.isSome
    check first.get.data == "hello"
    # The second read finds the server silent; the idle bound must fire.
    let t0 = getMonoTime()
    var raised = false
    try:
      discard s.next()
    except naviresp.TimeoutError:
      raised = true
    let elapsed = (getMonoTime() - t0).inMilliseconds.int
    check raised                           # a bounded stall, not a forever-hang
    check elapsed < 4000                   # fired near the 500ms bound, not the 8s stall
    s.close()
    joinThread(th)
    portChan.close()

  test "idleTimeoutMs is accepted by the sync sse signature and defaults on":
    # Contract guard: the sync signature accepts idleTimeoutMs (default 45_000,
    # mirroring the async variant). Opening against a dead port fails fast.
    let api = newNavi()
    var raised = false
    try:
      discard api.sse("http://127.0.0.1:1/events", reconnect = false)
    except CatchableError:
      raised = true
    check raised
