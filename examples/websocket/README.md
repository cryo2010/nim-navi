# WebSocket examples

The same round trip — connect, send a message, receive it echoed back — on each
of navi's four backends. They all talk to one small echo server built from
navi's sans-io WebSocket core.

## 1. Start the echo server

```sh
nim c -r examples/websocket/echo_server.nim
```

It listens on `ws://127.0.0.1:9700/` and echoes every message. Leave it running.

## 2. Run a client (native backends)

Each in its own terminal:

```sh
nim c -r examples/websocket/sync.nim            # import navi
nim c -r examples/websocket/asyncdispatch.nim   # import navi/asyncdispatch
nim c -r examples/websocket/chronos.nim         # import navi/chronos  (needs the chronos package)
```

Each prints:

```
sent: hello from the <backend> backend
recv: hello from the <backend> backend
ok
```

## 3. Run the browser client (navi/js)

`js.nim` does the same round trip with `import navi/js`, which runs over the
runtime's native `WebSocket`, and writes each step into the page.

```sh
# compile navi to JavaScript
nim js -o:examples/websocket/navi_ws.js examples/websocket/js.nim

# serve this folder and open the page (WebSocket from a file:// page is blocked
# in some browsers, so serve over http)
cd examples/websocket
python3 -m http.server 8000
# then open http://127.0.0.1:8000/index.html
```

With the echo server running, the page shows:

```
connecting to ws://127.0.0.1:9700/ ...
connected
sent: hello from the navi/js backend
recv: hello from the navi/js backend
ok - the server echoed the message back
```

The compiled `navi_ws.js` is a build artifact (git-ignored); regenerate it with
the `nim js` command above.

## Notes

- The API is the same across backends: `api.websocket(url)` then
  `send` / `receive` / `close`. On the async backends (asyncdispatch, chronos,
  js) the calls are `await`ed; on the sync backend they block.
- The `navi/js` client ignores custom handshake headers and lets the runtime
  handle ping/pong — a browser `WebSocket` can't do either from script.
