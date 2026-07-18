# WebSocket examples

The same round trip — connect, send a message, receive it echoed back — on each
of navi's four backends. They all talk to one small echo server built from
navi's sans-io WebSocket core.

## Run everything with one command (Docker)

```sh
nimble demoWs
```

Builds and runs one container: the sync/asyncdispatch/chronos clients print
their round trip in the logs, and the navi/js page is served at
<http://localhost:8000/> for your browser. See `demos/websocket/`. To run the
pieces by hand instead (no Docker), read on.

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

## 4. Over TLS (wss)

The same round trip, encrypted. The `wss_*` files mirror the plain ones but use
a `wss://` URL and a `TlsConfig`; the server wraps each connection in TLS and
generates a self-signed cert for localhost on first run. Compile with `-d:ssl`:

```sh
nim c -r -d:ssl examples/websocket/wss_echo_server.nim   # one terminal (needs openssl)
nim c -r -d:ssl examples/websocket/wss_sync.nim           # another
nim c -r -d:ssl examples/websocket/wss_asyncdispatch.nim
nim c -r -d:ssl examples/websocket/wss_chronos.nim
```

The only difference from the plain clients is the URL and:

```nim
let api = newNavi(NaviOptions(tls: TlsConfig(verify: some(false))))
```

`verify` is off because the demo cert is self-signed — a real deployment would
verify against a trusted CA (`TlsConfig(caFile: "ca.pem")`). Note that chronos
(BearSSL) can *only* use `verify: false` for a self-signed cert, since it can't
add a custom CA.

**Browser (navi/js) over wss:** the js client speaks wss unchanged — just give
`websocket` a `wss://` URL. But the browser owns TLS trust and rejects
self-signed certs from script (there's no `verify: false`), so browser wss needs
a cert the browser already trusts: a real certificate, or a locally-trusted one
from [`mkcert`](https://github.com/FiloSottile/mkcert). With such a cert the page
behaves exactly like the `ws` version.

## Notes

- The API is the same across backends: `api.websocket(url)` then
  `send` / `receive` / `close`. On the async backends (asyncdispatch, chronos,
  js) the calls are `await`ed; on the sync backend they block.
- The `navi/js` client ignores custom handshake headers and lets the runtime
  handle ping/pong — a browser `WebSocket` can't do either from script.
