# Unix domain socket support

## Goal

Let a navi client speak HTTP over a Unix domain socket (UDS) instead of a TCP
connection, so it can talk to services that only listen on a socket file: the
Docker daemon (`/var/run/docker.sock`), systemd-activated services, local
sidecars, database proxies, etc. The HTTP semantics are unchanged; only the
transport dialing differs.

## User-facing design

Add a per-client config field, not a new URL scheme. The URL still carries the
`host` (used for the `Host` header and, if TLS, the SNI/verification name) and
the `path`; the socket path only redirects *where the bytes go*. This matches Go
(`http.Transport` dial override), hyperlocal, and docker SDKs, and keeps URL
parsing untouched.

```nim
let docker = newNavi(NaviConfig(
  unixSocket: "/var/run/docker.sock",
  prefixUrl: "http://localhost"))          # host is just for the Host header
let info = docker.get("/v1.45/info")
```

Field on `NaviConfigBase` (`src/navi/core/request.nim`, near `proxy*`):

```nim
unixSocket*: string   ## connect over this Unix socket path instead of TCP;
                      ## the URL host/port are used only for Host + TLS SNI. ""
                      ## (default) uses normal TCP. Ignored by the js backend.
```

Semantics / decisions:
- **Plaintext by default; TLS still allowed.** If the URL is `https`, we still
  layer TLS over the UDS using the URL host as the SNI/verify name (rare but
  valid, e.g. some proxies). `http` URLs stay plaintext, which is the common
  case for `docker.sock`.
- **UDS bypasses proxies.** When `unixSocket` is set, ignore `proxy` /
  `HTTP(S)_PROXY` / `NO_PROXY` entirely (a proxy CONNECT over a local socket is
  nonsensical). Document this.
- **Happy Eyeballs is skipped** for UDS (there is exactly one endpoint, no
  address family race, no `getAddrInfo`).
- **Pooling stays correct** by folding the socket path into the pool key so a UDS
  connection is never handed to a TCP request for the same host:port.

## Backend support matrix

| Backend | UDS? | How |
|---|---|---|
| sync | yes | `AF_UNIX`/`SOCK_STREAM` socket + `connectUnix`, then existing TLS/plaintext path |
| asyncdispatch | yes | `createAsyncNativeSocket(AF_UNIX, SOCK_STREAM)` + non-blocking connect to `sockaddr_un` |
| chronos | yes | chronos has native Unix transports (`connect(initTAddress(path))`, `AddressFamily.Unix`) |
| js | no | browser/`fetch` cannot dial a socket path; raise a clear error if `unixSocket` is set (Node's `undici` `socketPath` is a possible future follow-up, out of scope) |

Windows: modern Windows 10+ supports `AF_UNIX`, but Nim stdlib exposure of
`sockaddr_un` there is uneven. Scope this to POSIX; on Windows, raise a
"not supported" error. Gate the POSIX path with `when defined(posix)`.

## Implementation

### 1. Config field
- Add `unixSocket*: string` to `NaviConfigBase` in `src/navi/core/request.nim`.
- Add an accessor `proc unixSocket*(opts: NaviConfigBase): string = opts.unixSocket`
  for parity with the other `wantsX`/accessor helpers, if the engine needs it.

### 2. Thread the path into `connect`
The transport contract (`src/navi/backend/api.nim`) documents
`connect(host, port, tls, cfg) -> Conn`; the real signature already carries
`proxy, alpn, connectMs, readMs, totalMs`. Add one more optional param to all
three native backends and the doc comment:

```nim
proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget, alpn: seq[string] = @[],
              connectMs = 0, readMs = 0, totalMs = 0,
              unixSocket = ""): Conn
```

Prefer a plain param over stuffing it into `TlsConfig` (which is TLS-specific and
otherwise backend-owned). Update the engine call site
(`src/navi/core/engine.nim` ~L403) to pass `client.config.unixSocket`.

### 3. Dial logic per backend

**sync (`src/navi/backend/sync.nim`, `connect`):** Add an early branch before the
`proxy.isSet` / `elif tls` chain:

```nim
if unixSocket.len > 0:
  when defined(posix):
    result.fd = unixConnect(unixSocket, establishMs)   # new helper, AF_UNIX
    if tls:
      when defined(ssl):
        (result.ctx, result.ownsCtx) = obtainContext(cfg.contextStore, cfg, alpn)
        result.slot = resumeSlot(cfg, host & ":" & $port)
        result.ssl = startClientTls(result.ctx, result.fd, host,
                                    cfg.wantsVerify, result.slot)
        result.protocol = negotiatedProtocol(result.ssl)
      else: raise ...
  else:
    raise newException(ValueError,
      "navi: Unix domain sockets are only supported on POSIX")
  established = true
  return
```

New `unixConnect(path: string, connectMs: int): SocketHandle` mirroring
`tcpConnect`: `createNativeSocket(AF_UNIX, SOCK_STREAM, 0)`, fill a
`Sockaddr_un` (`std/posix`), `setBlocking(false)` for the connect-timeout path,
`connect`, then honor `connectMs` via `select` exactly as `tcpConnect` does. The
existing establish/defer cleanup in `connect` already frees `result.fd` on
failure. Note: a UDS path longer than `sizeof(sun_path)` (~104-108 bytes) must be
rejected with a clear error.

**asyncdispatch (`src/navi/backend/asyncdispatch.nim`, `connect`):** analogous
early branch. Use `createAsyncNativeSocket(AF_UNIX, SOCK_STREAM, cint(0))`, then
a non-blocking connect to the `sockaddr_un` and register the completion callback
the way the existing async connect does. No Happy Eyeballs race (single
endpoint). TLS layering reuses the existing async TLS setup.

**chronos (`src/navi/backend/chronos.nim`, `connect`):** use chronos' native Unix
transport: `connect(initTAddress(unixSocket))` (which yields a `StreamTransport`
just like the TCP path), then the existing BearSSL wrap if `tls`. This is the
least code since chronos models Unix addresses directly.

**js (`src/navi/backend/js.nim`):** if `unixSocket.len > 0`, raise
`newException(ValueError, "navi: Unix domain sockets are not supported on the js backend")`.

### 4. Pool key
UDS connections must not be shared with TCP ones. `originKey(u: Url)`
(`src/navi/core/url.nim`) keys on scheme+host+port only. Options:
- Simplest: in the engine, build the pool key as
  `originKey(rq.url) & (if unixSocket.len > 0: "#unix:" & unixSocket else: "")`.
  Keeps `originKey` pure and confines the concern to the one call site
  (`engine.nim` ~L373).

Since `unixSocket` is a per-client field, all requests from one client share the
same suffix, so this only matters if a client mixes UDS and non-UDS via
`extend()`; the suffix keeps those pools disjoint regardless.

### 5. Docs
- README: add a short "Unix domain sockets" subsection under Usage with the
  Docker example and the "host is only for the Host header / bypasses proxy /
  POSIX + sync/async/chronos only" notes.
- `src/navi/backend/api.nim` module doc: mention the optional `unixSocket` param.
- CHANGELOG `[Unreleased]` `### Added` entry.

## Tests

`tests/test_unixsocket.nim` (guarded `when defined(posix)`):
1. **Round trip (sync).** Spawn a tiny thread that `bind`s a `SOCK_STREAM`
   `AF_UNIX` socket in a temp dir, accepts one connection, reads the request, and
   writes a canned `HTTP/1.1 200 OK` with a body. Client `get("http://localhost/")`
   with `unixSocket` set; assert status/body. Clean up the socket file.
2. **Host header uses the URL host, not the socket path.** Server echoes the
   `Host:` line; assert it equals the URL host.
3. **Path too long is rejected** with a clear error (path > `sun_path`).
4. **js backend rejects `unixSocket`** (compile a `nim js` check or a unit that
   asserts the raise) — or at minimum a `nim check --backend:js`.
5. Add an **async** variant (asyncdispatch) mirroring test 1 if the async server
   harness is cheap; otherwise cover async in the interop script.

Follow the existing test style (`tests/test_cookies.nim`): `unittest`, one
behavior per `test`, deterministic, no network. The listener binds a socket file
under a per-test temp path to stay order-independent.

## Verification
- `nim c -r --path:src -d:ssl tests/test_unixsocket.nim` passes (POSIX).
- Full suite (`tests/run.sh`) stays green; new field defaults to `""` so all
  existing TCP paths are untouched.
- Manual smoke against a real `docker.sock` if available:
  `newNavi(NaviConfig(unixSocket: "/var/run/docker.sock", prefixUrl: "http://localhost")).get("/v1.45/info")`.
- Confirm the connect-timeout (`connectMs`) is honored on a socket path that
  exists but never accepts (bind without accept) so a hang can't slip through.

## Out of scope
- Node `undici` `socketPath` support for the js backend (possible follow-up).
- Windows `AF_UNIX` (raise "not supported" for now).
- Abstract-namespace sockets (Linux `@`-prefixed); can be added later by
  detecting a leading NUL/`@` in the path.
