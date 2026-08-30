# Threat Model

navi is an HTTP **client**, so it sits on the untrusted-input boundary from the
client side: it decides *who to trust* (TLS), and then it parses
server-controlled bytes on every response, including status lines, headers,
HPACK, HTTP/2 frames, and compressed bodies. A malicious or compromised server,
or an attacker on the path, is the threat. This document describes what navi
defends against, organized under the [STRIDE](https://en.wikipedia.org/wiki/STRIDE_model)
framework, and how each defense is verified.

The guiding principle is **verify, and never over-trust the peer**: certificate
and hostname verification are on by default, credentials never follow a request
across an origin boundary, and everything a hostile server can make the client do
(allocate memory, wait, retry) is bounded.

To go beyond the defaults, see [HARDENING.md](HARDENING.md). To report a
vulnerability, see [SECURITY.md](SECURITY.md).

## Scope and trust boundaries

- **Trusted:** the calling application and the code it runs. navi's middleware
  runs with the application's privileges.
- **Untrusted:** the network path, the server, and any proxy. Every byte navi
  receives (status, headers, redirects, `Set-Cookie`, framing, compressed
  payloads) is treated as adversarial input.
- **Attacker positions considered:** a hostile or compromised origin server, a
  machine-in-the-middle on the network, and a malicious proxy.

navi is a library, not a sandbox: the application chooses which URLs to fetch and
supplies the credentials. Decisions that depend on deployment policy (which hosts
are safe to reach, how secrets are stored) are the application's, and are listed
under [Application responsibilities](#application-responsibilities).

## Secure-by-default posture

Defaults are secure. The security-relevant behaviors that are **on by default**:

| On by default | Mechanism |
|---------------|-----------|
| Certificate chain + hostname verification | `tls.verify = true` (`defaultTls()`) |
| TLS session resumption, scoped per origin | `tls.resumeSessions = true` |
| Cross-origin `Authorization` stripping on redirect | `core/redirect.nim` |
| Cookie re-scoping by domain/path (RFC 6265) | `core/cookies.nim` |
| Redirect cap (20) | `maxRedirects = 20` |
| Idempotent-only retries | `defaultRetryPolicy()` |
| `Retry-After` clamped to `retry.maxDelay` (10 s) | `core/retry.nim` |
| HPACK / CONTINUATION / framing bounds | `proto/h2/*` |
| HTTP/2 server push disabled | `proto/h2/conn.nim` |
| Raise on non-2xx | `throwHttpErrors = true` |

Security controls that are **opt-in** (off until you set them):

| Opt-in | Field / flag |
|--------|--------------|
| Response body cap (decompression-bomb guard) | `maxResponseBytes` (default `0`, unbounded) |
| WebSocket message cap | `websocket(..., maxMessageBytes)` (default `0`, unbounded) |
| Per-phase timeouts | `timeouts.connect` / `.read` / `.total` (default `0`) |
| TLS version floor/ceiling | `tls.minVersion` / `tls.maxVersion` |
| TLS cipher restriction | `tls.ciphers` / `tls.cipherSuites` |
| Custom CA / private trust anchor | `tls.caFile` |
| Client certificate (mTLS) | `tls.certFile` / `pkcs12File` / `certPem` |
| HTTP/3 | build with `-d:naviHttp3`, list `H3` in `http` |

[HARDENING.md](HARDENING.md) shows how to set the opt-in controls.

## STRIDE analysis

### Spoofing (is the peer who it claims to be?)

**Threat:** a machine-in-the-middle or an imposter server presents a forged or
mismatched certificate.

**Mitigation:** on the native OpenSSL backends (sync, asyncdispatch) navi builds
the `SSL_CTX` through std/net's `newContext`, then seeds `CVerifyPeer`
(`SSL_VERIFY_PEER`) from `tls.verify`, so the handshake aborts on an untrusted
chain. navi additionally confirms `SSL_get_verify_result == X509_V_OK` and
matches the requested host against the certificate's SAN/CN with `X509_check_host`
(skipped for IP literals, as std/net does). See `src/navi/backend/openssl_ctx.nim`
and `defaultTls()` in `src/navi/backend/api.nim`. A private CA is trusted via
`tls.caFile`; mutual authentication uses a client certificate
(`certFile`/`pkcs12File`/`certPem`). HTTP/3 performs the same certificate
verification against the QUIC handshake (`src/navi/backend/quic.nim`). Turning
verification off is a deliberate, explicit `tls.verify = false`.

**Verified by:** `badssl.nim` (rejects invalid certificates, accepts a valid one),
`mtls.sh` (client-certificate handshake).

**Residual risk:** SPKI/public-key pinning (`tls.pinnedKeys`) and a custom verify
callback (`tls.verifyCallback`) are available but off by default; revocation is
left to OpenSSL's defaults (no added OCSP/CRL). See
[Application responsibilities](#application-responsibilities).

### Tampering (was the response altered in transit?)

**Threat:** an on-path attacker modifies response bytes, or a hostile server
sends malformed framing to corrupt navi's state.

**Mitigation:** TLS provides integrity for the transported bytes once the peer is
authenticated (Spoofing, above). Above the transport, navi's sans-io parsers
treat the response as adversarial and reject malformed input rather than
misbehaving: the HTTP/1.1 parser rejects a negative `Content-Length` and bounds
the chunk size (`src/navi/proto/h1.nim`), and the HTTP/2 layer rejects frame-size
violations, padding that meets or exceeds the payload, and other protocol errors
with a connection error (`src/navi/proto/h2/conn.nim`, `frame.nim`).

**Verified by:** `test_h2_conn.nim`, `test_h2_frame.nim`, and the `fuzz_h1`,
`fuzz_frame` libFuzzer harnesses.

### Repudiation

**Threat / mitigation:** repudiation is minimal for an HTTP client, which does not
maintain an authoritative audit log. Request/response logging and auditing are the
application's concern; navi's middleware hook is the place to add them.

### Information Disclosure (does a secret leak to the wrong party?)

**Threat:** a redirect or a mis-scoped cookie hands a credential to a host that
should not receive it.

**Mitigation:**

- **Credentials on redirect.** Following a redirect must not hand a secret to a
  host that did not have it. `redirectRequest` (`src/navi/core/redirect.nim`)
  rebuilds the follow-up request and strips `Authorization` when the target's
  origin differs from the current one, drops the `Cookie` header unconditionally
  and recomputes it from the jar for the new target (so cookies are re-scoped, not
  replayed verbatim), and degrades a non-idempotent method to `GET` (dropping the
  body) on 301/302/303, matching browser/fetch behavior; 307/308 preserve method
  and body.
- **Cookie scoping.** The jar enforces RFC 6265 domain and path matching
  (`src/navi/core/cookies.nim`): a host-only cookie is not sent to a subdomain, a
  `Domain` cookie is, a `Set-Cookie` for an unrelated domain is rejected, path
  matching respects `/` boundaries, and the `Secure` attribute is honored (a
  Secure cookie is never sent over plaintext).
- **Session-cache scoping.** TLS session resumption is scoped per origin
  (`host:port`), so a cached session is only ever presented back to the server it
  came from.

**Verified by:** `test_cookies.nim` (RFC 6265 scoping), redirect coverage in
`test_entries.nim`.

**Residual risk:** a secret placed directly in a URL (query string) is the
application's to manage; navi re-scopes headers and cookies, not URL contents.

### Denial of Service (can a hostile server exhaust the client?)

**Threat:** a malicious server tries to make navi allocate unbounded memory, wait
indefinitely, or spin.

**Mitigation:** everything server-controlled is bounded.

- **Decompression bombs.** `maxResponseBytes` bounds the body; on the native
  backends the cap wraps the decoding sink, so it counts *decompressed* bytes and
  the overflowing chunk is never delivered (`src/navi/core/decompress.nim`,
  `response.nim`). For HTTP/2 the stream is RST. This is **opt-in** (default `0`,
  unbounded), so set it for untrusted servers.
- **HPACK bomb.** The decoder accumulates the decoded field-list size and raises
  *during* decode once it passes its cap, rather than expanding first, and rejects
  a dynamic-table size update above the advertised maximum
  (`src/navi/proto/h2/hpack.nim`).
- **CONTINUATION flood.** A peer streaming endless CONTINUATION frames to grow one
  header list is bounded by a per-stream 128 KiB accumulation cap; the stream is
  RST past it (`src/navi/proto/h2/conn.nim`).
- **Oversized chunks / framing.** The HTTP/1.1 chunk size is bounded and HTTP/2
  frame/padding violations raise, as under Tampering.
- **WebSocket frames / messages.** A single WebSocket frame is capped at 64 MiB
  (always on) and a reserved opcode or an out-of-range 64-bit length fails the
  connection (`src/navi/proto/ws.nim`). A *reassembled* message (across continuation
  frames) is bounded by the **opt-in** `websocket(..., maxMessageBytes)`; past it navi
  closes with 1009 and raises `WsMessageTooLarge`, so a peer cannot grow one message
  without bound.
- **`Retry-After` tarpit.** A `Retry-After` value is honored but clamped to
  `retry.maxDelay` (10 s default), so a server cannot park the client for hours
  (`src/navi/core/retry.nim`).
- **Stalls.** Per-phase `connect` / `read` / `total` timeouts bound
  establishment, a stalled response chunk, and the whole request. These are
  **opt-in**; `total` is enforced on all four backends, `connect`/`read` on the
  native ones.
- **HTTP/2 flow control and push.** navi advertises a bounded flow-control window
  and keeps server push disabled (`src/navi/proto/h2/conn.nim`), so a server
  cannot push unsolicited responses at the client.

**Verified by:** `test_h2_hpack.nim` (decoded-list bomb), `test_h2_conn.nim` (DoS
limits, flow control, padding), `test_entries.nim` and `test_stream_decompress.nim`
(`maxResponseBytes`), `test_ws.nim` (frame/message caps, reserved opcode, oversized
length), `retry.nim` coverage in `test_async.nim`.

### Elevation of Privilege

**Threat / mitigation:** largely not applicable to a client library. navi does
not cross a privilege boundary; its middleware runs with the calling
application's privileges and adds none. The closest real risk is a request being
steered at an internal resource (SSRF), which is a deployment-policy decision left
to the application (see below).

## Application responsibilities

Some client-side risks depend on how navi is used and belong to the calling
application, not the library:

- **SSRF.** navi connects to whatever URL it is given and follows redirects to
  wherever they point. If URLs (or redirect targets) can be attacker-influenced,
  the application must validate the destination (deny internal ranges, cloud
  metadata IPs, and so on); navi does not, because "which hosts are safe to reach"
  is deployment policy. `maxRedirects = 0` lets you inspect a 3xx before following
  it yourself.
- **Certificate pinning.** navi supports a custom CA (`tls.caFile` or the
  in-memory `tls.caBundle`), version and cipher pinning, SPKI/public-key pinning
  (`tls.pinnedKeys`), and a custom verify callback (`tls.verifyCallback`). Pin the
  trust anchor via `caFile`/`caBundle`, or pin the exact key via `pinnedKeys`, to
  constrain the accepted chain.
- **Secret handling.** `Authorization` set via `config.auth` or a header is
  stripped across origins on redirects, but secrets placed directly in a URL
  (query string) are the application's to manage.

## Known limitations

These are documented boundaries, not open holes:

- **Response body is unbounded by default.** `maxResponseBytes` defaults to `0`;
  set it when consuming untrusted servers. Header/HPACK/CONTINUATION growth is
  bounded regardless of this setting.
- **Revocation** is left to OpenSSL's defaults; navi does not add OCSP/CRL
  checking on top.
- **Fuzz corpus.** The sans-io decoders that consume untrusted bytes have
  coverage-guided libFuzzer harnesses in `tests/fuzz/` (`fuzz_h1`, `fuzz_hpack`,
  `fuzz_frame`, `fuzz_huffman`, `fuzz_h2conn`) run per PR from a committed seed
  corpus; there is no OSS-Fuzz-style growing corpus yet, so each run starts cold.

## Backend differences

- **chronos** uses BearSSL: verification is on by default (the same `tls.verify`
  flag) and a custom `caFile` is honored, but client certificates, cipher
  selection, and TLS 1.3 are not available (cipher selection raises rather than
  silently ignoring the request).
- **navi/js** dials through the runtime's `fetch`, so the TLS handshake,
  certificate verification, and redirect mechanics are the browser's or Node's;
  navi's body cap there counts wire bytes, since the runtime owns decoding.
