# Security

navi is an HTTP **client**, so it sits on the untrusted-input boundary from the
other side: it decides *who to trust* (TLS), and then it parses
server-controlled bytes on every response — status lines, headers, HPACK, HTTP/2
frames, compressed bodies. A malicious or compromised server, or an attacker on
the path, is the threat, and this document describes what navi defends against,
how to tune the defenses, and how they are verified.

The guiding principle is **verify, and never over-trust the peer**: certificate
and hostname verification are on by default, credentials never follow a request
across an origin boundary, and everything a hostile server can make the client
do — allocate memory, wait, retry — is bounded. Defaults are secure; the one
place bounding is opt-in (response body size) is called out below.

## Threat coverage

| Threat | Defense | Verified by |
|--------|---------|-------------|
| Man-in-the-middle / forged certificate | Chain verification (`SSL_VERIFY_PEER` + `SSL_get_verify_result`) and hostname match (`X509_check_host`) on by default; a bad cert aborts the handshake | `badssl.nim`, `mtls.sh` |
| TLS downgrade to a weak version | `minVersion` / `maxVersion` set explicitly on the context, independent of the system OpenSSL default | `tls_version.sh` |
| Weak cipher negotiation | `ciphers` (TLS 1.2) / `cipherSuites` (TLS 1.3) selection; an unusable value fails fast rather than silently falling back | `cipher_suite.sh` |
| Credential leak across a redirect | `Authorization` is stripped and `Cookie` dropped when the redirect changes origin | `redirect.nim`, `test_entries` |
| Cookie sent to the wrong host | RFC 6265 domain/path matching in the jar; a `Set-Cookie` for an unrelated domain is rejected | `test_cookies` |
| HPACK decompression bomb | Decoder bounds the decoded header-list size *during* decode, and rejects a dynamic-table update above the advertised maximum | `test_h2_hpack` |
| HTTP/2 CONTINUATION flood | Per-stream 128 KiB header-accumulation cap; the stream is RST past it | `test_h2_conn` |
| Unbounded / oversized response body | `maxResponseBytes` caps the body (counting *decompressed* bytes on native backends), raising `ResponseTooLargeError` | `test_entries`, `test_h2_conn`, `test_stream_decompress` |
| Malformed HTTP/2 framing | Frame-size, padding, and protocol-error checks send a connection error instead of misbehaving | `test_h2_conn`, `test_h2_frame` |
| Hostile `Retry-After` stalling the client | Backoff is clamped to `retry.maxDelay` | `retry.nim`, `test_async` |
| Slow / stalled server | Per-phase `connect` / `read` / `total` timeouts | `test_entries` |
| Unsafe retry of a non-idempotent request | Only idempotent verbs are retried, plus provably-unprocessed h2 streams (REFUSED_STREAM / above GOAWAY) | `test_h2_conn` |

## Configuration

Security-relevant fields live on `NaviConfig` (built with `initNaviConfig`) and,
for TLS, on `config.tls` (`TlsConfig`). A value of `0` or `""` disables the
corresponding check where noted.

| Setting | Default | Purpose |
|---------|---------|---------|
| `tls.verify` | `true` | Verify the certificate chain **and** hostname; `false` disables both (test only) |
| `tls.caFile` | `""` | Custom CA bundle to trust; `""` uses the system trust store |
| `tls.minVersion` | `tlsDefault` | Lowest TLS version to negotiate (`tls12` / `tls13` pin the floor) |
| `tls.maxVersion` | `tlsDefault` | Highest TLS version to negotiate |
| `tls.ciphers` | `""` | OpenSSL cipher list for TLS ≤ 1.2 (`""` keeps OpenSSL's default) |
| `tls.cipherSuites` | `""` | OpenSSL ciphersuites for TLS 1.3 (`""` keeps OpenSSL's default) |
| `maxResponseBytes` | `0` (unbounded) | Cap on the response body; counts decompressed bytes (decompression-bomb guard) |
| `maxRedirects` | `20` | Redirects to follow; `0` returns the 3xx as-is |
| `retry.limit` | `2` | Retry attempts for transient failures (idempotent verbs only) |
| `retry.maxDelay` | `10000` | Upper bound on backoff, ms — also clamps a hostile `Retry-After` |
| `timeouts.connect` | `0` (off) | TCP connect + TLS handshake deadline, ms |
| `timeouts.read` | `0` (off) | Per-read idle deadline (a stalled response chunk), ms |
| `timeouts.total` | `0` (off) | Whole-request deadline including retries/redirects, ms |
| `decompress` | `true` | Decode gzip/deflate/br response bodies |

## Defenses in detail

### Transport security (TLS)

On the native OpenSSL backends (sync, asyncdispatch) navi builds the `SSL_CTX`
through std/net's `newContext` — which owns the security-critical chain and
trust-store handling — and layers its own policy on top:

- **Verification is on by default.** `tls.verify` seeds `CVerifyPeer`
  (`SSL_VERIFY_PEER`), the handshake aborts on an untrusted chain, and navi
  additionally confirms `SSL_get_verify_result == X509_V_OK` and matches the
  requested host against the certificate's SAN/CN with `X509_check_host`
  (skipped for IP literals, as std/net does). Turning it off is a deliberate,
  explicit `verify = false`.
- **Version pinning.** `minVersion` / `maxVersion` are set on the context (via
  `SSL_CTX_ctrl`), with the return value checked, so a pin is enforced
  regardless of the library default — never silently ignored.
- **Cipher selection.** `ciphers` and `cipherSuites` restrict TLS 1.2 and TLS
  1.3 respectively; a value with no usable cipher raises at context build,
  before any bytes leave the process.
- **Custom trust.** `caFile` replaces the system trust store when a private CA
  is in use (e.g. mTLS interop, corporate roots).
- **Session resumption** (on by default) is scoped per origin (`host:port`), so
  a session is only ever presented back to the server it came from.

### Credentials and redirects

Following a redirect must not hand a secret to a host that did not have it.
`redirectRequest` (core/redirect.nim) rebuilds the follow-up request and:

- **strips `Authorization`** when the redirect target's origin differs from the
  current one;
- **drops the `Cookie` header** unconditionally and recomputes it from the jar
  for the new target, so cookies are re-scoped by domain/path rather than
  replayed verbatim;
- degrades a non-idempotent method to `GET` (and drops the body) on 301/302/303,
  matching fetch/browser behavior; 307/308 preserve method and body.

The cookie jar itself enforces RFC 6265 domain and path matching: a host-only
cookie is not sent to a subdomain, a `Domain` cookie is, a `Set-Cookie` for an
unrelated domain is rejected, and path matching respects `/` boundaries.

### Response parsing (untrusted server bytes)

The sans-io parsers treat the response as adversarial:

- **HPACK bomb.** A small header block can reference large dynamic-table entries
  repeatedly. The decoder accumulates the decoded field-list size and raises
  *during* decode once it passes its cap, rather than expanding first, and it
  rejects a dynamic-table size update above the capacity navi advertised
  (`SETTINGS_HEADER_TABLE_SIZE`, 4096).
- **CONTINUATION flood.** A peer streaming endless CONTINUATION frames to grow a
  single header list is bounded by a 128 KiB per-stream accumulation cap; the
  stream is RST when it is exceeded.
- **Body size.** `maxResponseBytes` bounds the body. On the native backends the
  cap wraps the decoding sink, so it counts *decompressed* bytes — a
  decompression-bomb guard — and the overflowing chunk is never delivered; for
  HTTP/2 the stream is RST. It defaults to `0` (unbounded), so set it when
  talking to untrusted servers.
- **Framing.** Frame-size violations, padding that meets or exceeds the payload,
  and other protocol errors raise a connection error instead of being processed.

### Hostile-server tarpits and retries

- **Retry-After clamp.** A `Retry-After` value is honored but clamped to
  `retry.maxDelay` (10 s), so a server cannot park the client for hours.
- **Per-phase timeouts.** `connect`, `read`, and `total` bound establishment, a
  stalled response chunk, and the whole request; `total` is enforced on all four
  backends, `connect`/`read` on the native ones.
- **Retry safety.** Only idempotent verbs are retried by default; a
  non-idempotent request is retried only when the server proves it was never
  processed (h2 REFUSED_STREAM or a stream above the peer's GOAWAY).

### Backend differences

- **chronos** uses BearSSL: verification is on by default (the same `verify`
  flag), custom `caFile` is honored, but client certificates, cipher selection,
  and TLS 1.3 are not available (documented, and cipher selection raises rather
  than silently ignoring the request).
- **navi/js** dials through the runtime's `fetch`, so the TLS handshake,
  certificate verification, and redirect mechanics are the browser's / Node's;
  navi's body cap there counts wire bytes, since the runtime owns decoding.

## Testing

Security behavior is covered at three levels.

- **Unit (pure, fast).** `test_h2_hpack.nim` drives the HPACK decoder with the
  RFC 7541 vectors, malformed input that must be rejected without crashing, and
  a decoded-list bomb that must raise. `test_h2_conn.nim` exercises the h2
  connection's DoS limits, flow control, padding, and retry classification
  against simulated server frames. `test_cookies.nim` covers RFC 6265
  domain/path scoping. `test_entries.nim` covers `maxResponseBytes`, per-phase
  timeouts, and the TLS config surface.
- **Integration (live server).** `mtls.sh` requires a client certificate,
  `tls_version.sh` and `cipher_suite.sh` pin an `openssl s_server` to a version /
  cipher and assert navi's policy is enforced, and `tls_fallback.sh` exercises
  handshake-aware address fallback. All run in the `interop` CI job.
- **Conformance / network.** `badssl.nim` (a nightly / on-demand job, since it
  hits badssl.com) asserts navi rejects invalid certificates with verification
  on and accepts a valid one; the nghttpd and multi-server interop suites run
  the h2 parser against real reference servers.

Run the fast suites with `nimble test`.

## Fuzzing

The sans-io decoders that consume untrusted server bytes have coverage-guided
libFuzzer harnesses in `tests/fuzz/`:

- `fuzz_h1` — the HTTP/1.1 response parser (whole-buffer and split feed).
- `fuzz_hpack` — the HPACK decoder across multiple blocks, so dynamic-table
  state carries between inputs.
- `fuzz_frame` — the HTTP/2 frame decoder.
- `fuzz_huffman` — HPACK Huffman decoding.
- `fuzz_h2conn` — the HTTP/2 client connection driven by attacker frames.

Every PR runs both a portable seed-corpus **replay** under ASan/UBSan and a
120 s coverage-guided run per target (`fuzz.yml`, 5 targets × 2). Locally,
`nimble fuzz` builds `tests/fuzz/Dockerfile` (clang + the libFuzzer runtime) and
fuzzes a target, exiting non-zero on a crash:

```
nimble fuzz
# or a specific target / duration via the script:
bash tests/fuzz/run.sh hpack 120
```

## Reporting a vulnerability

navi is pre-1.0. Please report suspected vulnerabilities privately to the
maintainer (Craig Younker, cryo2010@gmail.com) or via a private security
advisory on the repository, rather than opening a public issue. A short
description and a reproducing input (or a server that elicits the behavior) are
enough to start.

## Application responsibilities

Some client-side risks depend on how navi is used and belong to the calling
application, not the library:

- **SSRF.** navi connects to whatever URL it is given and follows redirects to
  wherever they point. If URLs (or redirect targets) can be attacker-influenced,
  the application must validate the destination (deny internal ranges, cloud
  metadata IPs, etc.) — navi does not, because "which hosts are safe to reach"
  is deployment policy. `maxRedirects = 0` lets you inspect a 3xx before
  following it yourself.
- **Certificate pinning.** navi supports a custom CA (`caFile`) plus version and
  cipher pinning, but not SPKI/public-key pinning. Pin the trust anchor via
  `caFile` if you need to constrain the accepted chain.
- **Secret handling.** `Authorization` set via `config.auth` or a header is
  stripped across origins on redirects, but secrets placed directly in a URL
  (query string) are the application's to manage.

## Known limitations and roadmap

These are future work, not open holes:

- **Response body is unbounded by default.** `maxResponseBytes` defaults to `0`;
  set it when consuming untrusted servers. Header/HPACK growth is bounded
  regardless of this setting.
- **Revocation** is left to OpenSSL's defaults; navi does not add OCSP/CRL
  checking on top.
- **Persisted fuzz corpus.** The harnesses run per PR from the committed seed
  corpus; there is no OSS-Fuzz-style growing corpus yet, so each run starts
  cold.
