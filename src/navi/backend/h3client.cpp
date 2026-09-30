// navi HTTP/3 client driver (C++20): a persistent QUIC connection that serves
// multiple HTTP/3 requests, using ngtcp2 (transport + OpenSSL 3.5 crypto binding)
// and nghttp3 (h3 + QPACK). navi's own code, compiled into navi by
// backend/quic.nim ({.compile.}) and driven from Nim via the extern "C" API
// below. Verified against the tests/interop/http3 Caddy origin.
//
// C++20 is used for RAII cleanup, std::string bodies/headers (no fixed caps),
// std::span over borrowed buffers, and std::string_view header parsing. The FFI
// boundary stays C: every extern "C" entry point catches all exceptions (a C++
// exception must never unwind into Nim-generated code), and the ngtcp2/nghttp3
// callbacks never throw across the C library frames that invoke them.
//
// The core is a non-blocking step function (send/recv/timer + submit/read); the
// async backends drive it from their event loop, and blocking sync wrappers drive
// it with a poll loop (navi_h3_pump). Multiplexed streams, incremental response
// reads, and streamed request bodies (a pull callback into navi, kept until acked)
// are all supported. Cert + hostname verified by default.
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>
#include <ngtcp2/ngtcp2_crypto_ossl.h>
#include <nghttp3/nghttp3.h>
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/x509v3.h>
#include <openssl/pem.h>
#include <openssl/pkcs12.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#include "h3client.h"   // NaviH3Tls: navi's TlsConfig across the FFI (#419)

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// Pull the next request-body chunk from navi (its `bodyStream` producer, which is
// synchronous): returns the chunk length and sets *out_ptr to the bytes (borrowed
// for the call only -- the driver copies them). 0 = end of body, < 0 = error.
extern "C" {
typedef std::ptrdiff_t (*NaviBodyPull)(void *env, const char **out_ptr);
}

namespace {

// --- last-error reporting (#446) ---------------------------------------------
// Every TLS and transport failure in this file used to be reported with
// fprintf(stderr, ...) plus a null/-1 return: library code writing unconditionally to
// the host process's stderr, while the Nim side raised a fixed-text QuicError naming
// no cause at all. An application could not tell a certificate rejection from a
// black-holed UDP path, and an operator whose caFile failed to parse for h3 only ever
// saw it on a stderr the process may own for something else.
//
// Each failure now records a NAVI_H3_ERR_* code and a reason here. The Nim wrappers
// read both right after a failing call (backend/quic.h3LastError) and put the reason
// into the QuicError they raise -- a QuicTlsError for the TLS codes, so a caller can
// tell a verification rejection from a network failure.
//
// Thread-local rather than per-connection: every failure is recorded on the thread
// that made the failing call and read by that same thread's raise site immediately
// after, and the slot must also carry the failures that happen before a connection
// exists (SSL_CTX_new, an unparseable caFile). A fixed buffer, so recording an error
// never allocates -- some of these paths are ngtcp2/nghttp3 callbacks that must not
// throw -- and the pointer navi_h3_last_error hands out stays valid until the next
// failure on that thread.
constexpr std::size_t kErrMax = 512;
thread_local char g_err_msg[kErrMax] = {0};
thread_local int g_err_code = NAVI_H3_ERR_NONE;

#if defined(__GNUC__)
__attribute__((format(printf, 2, 3)))
#endif
void set_error(int code, const char *fmt, ...) noexcept {
  g_err_code = code;
  std::va_list ap;
  va_start(ap, fmt);
  std::vsnprintf(g_err_msg, sizeof g_err_msg, fmt, ap);
  va_end(ap);
}

void clear_error() noexcept {
  g_err_msg[0] = '\0';
  g_err_code = NAVI_H3_ERR_NONE;
}

// The MOST RECENT queued OpenSSL error as text in `buf`, with the whole queue
// drained so a leftover entry cannot be misreported against a later failure.
// ERR_peek_last_error, not ERR_get_error: the queue is a FIFO and ERR_get_error pops
// the OLDEST entry, which on a multi-frame failure is the outermost and least
// specific one. A DER certificate handed to SSL_CTX_use_certificate_chain_file, for
// instance, queues PEM_R_NO_START_LINE first and the real reason behind it, so every
// such failure was reported as "no start line" whatever had actually gone wrong. The
// queue is cleared unconditionally, including in the empty case: an OpenSSL entry
// point can fail without queueing anything, and either way the next SSL_get_error
// read on this thread needs the queue empty.
const char *ossl_error(char *buf, std::size_t cap) noexcept {
  const unsigned long e = ERR_peek_last_error();
  if (e == 0)
    std::snprintf(buf, cap, "no OpenSSL error queued");
  else
    ERR_error_string_n(e, buf, cap);
  ERR_clear_error();
  return buf;
}

// RAII wrappers for the C resources this file manages by hand, so an acquire is freed
// on every path (including early returns and future edits) without a manual free.
struct X509Deleter { void operator()(X509 *p) const noexcept { X509_free(p); } };
using X509Ptr = std::unique_ptr<X509, X509Deleter>;

struct BioDeleter { void operator()(BIO *p) const noexcept { BIO_free(p); } };
using BioPtr = std::unique_ptr<BIO, BioDeleter>;
struct SslCtxDeleter { void operator()(SSL_CTX *p) const noexcept { SSL_CTX_free(p); } };
using SslCtxPtr = std::unique_ptr<SSL_CTX, SslCtxDeleter>;
struct Pkcs12Deleter { void operator()(PKCS12 *p) const noexcept { PKCS12_free(p); } };
using Pkcs12Ptr = std::unique_ptr<PKCS12, Pkcs12Deleter>;
struct EvpPkeyDeleter { void operator()(EVP_PKEY *p) const noexcept { EVP_PKEY_free(p); } };
using EvpPkeyPtr = std::unique_ptr<EVP_PKEY, EvpPkeyDeleter>;
struct X509StackDeleter {
  void operator()(STACK_OF(X509) *p) const noexcept { sk_X509_pop_free(p, X509_free); }
};
using X509StackPtr = std::unique_ptr<STACK_OF(X509), X509StackDeleter>;

struct AddrInfoDeleter {
  void operator()(addrinfo *p) const noexcept { freeaddrinfo(p); }
};
using AddrInfoPtr = std::unique_ptr<addrinfo, AddrInfoDeleter>;

// A raw file descriptor is not a pointer, so it needs its own guard: it closes the fd
// on scope exit unless `release()` hands ownership off first.
struct FdGuard {
  int fd = -1;
  FdGuard() = default;
  explicit FdGuard(int f) : fd(f) {}
  FdGuard(const FdGuard &) = delete;
  FdGuard &operator=(const FdGuard &) = delete;
  ~FdGuard() { if (fd >= 0) ::close(fd); }
  int release() noexcept { int f = fd; fd = -1; return f; }
};

// A produced-but-not-yet-acked request-body chunk. nghttp3 borrows the vec memory
// a data reader returns until it is acked, so a streamed chunk must stay put until
// `acked_stream_data` covers it -- hence a deque of stable std::strings, not one
// growing buffer that could reallocate and invalidate outstanding vecs.
struct BodyChunk {
  std::string data;
  std::size_t acked = 0;
};

// One in-flight request/response on the connection. Many can be live at once
// (multiplexing): each is keyed by its QUIC stream id.
struct Stream {
  long status = 0;
  std::string body;
  std::string resp_headers;                  // response fields as "name\nvalue\n"
  std::string resp_trailers;                 // trailing fields (after the body) same shape
  bool headers_done = false;  // all response headers delivered (body/end has begun)
  bool done = false;
  bool reset = false;     // closed without a normal end_stream (server reset/abort)
  std::string req_body;   // owned copy (so the caller need not keep it alive)
  // Response-length validation (RFC 9114 4.1.2): a body whose total length disagrees
  // with a declared Content-Length is malformed. `body` is drained on the streaming
  // path, so count total received bytes separately.
  bool is_head = false;           // request was HEAD -> Content-Length has no body
  long long content_length = -1;  // declared Content-Length, or -1 if absent
  unsigned long long body_total = 0;  // total DATA bytes received
  bool length_mismatch = false;   // set at end_stream when body_total != content_length
  bool cap_body = false;          // enforce the connection's max_body cap on this stream
                                  // (buffered requests only; a streaming read drains the
                                  // body incrementally and caps it navi-side instead)
  bool too_large = false;         // body exceeded the connection's max_body cap; the
                                  // driver stops buffering and navi raises TooLarge
  // Streaming request body (navi bodyStream): chunks pulled from Nim on demand and
  // kept until acked. `pull` null => this stream has no streamed body.
  std::deque<BodyChunk> out;
  bool out_eof = false;   // producer returned end-of-body (EOF flagged to nghttp3)
  NaviBodyPull pull = nullptr;
  void *pull_env = nullptr;
  bool abort = false;      // producer raised: reset just this stream (not the session)
  bool abort_sent = false; // RESET_STREAM already queued for it
  // Request trailers (RFC 9114 4.1): fields sent after the body as a trailing HEADERS
  // section. "name\nvalue\n...", submitted once the body reaches EOF (via
  // NGHTTP3_DATA_FLAG_NO_END_STREAM + nghttp3_conn_submit_trailers). Empty = none.
  std::string req_trailers;
  bool req_trailers_submitted = false;
  // WebSocket-over-h3 tunnel (RFC 9220 Extended CONNECT). The send side stays open
  // for full-duplex DATA: `tunnel_tx` holds outbound frames not yet handed to
  // nghttp3 (the reader pauses with WOULDBLOCK when it is empty, resumed by
  // navi_h3_tunnel_send), and `tunnel_fin` requests a half-close (flush EOF).
  bool is_tunnel = false;
  std::deque<std::string> tunnel_tx;
  bool tunnel_fin = false;
};

// Stream bytes that reached us before the nghttp3 session existed. The session is
// created by navi_h3_bind once the handshake completes, but the server's control
// stream (which carries its SETTINGS) can ride in the very datagram that completes
// it, and ngtcp2 never re-delivers what a recv_stream_data callback consumed. Such
// bytes are parked here and replayed into nghttp3 by navi_h3_bind, so nothing the
// peer sent is lost -- without it the peer's SETTINGS can vanish, which the Extended
// CONNECT gate (and QPACK encoder instructions) would then wait for forever.
struct PendingStreamData {
  std::int64_t id;
  std::string data;
  bool fin;
};

// Cap on the pre-bind hold-back. The window is a single handshake round trip, so a
// well-behaved peer sends a few hundred bytes; past this the peer is misbehaving and
// the connection is failed rather than buffered without bound.
constexpr std::size_t kPreBindMaxBytes = 256 * 1024;

// One QUIC/h3 connection. RAII: the destructor releases the library objects and
// the socket in the required order, replacing the old manual cleanup + goto.
struct H3Conn {
  ngtcp2_crypto_conn_ref ref{};
  ngtcp2_conn *conn = nullptr;
  nghttp3_conn *h3 = nullptr;
  int fd = -1;
  ngtcp2_path path{};
  sockaddr_storage local_ss{}, remote_ss{};
  SSL_CTX *ssl_ctx = nullptr;   // a reference to the shared, cached context (#454)
  SSL *ssl = nullptr;
  ngtcp2_crypto_ossl_ctx *ossl = nullptr;
  std::string authority;
  bool handshake_done = false;
  bool want_verify = false; // verify the peer certificate after the handshake (see below)
  bool has_abort = false;   // some stream's producer failed; reset it in send_step
  bool draining = false;    // peer closed the connection gracefully (CONNECTION_CLOSE /
                            // draining): a clean end, not a transport error
  unsigned long long max_body = 0;  // navi maxResponseBytes: cap on a single response
                                    // body the driver will buffer (0 = unlimited)
  unsigned long long handshake_timeout_ms = 0;  // navi connectMs; bounds the blocking
                                    // handshake drive loop (0 = the 120s safety net)
  // The peer's SETTINGS (RFC 9114 7.2.4), recorded by on_recv_settings.
  // `peer_settings` flips once the server's SETTINGS frame has been received, and
  // `peer_connect_protocol` carries its SETTINGS_ENABLE_CONNECT_PROTOCOL. The
  // WebSocket path gates its Extended CONNECT on both (RFC 9220 / RFC 8441 3);
  // nghttp3 has no getter for the remote settings, so the callback is the only way
  // to see them.
  bool peer_settings = false;
  bool peer_connect_protocol = false;
  std::deque<PendingStreamData> prebind;   // see PendingStreamData
  std::size_t prebind_bytes = 0;
  std::unordered_map<int64_t, Stream> streams;   // live streams by id
  // Self-pipe (RAII: closed with the connection) so another thread can interrupt the
  // pump's poll() to flush an outbound frame promptly -- the sync WebSocket's pump
  // thread. navi_h3_wake writes the write end; navi_h3_pump polls the read end.
  FdGuard wake_r, wake_w;

  // Teardown order is deliberate and load-bearing (which is why this is an explicit
  // destructor rather than per-member smart pointers whose order would follow
  // declaration order): each handle is released before the thing it depends on --
  // `conn` references `ossl` (ngtcp2_conn_set_tls_native_handle), `ossl` wraps `ssl`,
  // and `ssl` belongs to `ssl_ctx` (whose SSL_CTX_free here drops this connection's
  // reference: the shared context lives on while the cache or another connection
  // still holds one). It also relies on the library `_del` functions not
  // re-entering our callbacks (e.g. ngtcp2_conn_del must not fire on_stream_close, or
  // it would touch the already-freed `h3`). A new handle added here must be slotted in
  // by this dependency order, and freed here.
  ~H3Conn() {
    if (h3) nghttp3_conn_del(h3);
    if (conn) ngtcp2_conn_del(conn);
    if (ossl) ngtcp2_crypto_ossl_ctx_del(ossl);
    if (ssl) SSL_free(ssl);
    if (ssl_ctx) SSL_CTX_free(ssl_ctx);
    if (fd >= 0) ::close(fd);
  }
};

std::uint64_t now_ns() {
  timespec t{};
  clock_gettime(CLOCK_MONOTONIC, &t);
  return static_cast<std::uint64_t>(t.tv_sec) * NGTCP2_SECONDS + t.tv_nsec;
}

nghttp3_nv make_nv(std::string_view name, std::string_view value) {
  return nghttp3_nv{reinterpret_cast<std::uint8_t *>(const_cast<char *>(name.data())),
                    reinterpret_cast<std::uint8_t *>(const_cast<char *>(value.data())),
                    name.size(), value.size(), NGHTTP3_NV_FLAG_NONE};
}

// --- ngtcp2 / nghttp3 callbacks. Invoked from C library frames, so they must
// not let an exception escape (the append-based ones catch internally). ---

ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *r) {
  return static_cast<H3Conn *>(r->user_data)->conn;
}

void rand_cb(std::uint8_t *dest, std::size_t destlen, const ngtcp2_rand_ctx *) {
  RAND_bytes(dest, static_cast<int>(destlen));
}

int get_new_cid(ngtcp2_conn *, ngtcp2_cid *cid, std::uint8_t *token,
                std::size_t cidlen, void *) {
  if (RAND_bytes(cid->data, static_cast<int>(cidlen)) != 1)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  cid->datalen = cidlen;
  if (RAND_bytes(token, NGTCP2_STATELESS_RESET_TOKENLEN) != 1)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

int hs_done(ngtcp2_conn *, void *ud) {
  static_cast<H3Conn *>(ud)->handshake_done = true;
  return 0;
}

int on_recv_stream_data(ngtcp2_conn *conn, std::uint32_t flags,
                        std::int64_t stream_id, std::uint64_t, const std::uint8_t *data,
                        std::size_t datalen, void *ud, void *) {
  auto *c = static_cast<H3Conn *>(ud);
  int fin = (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0;
  if (!c->h3) {   // before navi_h3_bind: park it for replay instead of dropping it
    if (c->prebind_bytes + datalen > kPreBindMaxBytes) {
      set_error(NAVI_H3_ERR_PROTOCOL,
                "peer sent more than %zu bytes before the HTTP/3 session was bound",
                kPreBindMaxBytes);
      return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    try {
      c->prebind.push_back(
        {stream_id,
         std::string(reinterpret_cast<const char *>(data), datalen),
         fin != 0});
    } catch (...) {
      return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    c->prebind_bytes += datalen;
    return 0;   // the flow-control offsets are extended when it is replayed
  }
  nghttp3_ssize n = nghttp3_conn_read_stream(c->h3, stream_id, data, datalen, fin);
  if (n < 0) {
    // Recorded here, where the reason exists: all ngtcp2 passes back out is
    // NGTCP2_ERR_CALLBACK_FAILURE, and navi_h3_recv now keeps whatever this callback
    // recorded instead of overwriting it with that.
    set_error(NAVI_H3_ERR_PROTOCOL, "nghttp3 read_stream: %s",
              nghttp3_strerror(static_cast<int>(n)));
    return NGTCP2_ERR_CALLBACK_FAILURE;
  }
  ngtcp2_conn_extend_max_stream_offset(conn, stream_id, static_cast<std::uint64_t>(n));
  ngtcp2_conn_extend_max_offset(conn, static_cast<std::uint64_t>(n));
  return 0;
}

int on_acked(ngtcp2_conn *, std::int64_t stream_id, std::uint64_t, std::uint64_t datalen,
             void *ud, void *) {
  auto *c = static_cast<H3Conn *>(ud);
  if (c->h3) nghttp3_conn_add_ack_offset(c->h3, stream_id, datalen);
  return 0;
}

// The peer granted more send window for `stream_id` (MAX_STREAM_DATA). If we had
// blocked it on STREAM_DATA_BLOCKED, let nghttp3 offer its body again so a large
// streamed upload resumes instead of stalling forever.
int on_extend_max_stream_data(ngtcp2_conn *, std::int64_t stream_id, std::uint64_t,
                              void *ud, void *) {
  auto *c = static_cast<H3Conn *>(ud);
  if (c->h3 && nghttp3_conn_unblock_stream(c->h3, stream_id) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

int on_stream_close(ngtcp2_conn *, std::uint32_t, std::int64_t stream_id,
                    std::uint64_t app_error_code, void *ud, void *) {
  auto *c = static_cast<H3Conn *>(ud);
  if (c->h3) nghttp3_conn_close_stream(c->h3, stream_id, app_error_code);
  // A stream that closes without a normal end_stream (server RESET_STREAM/abort or
  // a connection-level error) would otherwise leave `done == false` forever, so a
  // waiter polling navi_h3_stream_done blocks until the whole connection dies. Mark
  // it done and flag it as reset, so the caller unblocks and raises rather than
  // returning a bogus empty response. A normally-ended stream already has done set
  // (on_end_stream runs first), so this never mislabels a successful response.
  auto it = c->streams.find(stream_id);
  if (it != c->streams.end() && !it->second.done) {
    it->second.reset = true;
    it->second.done = true;
  }
  return 0;
}

int on_recv_header(nghttp3_conn *, std::int64_t stream_id, std::int32_t,
                   nghttp3_rcbuf *name, nghttp3_rcbuf *value, std::uint8_t, void *cud,
                   void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  if (it == c->streams.end()) return 0;
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name);
  nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
  std::string_view nm{reinterpret_cast<char *>(n.base), n.len};
  std::string_view val{reinterpret_cast<char *>(v.base), v.len};
  try {
    if (nm == ":status") {
      // RFC 9110: :status is exactly three digits. Parse strictly rather than with
      // strtol, which silently accepts a leading sign or trailing garbage (#279).
      if (val.size() != 3 || val[0] < '0' || val[0] > '9' || val[1] < '0' ||
          val[1] > '9' || val[2] < '0' || val[2] > '9')
        return NGHTTP3_ERR_CALLBACK_FAILURE;
      it->second.status = (val[0] - '0') * 100 + (val[1] - '0') * 10 + (val[2] - '0');
    } else if (!nm.empty() && nm.front() != ':') {  // a regular response field
      it->second.resp_headers.append(nm).append("\n").append(val).append("\n");
      // Only trust a Content-Length from the final (>= 200) header section: recording
      // one from a 1xx interim section could flag a false length mismatch on the final
      // response (which may legitimately omit it) (#279).
      if (nm == "content-length" && it->second.status >= 200) {
        std::string vs(val);
        char *end = nullptr;
        long long cl = std::strtoll(vs.c_str(), &end, 10);
        if (!vs.empty() && end && *end == '\0' && cl >= 0)
          it->second.content_length = cl;   // ignore a malformed value (stays -1)
      }
    }
  } catch (...) {
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  return 0;
}

// Response trailers (RFC 9114 4.1): a trailing HEADERS section after the body.
// nghttp3 delivers them through this callback (distinct from recv_header). Keep the
// non-pseudo fields so navi can surface them on `res.trailers`.
int on_recv_trailer(nghttp3_conn *, std::int64_t stream_id, std::int32_t,
                    nghttp3_rcbuf *name, nghttp3_rcbuf *value, std::uint8_t, void *cud,
                    void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  if (it == c->streams.end()) return 0;
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name);
  nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
  std::string_view nm{reinterpret_cast<char *>(n.base), n.len};
  std::string_view val{reinterpret_cast<char *>(v.base), v.len};
  try {
    if (!nm.empty() && nm.front() != ':')      // pseudo-headers are invalid in trailers
      it->second.resp_trailers.append(nm).append("\n").append(val).append("\n");
  } catch (...) {
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  return 0;
}

// The response header section is complete (nghttp3 end_headers). For a final
// response (>= 200) mark the headers ready now -- crucial for a bodyless 200 such
// as a WebSocket Extended CONNECT accept (RFC 9220), which carries no DATA and no
// END_STREAM, so on_recv_data / on_end_stream would never fire to unblock the
// handshake. Interim 1xx sections (status < 200) are ignored; the final one follows.
int on_end_headers(nghttp3_conn *, std::int64_t stream_id, int, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  if (it != c->streams.end() && it->second.status >= 200)
    it->second.headers_done = true;
  return 0;
}

int on_recv_data(nghttp3_conn *, std::int64_t stream_id, const std::uint8_t *data,
                 std::size_t datalen, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  if (it == c->streams.end()) return 0;
  try {
    auto &s = it->second;
    s.headers_done = true;  // nghttp3 delivers all headers before any body
    s.body_total += datalen;   // total received (body is drained on streaming)
    // Enforce navi's maxResponseBytes at the source: once the cap is exceeded, stop
    // buffering (so a hostile/huge body cannot grow C memory without bound, matching
    // the h1/h2 cap) and flag it -- navi raises ResponseTooLargeError and frees the
    // stream. Flow control is still credited below so the peer is not stalled meanwhile.
    if (s.cap_body && c->max_body > 0 && s.body_total > c->max_body)
      s.too_large = true;
    else
      s.body.append(reinterpret_cast<const char *>(data), datalen);
  } catch (...) {
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  // nghttp3_conn_read_stream's return (extended in on_recv_stream_data) does NOT
  // count the DATA payload delivered here. Credit the CONNECTION window now (so other
  // streams on the shared connection are never starved), but DEFER the per-stream
  // credit until navi_h3_read_body drains the bytes to the app. That deferral is the
  // backpressure: a fast peer fills the ~8 MiB per-stream window and then blocks until
  // the app reads, instead of buffering the whole body in C memory (mirrors the h2
  // sink-mode gated window). Without the connection credit the shared window
  // (initial_max_data) would still leak ~body-size per stream.
  ngtcp2_conn_extend_max_offset(c->conn, datalen);
  return 0;
}

int on_end_stream(nghttp3_conn *, std::int64_t stream_id, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  if (it != c->streams.end()) {
    auto &s = it->second;
    s.headers_done = true;  // a bodyless response: headers are all there is
    s.done = true;
    // RFC 9114 4.1.2: a body whose length disagrees with a declared Content-Length is
    // malformed. Skip responses that carry no body by definition (HEAD; 1xx/204/304).
    if (!s.is_head && s.content_length >= 0 && s.status >= 200 && s.status != 204 &&
        s.status != 304 &&
        s.body_total != static_cast<unsigned long long>(s.content_length))
      s.length_mismatch = true;
  }
  return 0;
}

// nghttp3 kept some QUIC stream bytes buffered (e.g. QPACK-blocked) and has now
// released them; extend the QUIC flow-control window by that amount so the peer is
// not stalled. `on_recv_stream_data` extends by what nghttp3 consumed synchronously;
// this covers the rest. Without it a QPACK-blocked stream can wedge the connection.
int on_deferred_consume(nghttp3_conn *, std::int64_t stream_id, std::size_t consumed,
                        void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  if (c->conn) {
    ngtcp2_conn_extend_max_stream_offset(c->conn, stream_id, consumed);
    ngtcp2_conn_extend_max_offset(c->conn, consumed);
  }
  return 0;
}

// The peer's SETTINGS frame landed: record that it arrived and whether it enabled
// the Extended CONNECT protocol (RFC 9220). navi's WebSocket-over-h3 handshake waits
// for this before it may submit a CONNECT, exactly as the h2 path waits for the h2
// SETTINGS (issue #393). nghttp3 >= 1.14 deprecates `recv_settings` in favour of
// `recv_settings2` (a nghttp3_proto_settings), so bind whichever the headers offer;
// both carry enable_connect_protocol, which is all this needs.
#if defined(NGHTTP3_VERSION_NUM) && NGHTTP3_VERSION_NUM >= 0x010e00
int on_recv_settings(nghttp3_conn *, const nghttp3_proto_settings *settings,
                     void *cud) {
#else
int on_recv_settings(nghttp3_conn *, const nghttp3_settings *settings, void *cud) {
#endif
  auto *c = static_cast<H3Conn *>(cud);
  c->peer_connect_protocol = settings->enable_connect_protocol != 0;
  c->peer_settings = true;
  return 0;
}

// Submit the stream's request trailers (once) as a trailing HEADERS section. Parses
// the "name\nvalue\n..." blob into nghttp3_nv pointing into the stable req_trailers
// string; nghttp3_conn_submit_trailers copies the data, so it may be freed after.
// Called from the data reader when the body reaches EOF (the reader has already set
// NGHTTP3_DATA_FLAG_NO_END_STREAM so nghttp3 keeps the stream open for the trailers).
int submit_stream_trailers(H3Conn *c, std::int64_t stream_id, Stream &s) {
  if (s.req_trailers_submitted || s.req_trailers.empty()) return 0;
  s.req_trailers_submitted = true;
  std::vector<nghttp3_nv> nva;
  std::string_view hs{s.req_trailers};
  std::vector<std::string_view> toks;
  std::size_t start = 0;
  for (std::size_t i = 0; i < hs.size(); ++i)
    if (hs[i] == '\n') {
      toks.push_back(hs.substr(start, i - start));
      start = i + 1;
    }
  for (std::size_t i = 0; i + 1 < toks.size(); i += 2)
    nva.push_back(make_nv(toks[i], toks[i + 1]));
  if (nva.empty()) return 0;
  return nghttp3_conn_submit_trailers(c->h3, stream_id, nva.data(), nva.size());
}

// Hand nghttp3 the whole buffered request body for `stream_id` in one vec, with EOF,
// on the first call. The body is borrowed for the request's duration. When the request
// carries trailers, keep the stream open past the DATA (NO_END_STREAM) and submit them.
nghttp3_ssize read_body(nghttp3_conn *, std::int64_t stream_id, nghttp3_vec *vec,
                        std::size_t, std::uint32_t *pflags, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  *pflags |= NGHTTP3_DATA_FLAG_EOF;
  if (it == c->streams.end()) return 0;
  Stream &s = it->second;
  nghttp3_ssize nv = 0;
  if (!s.req_body.empty()) {
    vec[0].base = reinterpret_cast<std::uint8_t *>(s.req_body.data());
    vec[0].len = s.req_body.size();
    nv = 1;
  }
  if (!s.req_trailers.empty()) {
    *pflags |= NGHTTP3_DATA_FLAG_NO_END_STREAM;
    if (submit_stream_trailers(c, stream_id, s) != 0) return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  return nv;
}

// Streaming request body: pull the next chunk from navi (via the stream's `pull`
// callback) and hand it to nghttp3, keeping it alive in `out` until acked. Setting
// EOF one call after the last non-empty chunk (when the producer returns 0) matches
// nghttp3's read-until-EOF loop. Returning WOULDBLOCK is never needed: navi's
// producer is synchronous and always returns immediately (a chunk, or 0 at end).
nghttp3_ssize read_body_stream(nghttp3_conn *, std::int64_t stream_id, nghttp3_vec *vec,
                               std::size_t, std::uint32_t *pflags, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  if (it == c->streams.end()) { *pflags |= NGHTTP3_DATA_FLAG_EOF; return 0; }
  Stream &s = it->second;
  // End of body: EOF, plus (if the request carries trailers) keep the stream open for
  // a trailing HEADERS section (NO_END_STREAM) and submit it.
  auto finishBody = [&](std::uint32_t *pf) -> nghttp3_ssize {
    *pf |= NGHTTP3_DATA_FLAG_EOF;
    if (!s.req_trailers.empty()) {
      *pf |= NGHTTP3_DATA_FLAG_NO_END_STREAM;
      if (submit_stream_trailers(c, stream_id, s) != 0) return NGHTTP3_ERR_CALLBACK_FAILURE;
    }
    return 0;
  };
  if (s.out_eof || !s.pull) return finishBody(pflags);
  const char *p = nullptr;
  std::ptrdiff_t n = s.pull(s.pull_env, &p);
  if (n < 0) {
    // Producer raised. Reset just THIS stream (send_step flushes RESET_STREAM) rather
    // than failing the whole session, which would take down every other multiplexed
    // request. Pause the stream so nghttp3 stops pulling until the reset lands.
    s.abort = true;
    c->has_abort = true;
    return NGHTTP3_ERR_WOULDBLOCK;
  }
  if (n == 0) { s.out_eof = true; return finishBody(pflags); }
  try {
    s.out.push_back(BodyChunk{std::string(p, static_cast<std::size_t>(n)), 0});
  } catch (...) {
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  vec[0].base = reinterpret_cast<std::uint8_t *>(const_cast<char *>(s.out.back().data.data()));
  vec[0].len = s.out.back().data.size();
  return 1;
}

// A streamed request body's bytes have been acked; drop them from `out` so a large
// upload stays bounded by the flow-control window rather than buffering the whole
// body. Acked bytes are always a prefix of what we handed out, so free from front.
int on_body_acked(nghttp3_conn *, std::int64_t stream_id, std::uint64_t datalen,
                  void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  if (it == c->streams.end()) return 0;
  auto &out = it->second.out;
  std::uint64_t left = datalen;
  while (left > 0 && !out.empty()) {
    BodyChunk &f = out.front();
    std::size_t avail = f.data.size() - f.acked;
    std::size_t take = static_cast<std::size_t>(std::min<std::uint64_t>(left, avail));
    f.acked += take;
    left -= take;
    if (f.acked == f.data.size()) out.pop_front();
  }
  return 0;
}

// Data reader for a WebSocket-over-h3 tunnel (RFC 9220 Extended CONNECT). Unlike a
// request body, the send side has no natural EOF: it emits queued outbound frames
// as DATA and otherwise pauses with WOULDBLOCK until navi enqueues more (which calls
// nghttp3_conn_resume_stream). A half-close (tunnel_fin) flushes EOF. Handed-out
// chunks live in `out` until acked (freed by on_body_acked), bounding memory.
nghttp3_ssize read_tunnel(nghttp3_conn *, std::int64_t stream_id, nghttp3_vec *vec,
                          std::size_t, std::uint32_t *pflags, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  auto it = c->streams.find(stream_id);
  if (it == c->streams.end()) { *pflags |= NGHTTP3_DATA_FLAG_EOF; return 0; }
  Stream &s = it->second;
  if (s.abort) return NGHTTP3_ERR_WOULDBLOCK;
  if (s.tunnel_tx.empty()) {
    if (s.tunnel_fin) { *pflags |= NGHTTP3_DATA_FLAG_EOF; return 0; }
    return NGHTTP3_ERR_WOULDBLOCK;                 // paused until resume_stream
  }
  try {
    s.out.push_back(BodyChunk{std::move(s.tunnel_tx.front()), 0});
  } catch (...) {
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  s.tunnel_tx.pop_front();
  vec[0].base = reinterpret_cast<std::uint8_t *>(const_cast<char *>(s.out.back().data.data()));
  vec[0].len = s.out.back().data.size();
  return 1;
}

int udp_connect(const char *host, const char *port, H3Conn *c) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  addrinfo *raw = nullptr;
  if (getaddrinfo(host, port, &hints, &raw) != 0) return -1;
  AddrInfoPtr res{raw};                        // freed on every return below
  int fd = -1;
  for (addrinfo *rp = res.get(); rp; rp = rp->ai_next) {
    fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (fd < 0) continue;
    if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
      std::memcpy(&c->remote_ss, rp->ai_addr, rp->ai_addrlen);
      c->path.remote.addr = reinterpret_cast<ngtcp2_sockaddr *>(&c->remote_ss);
      c->path.remote.addrlen = rp->ai_addrlen;
      break;
    }
    ::close(fd);
    fd = -1;
  }
  if (fd < 0) return -1;
  FdGuard sock{fd};                            // closes fd on any early return below
  fcntl(sock.fd, F_SETFL, fcntl(sock.fd, F_GETFL, 0) | O_NONBLOCK);  // async reader
  socklen_t ll = sizeof c->local_ss;
  getsockname(sock.fd, reinterpret_cast<sockaddr *>(&c->local_ss), &ll);
  c->path.local.addr = reinterpret_cast<ngtcp2_sockaddr *>(&c->local_ss);
  c->path.local.addrlen = ll;
  c->path.user_data = nullptr;
  return sock.release();                        // hand the fd to the caller (H3Conn)
}

// Fill buf with the next datagram to send: its length, 0 = nothing, <0 = error.
ngtcp2_ssize send_step(H3Conn *c, std::span<std::uint8_t> buf) {
  // Flush RESET_STREAM for any stream whose body producer raised (read_body_stream
  // marked it). Resets just that stream (STOP_SENDING + RESET_STREAM); on_stream_close
  // then surfaces it to the caller as a reset, leaving the connection and every other
  // multiplexed stream intact.
  if (c->has_abort) {
    for (auto &kv : c->streams)
      if (kv.second.abort && !kv.second.abort_sent) {
        ngtcp2_conn_shutdown_stream(c->conn, 0, kv.first, NGHTTP3_H3_INTERNAL_ERROR);
        kv.second.abort_sent = true;
      }
    c->has_abort = false;
  }
  for (;;) {
    std::int64_t stream_id = -1;
    int fin = 0;
    std::array<nghttp3_vec, 16> vec{};
    nghttp3_ssize sveccnt = 0;
    if (c->h3) {
      sveccnt = nghttp3_conn_writev_stream(c->h3, &stream_id, &fin, vec.data(), vec.size());
      if (sveccnt < 0) {
        set_error(NAVI_H3_ERR_PROTOCOL, "nghttp3 writev_stream: %s",
                  nghttp3_strerror(static_cast<int>(sveccnt)));
        return -1;
      }
    }
    ngtcp2_ssize ndatalen = 0;
    std::uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE;
    if (fin) flags |= NGTCP2_WRITE_STREAM_FLAG_FIN;
    ngtcp2_pkt_info pi;
    // nghttp3_vec and ngtcp2_vec are layout-compatible ({base, len}).
    ngtcp2_ssize wrote = ngtcp2_conn_writev_stream(
        c->conn, &c->path, &pi, buf.data(), buf.size(), &ndatalen, flags, stream_id,
        reinterpret_cast<const ngtcp2_vec *>(vec.data()),
        static_cast<std::size_t>(sveccnt), now_ns());
    if (wrote == NGTCP2_ERR_WRITE_MORE) {
      nghttp3_conn_add_write_offset(c->h3, stream_id, static_cast<std::size_t>(ndatalen));
      continue;
    }
    // The stream can't send right now: its QUIC flow-control window is exhausted
    // (a large streamed upload) or its write side is shut. Tell nghttp3 to stop
    // offering that stream's body until it reopens (via extend_max_stream_data ->
    // unblock); NOT a fatal error, so keep writing other streams / control frames.
    if (wrote == NGTCP2_ERR_STREAM_DATA_BLOCKED) {
      nghttp3_conn_block_stream(c->h3, stream_id);
      continue;
    }
    if (wrote == NGTCP2_ERR_STREAM_SHUT_WR) {
      nghttp3_conn_shutdown_stream_write(c->h3, stream_id);
      continue;
    }
    if (wrote < 0) {
      set_error(NAVI_H3_ERR_NETWORK, "ngtcp2 writev_stream: %s",
                ngtcp2_strerror(static_cast<int>(wrote)));
      return -1;
    }
    if (ndatalen > 0)
      nghttp3_conn_add_write_offset(c->h3, stream_id, static_cast<std::size_t>(ndatalen));
    return wrote;
  }
}

// Blocking driver for the sync wrappers: advance the step functions with a poll
// loop until *flag is set (handshake completed / request done).
int drive_until(H3Conn *c, const bool *flag, unsigned long long budget_ms = 0);
  // fwd decl (uses the extern "C" steps)

// --- TLS configuration (NaviH3Tls -> SSL_CTX) --------------------------------
// The QUIC leg used to receive only a CA file and a verify flag, so caBundle, the
// client credential and the cipher/version bounds were silently dropped on h3 while
// the TCP backends honoured them (#419). These helpers apply the same policy to the
// QUIC SSL_CTX; each one records the reason (set_error) and returns false so
// navi_h3_new fails closed rather than connecting with a weaker configuration.

bool is_set(const char *s) { return s != nullptr && s[0] != '\0'; }

// Passphrase source for encrypted PEM material. OpenSSL's default callback prompts
// on the controlling terminal, which would hang a server process; this one fails
// instead when no password is configured.
int navi_pw_cb(char *buf, int size, int /*rwflag*/, void *u) {
  const char *pw = static_cast<const char *>(u);
  // 0, not -1, when there is no usable password: that is what
  // openssl_ctx.pemPassword returns on the TCP backends, and the two values do not
  // produce the same OpenSSL error. A 0 becomes PEM_R_BAD_PASSWORD_READ, which is
  // what the "set tls.password" wording below is written against; a negative return
  // surfaces as an unrelated generic decode failure, so the same missing passphrase
  // was reported differently depending on which leg loaded the key.
  if (!is_set(pw) || size <= 0) return 0;
  const int n = static_cast<int>(std::strlen(pw));
  if (n > size) return 0;
  std::memcpy(buf, pw, static_cast<std::size_t>(n));
  return n;
}

bool read_whole_file(const char *path, std::string &out) {
  BioPtr bio{BIO_new_file(path, "rb")};
  if (!bio) {
    char eb[256];
    set_error(NAVI_H3_ERR_TLS, "could not read %s: %s", path,
              ossl_error(eb, sizeof eb));
    return false;
  }
  char chunk[8192];
  int n;
  while ((n = BIO_read(bio.get(), chunk, sizeof(chunk))) > 0)
    out.append(chunk, static_cast<std::size_t>(n));
  return true;
}

// Add every certificate in an in-memory PEM string to the context's trust store, so
// a chain anchored at one of them verifies. Supplements caFile / the system roots
// rather than replacing them (same contract as openssl_ctx.addCaBundle).
bool apply_ca_bundle(SSL_CTX *ctx, const char *pem) {
  X509_STORE *store = SSL_CTX_get_cert_store(ctx);
  if (!store) {
    set_error(NAVI_H3_ERR_INTERNAL, "the QUIC SSL_CTX has no certificate store");
    return false;
  }
  BioPtr bio{BIO_new_mem_buf(pem, -1)};
  if (!bio) {
    char eb[256];
    set_error(NAVI_H3_ERR_TLS, "could not read TlsConfig.caBundle: %s",
              ossl_error(eb, sizeof eb));
    return false;
  }
  int added = 0;
  while (true) {
    X509Ptr cert{PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr)};
    if (!cert) break;
    // X509_STORE_add_cert bumps the refcount, so our reference is still ours to free.
    X509_STORE_add_cert(store, cert.get());
    ++added;
  }
  ERR_clear_error();   // the loop always ends on a PEM "no start line" error
  if (added == 0) {
    set_error(NAVI_H3_ERR_TLS, "no certificate found in TlsConfig.caBundle");
    return false;
  }
  return true;
}

// Install the leaf + any chain certificates from a PEM blob (the first certificate
// is the leaf; the rest extend the chain, matching openssl_ctx.useCertChainPem).
bool use_cert_chain_pem(SSL_CTX *ctx, const std::string &pem) {
  BioPtr bio{BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()))};
  if (!bio) return false;
  X509Ptr leaf{PEM_read_bio_X509(bio.get(), nullptr, navi_pw_cb, nullptr)};
  if (!leaf) return false;
  if (SSL_CTX_use_certificate(ctx, leaf.get()) != 1) return false;
  SSL_CTX_clear_chain_certs(ctx);
  while (true) {
    X509Ptr extra{PEM_read_bio_X509(bio.get(), nullptr, navi_pw_cb, nullptr)};
    if (!extra) break;
    if (SSL_CTX_add1_chain_cert(ctx, extra.get()) != 1) return false;
  }
  ERR_clear_error();
  return true;
}

bool use_key_pem(SSL_CTX *ctx, const std::string &pem, const char *password) {
  BioPtr bio{BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()))};
  if (!bio) return false;
  EvpPkeyPtr key{PEM_read_bio_PrivateKey(bio.get(), nullptr, navi_pw_cb,
                                         const_cast<char *>(password))};
  if (!key) return false;
  return SSL_CTX_use_PrivateKey(ctx, key.get()) == 1;
}

// A PKCS#12 bundle: leaf + key + any CA certificates it carries, which become the
// presented chain (PKCS12_parse hands them back in `ca`).
bool use_pkcs12(SSL_CTX *ctx, const std::string &der, const char *password) {
  BioPtr bio{BIO_new_mem_buf(der.data(), static_cast<int>(der.size()))};
  if (!bio) return false;
  Pkcs12Ptr p12{d2i_PKCS12_bio(bio.get(), nullptr)};
  if (!p12) return false;
  EVP_PKEY *rawKey = nullptr;
  X509 *rawCert = nullptr;
  STACK_OF(X509) *rawCa = nullptr;
  if (PKCS12_parse(p12.get(), is_set(password) ? password : "", &rawKey, &rawCert,
                   &rawCa) != 1)
    return false;
  EvpPkeyPtr key{rawKey};
  X509Ptr cert{rawCert};
  X509StackPtr ca{rawCa};
  if (!cert || !key) return false;
  if (SSL_CTX_use_certificate(ctx, cert.get()) != 1) return false;
  if (SSL_CTX_use_PrivateKey(ctx, key.get()) != 1) return false;
  SSL_CTX_clear_chain_certs(ctx);
  for (int i = 0; ca && i < sk_X509_num(ca.get()); ++i)
    if (SSL_CTX_add1_chain_cert(ctx, sk_X509_value(ca.get(), i)) != 1) return false;
  return true;
}

// Install a DER private key from `path`: a traditional RSA/EC key, an unencrypted
// PKCS#8 PrivateKeyInfo, or an encrypted PKCS#8 EncryptedPrivateKeyInfo (from
// `openssl pkcs8 -topk8 -outform DER -v2 aes-256-cbc`). SSL_CTX_use_PrivateKey_file
// with SSL_FILETYPE_ASN1, which this replaces, decodes only the first two: it calls
// d2i_PrivateKey and never consults the passphrase callback, so an encrypted DER key
// failed on h3 with `password` silently ignored, exactly as on the TCP backends
// (#436). Mirrors openssl_ctx.useKeyDer, including the order of the two attempts.
bool use_key_der_file(SSL_CTX *ctx, const char *path, const char *password) {
  EvpPkeyPtr key;
  {
    BioPtr bio{BIO_new_file(path, "rb")};
    if (!bio) return false;
    key.reset(d2i_PrivateKey_bio(bio.get(), nullptr));
  }
  if (!key) {
    // Not an unencrypted key; re-read the file as EncryptedPrivateKeyInfo. The
    // failed attempt left entries on the thread's error queue.
    ERR_clear_error();
    BioPtr bio{BIO_new_file(path, "rb")};
    if (!bio) return false;
    // navi_pw_cb rather than a null callback: OpenSSL's default prompts on the
    // controlling terminal, and ours fails when no password is configured.
    key.reset(d2i_PKCS8PrivateKey_bio(bio.get(), nullptr, navi_pw_cb,
                                      const_cast<char *>(password)));
  }
  if (!key) {
    // The same two messages openssl_ctx.useKeyDer raises, so an encrypted DER key
    // is reported identically whichever leg loaded it: "wrong password?" when one
    // was configured, and the hint to set `tls.password` when none was. The generic
    // "could not load the client certificate/key: <OpenSSL reason>" this used to
    // fall through to named the mechanism (a bad decrypt) and not the fix.
    //
    // The queue is drained first: it holds the second attempt's decrypt failure,
    // which adds nothing to either message and must not be misreported against a
    // later call (the same reason openssl_ctx.useKeyDer clears it here).
    ERR_clear_error();
    if (is_set(password))
      set_error(NAVI_H3_ERR_TLS,
                "could not load the DER private key (wrong password?): %s", path);
    else
      set_error(NAVI_H3_ERR_TLS,
                "could not load the DER private key; set tls.password if it is an "
                "encrypted PKCS#8 key: %s", path);
    return false;
  }
  return SSL_CTX_use_PrivateKey(ctx, key.get()) == 1;
}

// Install the client credential described by `t`. Precedence matches the TCP path
// (openssl_ctx.loadClientCert): PKCS#12, then in-memory PEM, then the file pair.
// Files may be PEM or DER; PEM is tried first and DER is the fallback.
bool apply_client_cert(SSL_CTX *ctx, const NaviH3Tls *t) {
  if (!is_set(t->pkcs12_file) && !is_set(t->cert_pem) && !is_set(t->cert_file))
    return true;   // no credential configured
  SSL_CTX_set_default_passwd_cb(ctx, navi_pw_cb);
  SSL_CTX_set_default_passwd_cb_userdata(ctx, const_cast<char *>(t->password));
  // The userdata is a borrowed pointer into the caller's password string, which is
  // only valid for this call, so it must not outlive it -- the context is cached and
  // reused across connections now (#454). Nothing reads it after the key material is
  // loaded below; dropping it here makes that structural rather than incidental. The
  // callback itself stays installed, since the OpenSSL default it would fall back to
  // prompts on the controlling terminal, while ours fails on an unset password.
  struct PwGuard {
    SSL_CTX *ctx;
    ~PwGuard() { SSL_CTX_set_default_passwd_cb_userdata(ctx, nullptr); }
  } pwGuard{ctx};
  bool ok = false;
  if (is_set(t->pkcs12_file)) {
    std::string der;
    ok = read_whole_file(t->pkcs12_file, der) && use_pkcs12(ctx, der, t->password);
  } else if (is_set(t->cert_pem)) {
    ok = use_cert_chain_pem(ctx, t->cert_pem) &&
         use_key_pem(ctx, is_set(t->key_pem) ? t->key_pem : t->cert_pem, t->password);
  } else {
    const char *keyPath = is_set(t->key_file) ? t->key_file : t->cert_file;
    ok = SSL_CTX_use_certificate_chain_file(ctx, t->cert_file) == 1 ||
         SSL_CTX_use_certificate_file(ctx, t->cert_file, SSL_FILETYPE_ASN1) == 1;
    if (ok)
      ok = SSL_CTX_use_PrivateKey_file(ctx, keyPath, SSL_FILETYPE_PEM) == 1 ||
           use_key_der_file(ctx, keyPath, t->password);
  }
  if (!ok) {
    // Only when no helper recorded something better: use_key_der_file names the
    // file and what to do about it, and the OpenSSL reason behind it is the
    // mechanism, not the cause, so overwriting it threw the useful message away.
    if (g_err_code == NAVI_H3_ERR_NONE) {
      char eb[256];
      set_error(NAVI_H3_ERR_TLS, "could not load the client certificate/key: %s",
                ossl_error(eb, sizeof eb));
    } else {
      ERR_clear_error();   // the recorded reason stands; do not leave the queue dirty
    }
    return false;
  }
  if (SSL_CTX_check_private_key(ctx) != 1) {
    set_error(NAVI_H3_ERR_TLS,
              "the client certificate and private key do not match");
    return false;
  }
  ERR_clear_error();
  return true;
}

// QUIC is TLS 1.3 only (RFC 9001 4.2), so a version bound can only ever confirm or
// exclude 1.3. A maxVersion below 1.3 makes the h3 leg unusable; the Nim side skips
// the h3 endpoint in that case, and this is the backstop for a direct FFI caller.
bool apply_versions(SSL_CTX *ctx, const NaviH3Tls *t) {
  if (t->max_version != 0 && t->max_version < 13) {
    set_error(NAVI_H3_ERR_TLS,
              "HTTP/3 requires TLS 1.3, but TlsConfig.maxVersion is lower");
    return false;
  }
  if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1 ||
      SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1) {
    char eb[256];
    set_error(NAVI_H3_ERR_TLS, "could not pin the QUIC context to TLS 1.3: %s",
              ossl_error(eb, sizeof eb));
    return false;
  }
  return true;
}

bool apply_ciphers(SSL_CTX *ctx, const NaviH3Tls *t) {
  // `ciphers` selects TLS <= 1.2 suites, which QUIC never negotiates; it is still
  // validated and applied so a typo is reported rather than silently ignored.
  if (is_set(t->ciphers) && SSL_CTX_set_cipher_list(ctx, t->ciphers) != 1) {
    set_error(NAVI_H3_ERR_TLS, "no usable cipher in TlsConfig.ciphers");
    return false;
  }
  if (is_set(t->cipher_suites) &&
      SSL_CTX_set_ciphersuites(ctx, t->cipher_suites) != 1) {
    set_error(NAVI_H3_ERR_TLS, "no usable ciphersuite in TlsConfig.cipherSuites");
    return false;
  }
  return true;
}

// Build the QUIC SSL_CTX described by `t`. Everything here depends on the TLS
// policy alone, never on the connection, which is what makes the result shareable;
// the per-connection settings (SNI, the expected peer identity, ALPN) are applied to
// the SSL in navi_h3_new. Returns nullptr with the reason in the last-error slot.
SSL_CTX *build_ssl_ctx(const NaviH3Tls *t) {
  SslCtxPtr ctx{SSL_CTX_new(TLS_method())};
  if (!ctx) {
    char eb[256];
    set_error(NAVI_H3_ERR_TLS, "SSL_CTX_new failed: %s", ossl_error(eb, sizeof eb));
    return nullptr;
  }
  // Verify the server certificate by default (matching navi's TlsConfig.verify),
  // but do it AFTER the handshake (navi_h3_bind), not with SSL_VERIFY_PEER. On a
  // rejected certificate, OpenSSL's in-handshake abort drives ngtcp2's experimental
  // crypto_ossl binding to over-release its crypto buffers, tripping an assert
  // (crypto_ossl_ctx_release_crypto_data). Setting SSL_VERIFY_NONE lets the
  // handshake complete; we then check SSL_get_verify_result and reject before any
  // request is sent -- the same post-handshake pattern navi's TCP backends use
  // (backend/openssl_ctx postHandshakeVerify). The chain is still built and the
  // hostname still matched (SSL_set1_host feeds the verify result); nothing is sent
  // to an unverified peer, since h3Open verifies before returning.
  SSL_CTX_set_verify(ctx.get(), SSL_VERIFY_NONE, nullptr);
  if (t->verify) {
    if (is_set(t->ca_file)) {
      if (SSL_CTX_load_verify_locations(ctx.get(), t->ca_file, nullptr) != 1) {
        char eb[256];
        set_error(NAVI_H3_ERR_TLS, "could not load the CA file %s: %s", t->ca_file,
                  ossl_error(eb, sizeof eb));
        return nullptr;
      }
    } else if (SSL_CTX_set_default_verify_paths(ctx.get()) != 1) {
      // Its result used to be ignored (#446), which would have left the context
      // verifying against an empty trust store: every chain then fails in
      // navi_h3_bind, per connection, with nothing saying why. Fail closed instead.
      char eb[256];
      set_error(NAVI_H3_ERR_TLS, "could not load the system trust store: %s",
                ossl_error(eb, sizeof eb));
      return nullptr;
    }
    // Extra in-memory roots supplement caFile / the system store, exactly as on
    // the TCP backends; only meaningful when a chain is actually being built.
    if (is_set(t->ca_bundle) && !apply_ca_bundle(ctx.get(), t->ca_bundle))
      return nullptr;
  }
  // The client credential, the version bounds and the cipher selection apply
  // whether or not the peer is verified: they describe what navi offers, not what
  // it accepts. Each fails closed rather than connecting under a weaker policy.
  if (!apply_client_cert(ctx.get(), t)) return nullptr;
  if (!apply_versions(ctx.get(), t)) return nullptr;
  if (!apply_ciphers(ctx.get(), t)) return nullptr;
  return ctx.release();
}

// --- the shared SSL_CTX cache (#454) -----------------------------------------
// Building that context is the dominant cost of navi_h3_new: it re-reads the trust
// store, re-parses caBundle and (since #419) re-decodes the client credential --
// and a PKCS#12 decode is deliberately slow. The context is immutable once built
// and OpenSSL refcounts it, so connections share one and only SSL_new stays per
// connection, exactly as the TCP backends share a context through
// TlsConfig.contextStore (backend/openssl_ctx obtainContext). That matters on every
// idle-timeout eviction, cold start per origin and server-forced reconnect.
//
// Key: the owning client (NaviH3Tls.ctx_owner, the address of its context store; 0
// for a bare TlsConfig with no store) plus every field that shapes the context and,
// for the file-based inputs, their size and mtime -- so a rewritten certificate on
// disk is never served from cache. An entry lives until its owner is closed
// (navi_h3_ctx_release) or the LRU bound evicts it; either way a connection built
// from it holds its own reference, so the SSL_CTX outlives every SSL made from it.
//
// Thread safety: one mutex around the table (the sync ws-over-h3 pump runs on its
// own thread and may share a process with the async clients). SSL_CTX itself is
// refcounted and safe for concurrent SSL_new, and the build runs outside the lock.
constexpr std::size_t kCtxCacheMax = 8;   // bound: churning configs cannot grow it

struct CtxCacheEntry {
  std::string key;
  SSL_CTX *ctx = nullptr;             // the cache's own reference
  unsigned long long owner = 0;
  unsigned long long used = 0;        // LRU stamp
};

struct CtxCache {
  std::mutex mu;
  std::vector<CtxCacheEntry> entries;   // at most kCtxCacheMax, so a linear scan
  unsigned long long tick = 0, builds = 0, reuses = 0;
};

// Deliberately never destroyed: freeing an SSL_CTX from a static destructor races
// OpenSSL's own atexit cleanup. The bound keeps what that leaves at exit tiny, and
// the function-local static is initialised thread safely.
CtxCache &ctx_cache() {
  static CtxCache *c = new CtxCache();
  return *c;
}

// NAVI_H3_CTX_CACHE=0 builds a fresh context per connection (the pre-#454
// behaviour), for measurement and as an escape hatch. Read once.
bool ctx_cache_enabled() {
  static const bool on = [] {
    const char *v = std::getenv("NAVI_H3_CTX_CACHE");
    return !(v && v[0] == '0' && v[1] == '\0');
  }();
  return on;
}

void key_add_num(std::string &k, unsigned long long v) {
  k.append(reinterpret_cast<const char *>(&v), sizeof v);
}

// Length-prefixed, so no combination of values can spell another combination.
void key_add(std::string &k, const char *s) {
  const std::size_t n = is_set(s) ? std::strlen(s) : 0;
  key_add_num(k, n);
  if (n > 0) k.append(s, n);
}

// A path plus its identity on disk, so rewriting the file invalidates the entry.
void key_add_file(std::string &k, const char *path) {
  key_add(k, path);
  struct ::stat st {};
  if (is_set(path) && ::stat(path, &st) == 0) {
    key_add_num(k, static_cast<unsigned long long>(st.st_size));
    key_add_num(k, static_cast<unsigned long long>(st.st_mtime));
#if defined(__APPLE__)
    key_add_num(k, static_cast<unsigned long long>(st.st_mtimespec.tv_nsec));
#else
    key_add_num(k, static_cast<unsigned long long>(st.st_mtim.tv_nsec));
#endif
  }
}

// Every input build_ssl_ctx reads, and nothing else: handshake_timeout_ms is a
// per-connection ngtcp2 setting, not part of the context.
std::string ctx_key(const NaviH3Tls *t) {
  std::string k;
  k.reserve(256);
  key_add_num(k, t->ctx_owner);
  key_add_num(k, static_cast<unsigned long long>(t->verify));
  key_add_num(k, static_cast<unsigned long long>(t->min_version));
  key_add_num(k, static_cast<unsigned long long>(t->max_version));
  key_add_file(k, t->ca_file);
  key_add_file(k, t->pkcs12_file);
  key_add_file(k, t->cert_file);
  key_add_file(k, t->key_file);
  key_add(k, t->ca_bundle);
  key_add(k, t->cert_pem);
  key_add(k, t->key_pem);
  key_add(k, t->password);
  key_add(k, t->ciphers);
  key_add(k, t->cipher_suites);
  return k;
}

// Drop the least recently used entry. Caller holds the lock.
void ctx_cache_evict(CtxCache &cc) {
  auto lru = cc.entries.begin();
  for (auto it = cc.entries.begin(); it != cc.entries.end(); ++it)
    if (it->used < lru->used) lru = it;
  SSL_CTX_free(lru->ctx);   // only the cache's reference; live SSLs hold their own
  cc.entries.erase(lru);
}

// The context for `t`, from the cache when one matches. The caller owns the
// returned reference and frees it with SSL_CTX_free (the H3Conn destructor does).
SSL_CTX *obtain_ssl_ctx(const NaviH3Tls *t) {
  if (!ctx_cache_enabled()) {
    SSL_CTX *ctx = build_ssl_ctx(t);
    if (ctx) {
      CtxCache &cc = ctx_cache();
      std::lock_guard<std::mutex> lk(cc.mu);
      ++cc.builds;
    }
    return ctx;
  }
  const std::string key = ctx_key(t);
  CtxCache &cc = ctx_cache();
  {
    std::lock_guard<std::mutex> lk(cc.mu);
    for (auto &e : cc.entries)
      if (e.key == key) {
        e.used = ++cc.tick;
        ++cc.reuses;
        SSL_CTX_up_ref(e.ctx);
        return e.ctx;
      }
  }
  // Built outside the lock: holding the mutex across a PKCS#12 decode would
  // serialise every h3 connect in the process. Two threads racing the same new
  // config both build, and the loser simply drops its context below.
  SSL_CTX *ctx = build_ssl_ctx(t);
  if (!ctx) return nullptr;
  std::lock_guard<std::mutex> lk(cc.mu);
  ++cc.builds;
  for (auto &e : cc.entries)
    if (e.key == key) {              // another thread won the race; keep its entry
      e.used = ++cc.tick;
      ++cc.reuses;
      SSL_CTX_up_ref(e.ctx);
      SSL_CTX_free(ctx);
      return e.ctx;
    }
  if (cc.entries.size() >= kCtxCacheMax) ctx_cache_evict(cc);
  SSL_CTX_up_ref(ctx);               // one reference for the cache, one returned
  cc.entries.push_back(CtxCacheEntry{key, ctx, t->ctx_owner, ++cc.tick});
  return ctx;
}

// An origin written as an IP literal is not a DNS name: RFC 6066 3 forbids sending
// it as SNI, and it must be matched against the certificate's iPAddress SAN rather
// than its dNSName SANs. SSL_set1_host gets the second half right only by accident
// (it tries X509_VERIFY_PARAM_set1_ip_asc and falls back to the DNS-name matcher),
// and that fallback misses a bracketed literal -- [::1], the form a URL authority
// uses for IPv6 -- which is then matched as a DNS name no certificate answers
// (#451). Recognise IPv4 and IPv6 literals here, bracketed or not, and hand back
// the bare address so the caller can bind it explicitly. The h3 twin of the
// isIpAddress split openssl_ctx.nim makes on the TCP backends.
bool ip_literal(const char *s, std::string &out) {
  if (!is_set(s)) return false;
  std::string_view v(s);
  if (v.size() >= 2 && v.front() == '[' && v.back() == ']')
    v = v.substr(1, v.size() - 2);
  if (v.empty()) return false;
  std::string bare(v);
  unsigned char addr[16];
  if (inet_pton(AF_INET, bare.c_str(), addr) != 1 &&
      inet_pton(AF_INET6, bare.c_str(), addr) != 1)
    return false;
  out = std::move(bare);
  return true;
}

nghttp3_nv method_nv(const char *method) {  // :method value is a C string param
  return nghttp3_nv{reinterpret_cast<std::uint8_t *>(const_cast<char *>(":method")),
                    reinterpret_cast<std::uint8_t *>(const_cast<char *>(method)), 7,
                    std::strlen(method), NGHTTP3_NV_FLAG_NONE};
}

}  // namespace

extern "C" {

// Drop every cached SSL_CTX built for `owner` (a client's TLS context store), the
// h3 half of closing that store. A no-op for 0, which is every bare TlsConfig: those
// entries share one owner and are bounded by the LRU instead. Connections still
// using a released context hold their own reference and keep working.
void navi_h3_ctx_release(unsigned long long owner) {
  if (owner == 0) return;
  try {
    CtxCache &cc = ctx_cache();
    std::lock_guard<std::mutex> lk(cc.mu);
    for (auto it = cc.entries.begin(); it != cc.entries.end();) {
      if (it->owner == owner) {
        SSL_CTX_free(it->ctx);
        it = cc.entries.erase(it);
      } else {
        ++it;
      }
    }
  } catch (...) {   // a C++ exception must never unwind into Nim
  }
}

void navi_h3_ctx_cache_stats(unsigned long long *builds, unsigned long long *reuses,
                             unsigned long long *entries) {
  try {
    CtxCache &cc = ctx_cache();
    std::lock_guard<std::mutex> lk(cc.mu);
    if (builds) *builds = cc.builds;
    if (reuses) *reuses = cc.reuses;
    if (entries) *entries = cc.entries.size();
  } catch (...) {
  }
}

// The reason the driver recorded for the most recent failure on THIS thread, and its
// NAVI_H3_ERR_* code (#446). "" / NAVI_H3_ERR_NONE when nothing is recorded. The
// pointer stays valid until the next failure on this thread; the Nim wrappers copy it
// immediately, in the raise that reports the failing call.
const char *navi_h3_last_error(void) { return g_err_msg; }
int navi_h3_last_error_code(void) { return g_err_code; }

int navi_h3_fd(H3Conn *c) { return c->fd; }

int navi_h3_handshake_done(H3Conn *c) {
  return ngtcp2_conn_get_handshake_completed(c->conn);
}

ngtcp2_ssize navi_h3_send(H3Conn *c, std::uint8_t *buf, std::size_t buflen) {
  return send_step(c, {buf, buflen});
}

// Returns 0 on success, 1 if the peer closed the connection gracefully (a clean end,
// not an error), or -1 on a real transport error. Distinguishing the graceful case
// (#278) lets the reader deliver already-completed streams and fail only in-flight ones,
// instead of treating a normal server shutdown as an abnormal transport failure.
int navi_h3_recv(H3Conn *c, const std::uint8_t *pkt, std::size_t len) {
  clear_error();   // see navi_h3_new: never report a stale reason as this one
  ngtcp2_pkt_info pi{};
  int rv = ngtcp2_conn_read_pkt(c->conn, &c->path, &pi, pkt, len, now_ns());
  if (rv == 0) return 0;
  // A received CONNECTION_CLOSE (closing/draining) or a drop-connection signal is the
  // peer ending the connection, not an I/O failure. Flag it and report it distinctly;
  // do not log it as an error.
  if (rv == NGTCP2_ERR_DRAINING || rv == NGTCP2_ERR_CLOSING ||
      rv == NGTCP2_ERR_DROP_CONN) {
    c->draining = true;
    return 1;
  }
  // Only if nothing more specific was recorded while the packet was being handled.
  // read_pkt runs navi's ngtcp2 callbacks, and on_recv_stream_data records a PROTOCOL
  // reason (a peer flooding before the HTTP/3 session is bound, an nghttp3 stream
  // error) before returning NGTCP2_ERR_CALLBACK_FAILURE. Overwriting it with "ngtcp2
  // read_pkt: callback failed" replaced the cause with the mechanism by which it was
  // reported.
  if (g_err_code == NAVI_H3_ERR_NONE)
    set_error(NAVI_H3_ERR_NETWORK, "ngtcp2 read_pkt: %s", ngtcp2_strerror(rv));
  return -1;
}

// 1 once the peer has gracefully closed the connection (see navi_h3_recv).
int navi_h3_draining(H3Conn *c) { return c->draining ? 1 : 0; }

std::uint64_t navi_h3_timeout_ms(H3Conn *c) {
  ngtcp2_tstamp e = ngtcp2_conn_get_expiry(c->conn);
  if (e == UINT64_MAX) return 1000;
  ngtcp2_tstamp t = now_ns();
  return e <= t ? 0 : (e - t) / NGTCP2_MILLISECONDS;
}

int navi_h3_handle_timeout(H3Conn *c) {
  if (ngtcp2_conn_handle_expiry(c->conn, now_ns()) != 0) {
    set_error(NAVI_H3_ERR_NETWORK, "ngtcp2 handle_expiry failed");
    return -1;
  }
  return 0;
}

// One blocking I/O cycle: flush all pending datagrams, then wait (bounded by the
// QUIC timer) for one readable batch or the timer, and service it. The building
// block for the sync blocking loops (handshake, buffered request, and the sync
// streaming reader). Returns 0 on success, -1 on a transport error.
int navi_h3_pump(H3Conn *c) {
  clear_error();   // see navi_h3_new
  std::array<std::uint8_t, 1500> buf{};
  ngtcp2_ssize n;
  while ((n = navi_h3_send(c, buf.data(), buf.size())) > 0)
    if (send(c->fd, buf.data(), static_cast<std::size_t>(n), 0) < 0) {
      set_error(NAVI_H3_ERR_NETWORK, "datagram send failed: %s",
                std::strerror(errno));
      return -1;
    }
  if (n < 0) return -1;
  // Also poll the wake pipe (fd -1 if it failed to open -> poll ignores it) so
  // another thread can interrupt this cycle via navi_h3_wake to flush a send.
  pollfd pfds[2] = {{c->fd, POLLIN, 0}, {c->wake_r.fd, POLLIN, 0}};
  int pr = poll(pfds, 2, static_cast<int>(navi_h3_timeout_ms(c)));
  if (pr > 0) {
    if (pfds[1].revents & POLLIN) {   // wakeup: drain the pipe (it is edge-agnostic)
      std::array<std::uint8_t, 64> wb{};
      while (::read(c->wake_r.fd, wb.data(), wb.size()) > 0) {}
    }
    if (pfds[0].revents & POLLIN)
      for (;;) {   // drain EVERY queued datagram this cycle, not just one: a streamed
        ssize_t r = recv(c->fd, buf.data(), buf.size(), 0);   // upload otherwise advances
        if (r <= 0) break;                                    // one MAX_STREAM_DATA per
        int rc = navi_h3_recv(c, buf.data(), static_cast<std::size_t>(r));  // cycle -> crawls
        if (rc < 0) return -1;
        if (rc > 0) break;   // peer closed gracefully; drive loops observe navi_h3_draining
      }
  }
  if (navi_h3_timeout_ms(c) == 0 && navi_h3_handle_timeout(c) != 0) return -1;
  return 0;
}

// Flush pending outgoing packets without waiting for input -- navi_h3_pump minus the
// poll/recv. Used at teardown to push a stream FIN / CONNECTION_CLOSE promptly
// instead of blocking on the QUIC timer.
int navi_h3_flush(H3Conn *c) {
  clear_error();   // see navi_h3_new
  std::array<std::uint8_t, 1500> buf{};
  ngtcp2_ssize n;
  while ((n = navi_h3_send(c, buf.data(), buf.size())) > 0)
    if (send(c->fd, buf.data(), static_cast<std::size_t>(n), 0) < 0) {
      // Recorded, like navi_h3_pump's identical send: this was the one driver entry
      // point that could fail leaving the last-error slot empty, so a caller's
      // h3Reason had no reason to append.
      set_error(NAVI_H3_ERR_NETWORK, "datagram send failed: %s",
                std::strerror(errno));
      return -1;
    }
  return n < 0 ? -1 : 0;
}

// Wake the pump's poll() from another thread so a just-enqueued outbound frame is
// flushed without waiting for the next QUIC timer. Touches only the pipe, so it is
// safe to call from a thread other than the one driving ngtcp2/nghttp3.
void navi_h3_wake(H3Conn *c) {
  if (c->wake_w.fd < 0) return;
  const std::uint8_t b = 1;
  ssize_t n = ::write(c->wake_w.fd, &b, 1);
  (void)n;   // a full pipe already means a wake is pending; nothing more to do
}

// Create the nghttp3 client session and bind the control + QPACK streams. Called by
// both drivers once the handshake has completed, so it is also where the post-
// handshake certificate verification runs (see navi_h3_new): reject an untrusted or
// mismatched peer here, before any h3 stream is opened.
int navi_h3_bind(H3Conn *c) {
  clear_error();   // see navi_h3_new (#446)
  if (c->want_verify) {
    X509Ptr cert{SSL_get1_peer_certificate(c->ssl)};   // freed on every path below
    if (!cert) {   // a TLS server always sends one; its absence is a failure
      set_error(NAVI_H3_ERR_TLS_VERIFY, "peer presented no certificate");
      return -1;
    }
    long vr = SSL_get_verify_result(c->ssl);   // chain + hostname (SSL_set1_host)
    if (vr != X509_V_OK) {
      set_error(NAVI_H3_ERR_TLS_VERIFY, "certificate verification failed: %s (%ld)",
                X509_verify_cert_error_string(vr), vr);
      return -1;
    }
  }
  nghttp3_settings settings;
  nghttp3_settings_default(&settings);
  settings.enable_connect_protocol = 1;   // allow WebSocket Extended CONNECT (RFC 9220)
  nghttp3_callbacks cb{};
  cb.recv_header = on_recv_header;
  cb.end_headers = on_end_headers;        // headers-ready even for a bodyless 200 (ws tunnel)
  cb.recv_trailer = on_recv_trailer;      // surface response trailers on res.trailers
  cb.recv_data = on_recv_data;
  cb.end_stream = on_end_stream;
  cb.deferred_consume = on_deferred_consume;
  cb.acked_stream_data = on_body_acked;   // free acked streamed-upload chunks
#if defined(NGHTTP3_VERSION_NUM) && NGHTTP3_VERSION_NUM >= 0x010e00
  cb.recv_settings2 = on_recv_settings;   // peer SETTINGS: gates Extended CONNECT
#else
  cb.recv_settings = on_recv_settings;
#endif
  // Each of the three arms below records its reason: they are the rest of the "every
  // failure explains itself" contract, and a bind that returned -1 with an empty slot
  // left the wrappers raising "navi HTTP/3 bind failed" with nothing after it.
  if (nghttp3_conn_client_new(&c->h3, &cb, &settings, nullptr, c) != 0) {
    set_error(NAVI_H3_ERR_INTERNAL, "nghttp3_conn_client_new failed");
    return -1;
  }
  std::int64_t ctrl, qenc, qdec;
  if (ngtcp2_conn_open_uni_stream(c->conn, &ctrl, nullptr) != 0 ||
      ngtcp2_conn_open_uni_stream(c->conn, &qenc, nullptr) != 0 ||
      ngtcp2_conn_open_uni_stream(c->conn, &qdec, nullptr) != 0) {
    set_error(NAVI_H3_ERR_PROTOCOL,
              "the peer did not allow the HTTP/3 control and QPACK streams");
    return -1;
  }
  if (nghttp3_conn_bind_control_stream(c->h3, ctrl) != 0 ||
      nghttp3_conn_bind_qpack_streams(c->h3, qenc, qdec) != 0) {
    set_error(NAVI_H3_ERR_INTERNAL, "could not bind the HTTP/3 control streams");
    return -1;
  }
  // Feed nghttp3 whatever the peer sent before this session existed (see
  // PendingStreamData), in arrival order, and only now extend the flow-control
  // offsets for it.
  for (const auto &p : c->prebind) {
    nghttp3_ssize n = nghttp3_conn_read_stream(
      c->h3, p.id, reinterpret_cast<const std::uint8_t *>(p.data.data()),
      p.data.size(), p.fin ? 1 : 0);
    if (n < 0) {
      set_error(NAVI_H3_ERR_PROTOCOL, "nghttp3 read_stream while binding: %s",
                nghttp3_strerror(static_cast<int>(n)));
      return -1;
    }
    ngtcp2_conn_extend_max_stream_offset(c->conn, p.id,
                                         static_cast<std::uint64_t>(n));
    ngtcp2_conn_extend_max_offset(c->conn, static_cast<std::uint64_t>(n));
  }
  c->prebind.clear();
  c->prebind_bytes = 0;
  return 0;
}

// Create a connection and set up ngtcp2/nghttp3 + TLS, but do NOT drive the
// handshake (no I/O, non-blocking).
H3Conn *navi_h3_new(const char *host, const char *port, const char *sni,
                    const NaviH3Tls *tls, unsigned long long max_body) {
  clear_error();   // so a failure here is never confused with an older one (#446)
  static const NaviH3Tls defaultTls{};   // all-unset: no verification, no credential
  if (!tls) tls = &defaultTls;
  const int verify = tls->verify;
  static bool crypto_inited = false;
  if (!crypto_inited) {
    if (ngtcp2_crypto_ossl_init() != 0) {
      set_error(NAVI_H3_ERR_INTERNAL, "ngtcp2_crypto_ossl_init failed");
      return nullptr;
    }
    crypto_inited = true;
  }
  try {
    // Owned locally so any early return / thrown exception frees it and the C
    // handles it has acquired; ownership is handed to the caller via release().
    auto c = std::make_unique<H3Conn>();
    c->max_body = max_body;
    c->authority = std::string(sni) + ":" + port;
    c->fd = udp_connect(host, port, c.get());
    // Self-pipe for navi_h3_wake (nonblocking, close-on-exec). Best-effort: if it
    // fails the pump still works, a send just waits for the next QUIC timer wakeup.
    int wp[2];
    if (::pipe(wp) == 0) {
      for (int f : wp) {
        ::fcntl(f, F_SETFL, ::fcntl(f, F_GETFL, 0) | O_NONBLOCK);
        ::fcntl(f, F_SETFD, FD_CLOEXEC);
      }
      c->wake_r.fd = wp[0];
      c->wake_w.fd = wp[1];
    }
    if (c->fd < 0) {
      set_error(NAVI_H3_ERR_NETWORK, "could not open a UDP socket to %s:%s: %s",
                host, port, std::strerror(errno));
      return nullptr;
    }

    // The whole TLS policy (trust store, client credential, cipher and version
    // bounds) is built once per policy and shared: this hands back a reference to a
    // cached SSL_CTX, building one only on the first connection for that policy
    // (#454). The reference is the connection's own, released in ~H3Conn.
    c->ssl_ctx = obtain_ssl_ctx(tls);
    if (!c->ssl_ctx) return nullptr;   // the reason is already recorded
    c->want_verify = verify != 0;
    c->ssl = SSL_new(c->ssl_ctx);
    if (!c->ssl) {
      char eb[256];
      set_error(NAVI_H3_ERR_TLS, "SSL_new failed: %s", ossl_error(eb, sizeof eb));
      return nullptr;
    }
    // Bind the identity the peer must prove before the handshake, the way the TCP
    // backends' bindExpectedIdentity does: an IP-literal origin against the
    // certificate's iPAddress SAN, every other host against its dNSName SANs (#451).
    std::string ip_host;
    const bool sni_is_ip = ip_literal(sni, ip_host);
    if (verify) {
      if (sni_is_ip) {
        if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(c->ssl),
                                          ip_host.c_str()) != 1) {
          set_error(NAVI_H3_ERR_TLS,
                    "could not require the certificate to match the IP %s",
                    ip_host.c_str());
          return nullptr;
        }
      } else {
        SSL_set_hostflags(c->ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
        if (SSL_set1_host(c->ssl, sni) != 1) {
          set_error(NAVI_H3_ERR_TLS,
                    "could not require the certificate to match the host %s", sni);
          return nullptr;
        }
      }
    }
    if (ngtcp2_crypto_ossl_ctx_new(&c->ossl, c->ssl) != 0) {
      set_error(NAVI_H3_ERR_INTERNAL, "ngtcp2_crypto_ossl_ctx_new failed");
      return nullptr;
    }
    c->ref.get_conn = get_conn;
    c->ref.user_data = c.get();
    SSL_set_app_data(c->ssl, &c->ref);
    SSL_set_connect_state(c->ssl);
    if (ngtcp2_crypto_ossl_configure_client_session(c->ssl) != 0) {
      set_error(NAVI_H3_ERR_INTERNAL,
                "ngtcp2_crypto_ossl_configure_client_session failed");
      return nullptr;
    }
    SSL_set_alpn_protos(c->ssl, reinterpret_cast<const unsigned char *>("\x02h3"), 3);
    // RFC 6066 3: an IP literal must not be sent as a server_name, so only a real
    // DNS host gets SNI -- same rule as openssl_ctx.newClientSsl (#451).
    if (!sni_is_ip) SSL_set_tlsext_host_name(c->ssl, sni);

    ngtcp2_callbacks cb{};
    cb.client_initial = ngtcp2_crypto_client_initial_cb;
    cb.recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb;
    cb.encrypt = ngtcp2_crypto_encrypt_cb;
    cb.decrypt = ngtcp2_crypto_decrypt_cb;
    cb.hp_mask = ngtcp2_crypto_hp_mask_cb;
    cb.recv_retry = ngtcp2_crypto_recv_retry_cb;
    cb.update_key = ngtcp2_crypto_update_key_cb;
    cb.delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb;
    cb.delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb;
    cb.get_path_challenge_data = ngtcp2_crypto_get_path_challenge_data_cb;
    cb.version_negotiation = ngtcp2_crypto_version_negotiation_cb;
    cb.rand = rand_cb;
    cb.get_new_connection_id = get_new_cid;
    cb.handshake_completed = hs_done;
    cb.recv_stream_data = on_recv_stream_data;
    cb.acked_stream_data_offset = on_acked;
    cb.stream_close = on_stream_close;
    cb.extend_max_stream_data = on_extend_max_stream_data;   // resume a blocked upload

    ngtcp2_settings settings;
    ngtcp2_settings_default(&settings);
    settings.initial_ts = now_ns();
    // Bound the QUIC handshake by navi's connect budget. ngtcp2 defaults this to
    // UINT64_MAX, so a black-holed UDP port used to stall until the 30s idle timer
    // fired no matter what connectMs said (#432); with it set, ngtcp2 fails the
    // connection itself and the drive loops below observe that immediately.
    if (tls->handshake_timeout_ms > 0)
      settings.handshake_timeout =
        tls->handshake_timeout_ms * NGTCP2_MILLISECONDS;
    c->handshake_timeout_ms = tls->handshake_timeout_ms;

    ngtcp2_transport_params params;
    ngtcp2_transport_params_default(&params);
    params.initial_max_streams_uni = 3;
    // Receive windows we advertise to the server. The old 256 KiB per-stream / 1 MiB
    // connection windows forced the server to stop every 256 KiB of a download and
    // wait for our MAX_STREAM_DATA; any hitch extending/flushing that update stalls
    // the transfer, and a strict peer (quinn) parks the stream until its ~30s idle
    // timer. Match the h2 mux's 8 MiB per-stream window so a typical response streams
    // in a single window (no MAX_STREAM_DATA round-trip at all), with a large
    // connection window so many concurrent downloads are not connection-gated. The
    // body is drained incrementally regardless, so this bounds burst size, not the
    // steady-state buffer.
    params.initial_max_stream_data_bidi_local = 8 * 1024 * 1024;
    params.initial_max_stream_data_uni = 1024 * 1024;
    params.initial_max_data = 256 * 1024 * 1024;
    // Advertise our own idle timeout. The effective timeout is the minimum of the two
    // advertised values (RFC 9000 10.1), so this bounds how long a peer keeps state
    // for a connection we walked away from without a CONNECTION_CLOSE (a crash, a
    // lost close datagram, a killed process) instead of leaving that entirely to the
    // server's policy. 30 s is comfortably above the 15 s keep-alive PING below, so a
    // pooled-but-idle connection is kept alive by the PINGs and never trips this.
    params.max_idle_timeout = 30 * NGTCP2_SECONDS;

    ngtcp2_cid dcid, scid;
    dcid.datalen = 16;
    scid.datalen = 16;
    RAND_bytes(dcid.data, 16);
    RAND_bytes(scid.data, 16);

    if (ngtcp2_conn_client_new(&c->conn, &dcid, &scid, &c->path, NGTCP2_PROTO_VER_V1,
                               &cb, &settings, &params, nullptr, c.get()) != 0) {
      set_error(NAVI_H3_ERR_INTERNAL, "ngtcp2_conn_client_new failed");
      return nullptr;
    }
    ngtcp2_conn_set_tls_native_handle(c->conn, c->ossl);
    // Keepalive so the connection survives app-side idle -- essential for a
    // long-lived WebSocket, where the app may not send/receive for a while: ngtcp2
    // emits PINGs at this interval (reset the peer's idle timer), which the pump
    // flushes. Configurable (NAVI_H3_KEEPALIVE_MS) for tests; 15s is a sane default
    // comfortably under typical idle timeouts. 0 disables.
    {
      unsigned long kaMs = 15000UL;              // default
      if (const char *kaEnv = std::getenv("NAVI_H3_KEEPALIVE_MS")) {
        char *end = nullptr;
        unsigned long v = std::strtoul(kaEnv, &end, 10);
        if (end != kaEnv && *end == '\0') kaMs = v;   // fully-parsed; else keep default
      }
      if (kaMs > 86'400'000UL) kaMs = 86'400'000UL;    // clamp to 1 day (no ns overflow)
      if (kaMs > 0)
        ngtcp2_conn_set_keep_alive_timeout(c->conn, kaMs * NGTCP2_MILLISECONDS);
    }
    return c.release();   // ownership passes to the caller (freed via navi_h3_close)
  } catch (...) {
    return nullptr;
  }
}

// Close the connection and free everything it owns. Before freeing, tell the peer we
// are gone: write a CONNECTION_CLOSE (H3_NO_ERROR once h3 is up, transport NO_ERROR
// during the handshake) and send that one datagram. Without it the client just drops
// the UDP socket, so the server has no way to learn the connection is dead and holds
// its per-connection state until its own idle timer fires (~30 s in quic-go/Caddy).
// Under connection churn that is thousands of resident dead connections and gigabytes
// of server RSS. The frame is best-effort and non-blocking: a failed write only costs
// the peer the timeout it would have taken anyway, so nothing here is fatal.
void navi_h3_close(H3Conn *c) {
  if (!c) return;
  if (c->conn && c->fd >= 0 && !c->draining &&
      !ngtcp2_conn_in_closing_period(c->conn) &&
      !ngtcp2_conn_in_draining_period(c->conn)) {
    std::array<std::uint8_t, 1500> buf{};
    ngtcp2_ccerr ccerr;
    ngtcp2_ccerr_default(&ccerr);     // transport NO_ERROR: valid at any stage
    if (c->h3 && ngtcp2_conn_get_handshake_completed(c->conn))
      // An application CONNECTION_CLOSE is only legal once the handshake is done;
      // H3_NO_ERROR is the graceful HTTP/3 shutdown code (RFC 9114 8.1).
      ngtcp2_ccerr_set_application_error(&ccerr, NGHTTP3_H3_NO_ERROR, nullptr, 0);
    ngtcp2_pkt_info pi;
    ngtcp2_ssize n = ngtcp2_conn_write_connection_close(
        c->conn, &c->path, &pi, buf.data(), buf.size(), &ccerr, now_ns());
    if (n > 0) {
      ssize_t sent = send(c->fd, buf.data(), static_cast<std::size_t>(n), 0);
      (void)sent;   // best-effort: the peer falls back to its idle timer
    }
  }
  delete c;
}

// Sync convenience: create, drive the handshake to completion with a blocking
// poll loop, and bind the h3 session. Returns nullptr on failure.
H3Conn *navi_h3_open(const char *host, const char *port, const char *sni,
                     const NaviH3Tls *tls, unsigned long long max_body) {
  H3Conn *c = navi_h3_new(host, port, sni, tls, max_body);
  if (!c) return nullptr;
  // The handshake drive loop is bounded by the caller's connectMs when one is
  // configured, so a black-holed UDP path fails inside the connect budget instead
  // of waiting out the 30s idle timer (#432). navi_h3_bind runs the post-handshake
  // certificate + hostname check before any stream is opened.
  if (drive_until(c, &c->handshake_done, c->handshake_timeout_ms) != 0 ||
      navi_h3_bind(c) != 0) {
    navi_h3_close(c);
    return nullptr;
  }
  return c;
}

// The peer's leaf certificate in DER form, for navi's Nim-side post-handshake
// checks (TlsConfig.verifyCallback). Writes up to `cap` bytes into `out` and
// returns the full DER length, so a caller that got a short buffer can grow and
// retry; -1 when the peer presented no certificate or it could not be encoded.
long navi_h3_peer_cert_der(H3Conn *c, char *out, size_t cap) {
  if (!c || !c->ssl) return -1;
  X509Ptr cert{SSL_get1_peer_certificate(c->ssl)};
  if (!cert) return -1;
  const int n = i2d_X509(cert.get(), nullptr);
  if (n <= 0) return -1;
  if (out && cap >= static_cast<size_t>(n)) {
    unsigned char *p = reinterpret_cast<unsigned char *>(out);
    if (i2d_X509(cert.get(), &p) <= 0) return -1;
  }
  return n;
}

// Base64 SHA-256 of the peer leaf's SubjectPublicKeyInfo: the HPKP pin form navi's
// TlsConfig.pinnedKeys uses (`openssl ... | openssl dgst -sha256 -binary | base64`),
// computed here because the QUIC SSL lives on this side of the FFI. Writes the
// NUL-free pin into `out` and returns its length, or -1 on failure.
long navi_h3_peer_spki_pin(H3Conn *c, char *out, size_t cap) {
  if (!c || !c->ssl || !out) return -1;
  X509Ptr cert{SSL_get1_peer_certificate(c->ssl)};
  if (!cert) return -1;
  EvpPkeyPtr pkey{X509_get_pubkey(cert.get())};
  if (!pkey) return -1;
  const int n = i2d_PUBKEY(pkey.get(), nullptr);
  if (n <= 0) return -1;
  std::string der(static_cast<std::size_t>(n), '\0');
  unsigned char *p = reinterpret_cast<unsigned char *>(der.data());
  if (i2d_PUBKEY(pkey.get(), &p) <= 0) return -1;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int dlen = 0;
  if (EVP_Digest(der.data(), der.size(), digest, &dlen, EVP_sha256(), nullptr) != 1)
    return -1;
  // EVP_EncodeBlock writes 4 bytes per 3 input bytes plus a NUL terminator.
  const std::size_t need = 4 * ((dlen + 2) / 3);
  if (cap < need + 1) return -1;
  const int written = EVP_EncodeBlock(reinterpret_cast<unsigned char *>(out), digest,
                                      static_cast<int>(dlen));
  return written <= 0 ? -1 : written;
}

// Submit one request on the connection (non-blocking): open a bidi stream, queue
// the request, and register the stream. Returns the QUIC stream id (>= 0), or -1
// on error. Many streams may be in flight at once. The caller pumps the step
// functions until navi_h3_stream_done(sid), then reads with navi_h3_take_response.
// req_headers: extra fields as "name\nvalue\n..." or nullptr/"" for none; body may
// be null (borrowed until the stream completes).
// `pull` (with `pull_env`) streams the request body from navi on demand; if null,
// `body`/`body_len` is a buffered body (or none). The two are mutually exclusive.
// Append a "name\nvalue\n..." header blob (navi's h3 wire format for extra request
// headers) to `nva` as nghttp3 pairs. The views point into `blob`, which must
// outlive the submit call (nghttp3 copies the header data during submit).
void append_header_blob(const char *blob, std::vector<nghttp3_nv> &nva) {
  if (!blob) return;
  std::string_view hs{blob};
  std::size_t start = 0;
  std::vector<std::string_view> toks;
  for (std::size_t i = 0; i < hs.size(); ++i)
    if (hs[i] == '\n') { toks.push_back(hs.substr(start, i - start)); start = i + 1; }
  for (std::size_t i = 0; i + 1 < toks.size(); i += 2)
    nva.push_back(make_nv(toks[i], toks[i + 1]));
}

std::int64_t navi_h3_submit(H3Conn *c, const char *method, const char *path_,
                            const char *req_headers, const char *body,
                            std::size_t body_len, NaviBodyPull pull, void *pull_env,
                            const char *req_trailers, int cap_body) {
  try {
    std::int64_t sid;
    if (ngtcp2_conn_open_bidi_stream(c->conn, &sid, nullptr) != 0) return -1;
    Stream &s = c->streams[sid];
    s.is_head = std::strcmp(method, "HEAD") == 0;   // its Content-Length has no body
    s.cap_body = (cap_body != 0);   // buffered request: enforce max_body on the body

    if (pull) { s.pull = pull; s.pull_env = pull_env; }     // streamed body
    else if (body && body_len) s.req_body.assign(body, body_len);  // owned copy
    if (req_trailers && req_trailers[0]) s.req_trailers.assign(req_trailers);

    std::vector<nghttp3_nv> nva;
    nva.reserve(8);
    nva.push_back(method_nv(method));
    nva.push_back(make_nv(":scheme", "https"));
    nva.push_back(make_nv(":authority", c->authority));
    nva.push_back(make_nv(":path", path_));
    append_header_blob(req_headers, nva);

    nghttp3_data_reader dr{};
    const nghttp3_data_reader *drp = nullptr;
    if (s.pull) { dr.read_data = read_body_stream; drp = &dr; }
    // A data reader is needed for a buffered body OR trailers-only (no body): the
    // trailing HEADERS section is emitted from the reader once it signals body EOF.
    else if (!s.req_body.empty() || !s.req_trailers.empty()) {
      dr.read_data = read_body; drp = &dr;
    }
    // nghttp3 copies the header data during submit, so nva may be freed after.
    if (nghttp3_conn_submit_request(c->h3, sid, nva.data(), nva.size(), drp, nullptr) != 0) {
      c->streams.erase(sid);
      return -1;
    }
    return sid;
  } catch (...) {
    return -1;
  }
}

// 1 once the peer's SETTINGS frame has been received (RFC 9114 7.2.4). Until then
// nothing is known about the server's capabilities, so the Extended CONNECT path
// drives the connection until this turns 1 before it decides anything (#393).
int navi_h3_peer_settings_seen(H3Conn *c) { return c->peer_settings ? 1 : 0; }

// 1 if the peer's SETTINGS carried SETTINGS_ENABLE_CONNECT_PROTOCOL (RFC 9220), i.e.
// the server allows the Extended CONNECT method. Meaningful only once
// navi_h3_peer_settings_seen is 1; it reads 0 before that, like a server that never
// enabled it.
int navi_h3_peer_allows_connect(H3Conn *c) { return c->peer_connect_protocol ? 1 : 0; }

// Open a WebSocket-over-h3 tunnel (RFC 9220 Extended CONNECT): a bidi stream whose
// request is :method=CONNECT + :protocol, left open (no END_STREAM) for full-duplex
// DATA. Returns the stream id; the caller waits for :status via
// navi_h3_response_headers, then uses navi_h3_tunnel_send / navi_h3_read_body.
std::int64_t navi_h3_open_connect(H3Conn *c, const char *path_, const char *req_headers,
                                  const char *protocol) {
  clear_error();   // see navi_h3_new
  try {
    std::int64_t sid;
    // Each arm records its reason: quic.nim reports this failure with h3Reason, and a
    // bare -1 left it raising "h3 Extended CONNECT failed" and nothing else.
    if (ngtcp2_conn_open_bidi_stream(c->conn, &sid, nullptr) != 0) {
      set_error(NAVI_H3_ERR_PROTOCOL,
                "the peer's stream limit left no bidirectional stream for CONNECT");
      return -1;
    }
    Stream &s = c->streams[sid];
    s.is_tunnel = true;

    std::vector<nghttp3_nv> nva;
    nva.reserve(8);
    nva.push_back(make_nv(":method", "CONNECT"));
    nva.push_back(make_nv(":protocol", protocol));
    nva.push_back(make_nv(":scheme", "https"));
    nva.push_back(make_nv(":authority", c->authority));
    nva.push_back(make_nv(":path", path_));
    append_header_blob(req_headers, nva);

    nghttp3_data_reader dr{};
    dr.read_data = read_tunnel;
    const int rv = nghttp3_conn_submit_request(c->h3, sid, nva.data(), nva.size(),
                                              &dr, nullptr);
    if (rv != 0) {
      c->streams.erase(sid);
      set_error(NAVI_H3_ERR_PROTOCOL, "nghttp3 submit_request (CONNECT): %s",
                nghttp3_strerror(rv));
      return -1;
    }
    return sid;
  } catch (...) {
    set_error(NAVI_H3_ERR_INTERNAL, "out of memory opening the CONNECT stream");
    return -1;
  }
}

// Queue `len` bytes of outbound tunnel DATA and resume the stream so nghttp3 pulls
// it on the next send cycle (navi pumps the socket afterwards). Returns 0, or -1 on
// an unknown stream / allocation failure.
int navi_h3_tunnel_send(H3Conn *c, std::int64_t sid, const char *data, std::size_t len) {
  auto it = c->streams.find(sid);
  if (it == c->streams.end()) return -1;
  try {
    it->second.tunnel_tx.emplace_back(data, len);
  } catch (...) {
    return -1;
  }
  nghttp3_conn_resume_stream(c->h3, sid);
  return 0;
}

// Half-close the tunnel send side: the reader flushes EOF once the outbound queue
// drains. Returns 0, or -1 on an unknown stream.
int navi_h3_tunnel_close(H3Conn *c, std::int64_t sid) {
  auto it = c->streams.find(sid);
  if (it == c->streams.end()) return -1;
  it->second.tunnel_fin = true;
  nghttp3_conn_resume_stream(c->h3, sid);
  return 0;
}

int navi_h3_stream_done(H3Conn *c, std::int64_t sid) {
  auto it = c->streams.find(sid);
  return (it != c->streams.end() && it->second.done) ? 1 : 0;
}

// 1 if stream `sid` finished by a reset/abort rather than a normal response, else
// 0. Valid once navi_h3_stream_done(sid) is true and before take_response.
int navi_h3_stream_reset(H3Conn *c, std::int64_t sid) {
  auto it = c->streams.find(sid);
  return (it != c->streams.end() && it->second.reset) ? 1 : 0;
}

// 1 if stream `sid` ended cleanly but its body length disagreed with a declared
// Content-Length (malformed, RFC 9114 4.1.2). Valid once done and before free.
int navi_h3_stream_length_mismatch(H3Conn *c, std::int64_t sid) {
  auto it = c->streams.find(sid);
  return (it != c->streams.end() && it->second.length_mismatch) ? 1 : 0;
}

// 1 if stream `sid`'s response body exceeded the connection's max_body cap (navi
// maxResponseBytes). The driver stopped buffering; navi raises ResponseTooLargeError.
int navi_h3_stream_too_large(H3Conn *c, std::int64_t sid) {
  auto it = c->streams.find(sid);
  return (it != c->streams.end() && it->second.too_large) ? 1 : 0;
}

// Copy stream `sid`'s completed response into the caller's buffers and drop it.
// out_trailers receives the trailing fields ("name\nvalue\n"), empty if none.
int navi_h3_take_response(H3Conn *c, std::int64_t sid, long *out_status, char *out_body,
                          std::size_t out_cap, std::size_t *out_len,
                          char *out_headers, std::size_t hdr_cap, std::size_t *hdr_len,
                          char *out_trailers, std::size_t trl_cap, std::size_t *trl_len) {
  try {
    auto it = c->streams.find(sid);
    if (it == c->streams.end()) return -1;
    Stream &s = it->second;
    *out_status = s.status;
    // Report the TRUE sizes so a caller whose fixed buffers are too small can grow and
    // retry -- never silently truncate a response (#275, #276). If ANY buffer is too
    // small, copy nothing and keep the stream alive for the retry; only erase (consume)
    // once everything fits.
    *out_len = s.body.size();
    *hdr_len = s.resp_headers.size();
    *trl_len = s.resp_trailers.size();
    if (s.body.size() > out_cap || s.resp_headers.size() > hdr_cap ||
        s.resp_trailers.size() > trl_cap)
      return 0;
    std::memcpy(out_body, s.body.data(), s.body.size());
    std::memcpy(out_headers, s.resp_headers.data(), s.resp_headers.size());
    std::memcpy(out_trailers, s.resp_trailers.data(), s.resp_trailers.size());
    c->streams.erase(it);
    return 0;
  } catch (...) {
    return -1;
  }
}

// Copy stream `sid`'s response trailers into the caller's buffer WITHOUT dropping the
// stream (the streaming read path: trailers land after the body EOF, before free).
int navi_h3_response_trailers(H3Conn *c, std::int64_t sid, char *out_trailers,
                              std::size_t trl_cap, std::size_t *trl_len) {
  try {
    auto it = c->streams.find(sid);
    if (it == c->streams.end()) return -1;
    // Report the true size; if the buffer is too small, copy nothing so the caller can
    // grow and retry rather than get a truncated, parser-desyncing field block (#276).
    *trl_len = it->second.resp_trailers.size();
    if (it->second.resp_trailers.size() > trl_cap) return 0;
    std::memcpy(out_trailers, it->second.resp_trailers.data(),
                it->second.resp_trailers.size());
    return 0;
  } catch (...) {
    return -1;
  }
}

// --- streaming read path (for stream()/SSE) --------------------------------
// The buffered take_response reads the whole body at once and drops the stream.
// These let navi read a response incrementally: headers first, then body chunks
// as they arrive, keeping the stream alive until it is fully drained.

// If stream `sid`'s response headers are all in, copy status + headers into the
// caller's buffers WITHOUT dropping the stream, and set *out_ready = 1. Otherwise
// set *out_ready = 0 and touch nothing else. Returns 0 on success, -1 if the
// stream is unknown.
int navi_h3_response_headers(H3Conn *c, std::int64_t sid, long *out_status,
                            char *out_headers, std::size_t hdr_cap,
                            std::size_t *hdr_len, int *out_ready) {
  try {
    auto it = c->streams.find(sid);
    if (it == c->streams.end()) return -1;
    Stream &s = it->second;
    if (!s.headers_done) { *out_ready = 0; return 0; }
    *out_status = s.status;
    *out_ready = 1;
    // Report the true size; if the buffer is too small, copy nothing so the caller can
    // grow and retry rather than get a truncated, parser-desyncing header block (#276).
    *hdr_len = s.resp_headers.size();
    if (s.resp_headers.size() > hdr_cap) return 0;
    std::memcpy(out_headers, s.resp_headers.data(), s.resp_headers.size());
    return 0;
  } catch (...) {
    return -1;
  }
}

// Drain up to `cap` body bytes of stream `sid` into `buf`, removing them from the
// stream's buffer. Sets *out_eof = 1 once the stream has ended and its buffer is
// drained. Returns bytes copied (0 with *out_eof == 0 means "nothing yet, more
// coming"), or -1 if the stream is unknown. The stream is NOT dropped on EOF, so
// the caller can still check navi_h3_stream_reset (clean end vs abort); call
// navi_h3_stream_free once done.
ngtcp2_ssize navi_h3_read_body(H3Conn *c, std::int64_t sid, char *buf,
                               std::size_t cap, int *out_eof) {
  try {
    auto it = c->streams.find(sid);
    if (it == c->streams.end()) return -1;
    Stream &s = it->second;
    std::size_t k = std::min(s.body.size(), cap);
    if (k > 0) {
      std::memcpy(buf, s.body.data(), k);
      s.body.erase(0, k);
      // Return the per-stream flow-control credit deferred in on_recv_data as the app
      // consumes the bytes, so a slow reader backpressures the peer instead of letting
      // it fill memory. The connection window was already credited on receipt.
      if (c->conn) ngtcp2_conn_extend_max_stream_offset(c->conn, sid, k);
    }
    // A body that overran max_body (cap_body streams) is reported as end-of-body so the
    // navi side raises ResponseTooLargeError immediately rather than waiting for the
    // peer's END_STREAM (it checks navi_h3_stream_too_large at EOF).
    *out_eof = ((s.done && s.body.empty()) || s.too_large) ? 1 : 0;
    return static_cast<ngtcp2_ssize>(k);
  } catch (...) {
    return -1;
  }
}

// Drop stream `sid` without fully reading it (an abandoned streaming handle).
// Tells the peer to stop sending and abandons our send side (STOP_SENDING +
// RESET_STREAM, flushed by the reader's next send cycle), so an abandoned SSE
// stream doesn't leave the server streaming into a dropped buffer. Idempotent.
void navi_h3_stream_free(H3Conn *c, std::int64_t sid) {
  if (c->conn && c->streams.find(sid) != c->streams.end())
    ngtcp2_conn_shutdown_stream(c->conn, 0, sid, 0);
  c->streams.erase(sid);
}

// (The old all-in-one navi_h3_request was removed: the sync driver now submits, drives
// with a pump loop, and reads via the size-safe navi_h3_take_response grow/retry path,
// matching the async backend -- so a large buffered response is never truncated.)

}  // extern "C"

namespace {
// Drive the blocking loops (handshake, buffered request, streamed upload) until
// `*flag`. Bounded by wall-clock, not an iteration count: a large streamed upload
// legitimately needs many cycles, but a genuinely stuck connection must still fail
// rather than hang forever. `budget_ms` is the caller's own bound (the handshake
// uses navi's connectMs, #432); 0 falls back to a generous 120s safety net.
int drive_until(H3Conn *c, const bool *flag, unsigned long long budget_ms) {
  const std::uint64_t start = now_ns();
  const std::uint64_t budget = budget_ms > 0
    ? budget_ms * NGTCP2_MILLISECONDS : 120ULL * NGTCP2_SECONDS;
  while (!*flag) {
    if (navi_h3_pump(c) != 0) return -1;
    if (now_ns() - start > budget) {
      // Recorded so the caller's message says what happened: the last pump cycle
      // succeeded and therefore left the slot empty, so a black-holed UDP path used
      // to surface as a bare "navi HTTP/3 connect failed" with no reason at all.
      set_error(NAVI_H3_ERR_NETWORK,
                "timed out after %llu ms waiting for the HTTP/3 connection to make "
                "progress",
                static_cast<unsigned long long>(budget / NGTCP2_MILLISECONDS));
      return -1;
    }
  }
  return 0;
}
}  // namespace
