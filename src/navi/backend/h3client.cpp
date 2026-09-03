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

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <memory>
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

// RAII wrappers for the C resources this file manages by hand, so an acquire is freed
// on every path (including early returns and future edits) without a manual free.
struct X509Deleter { void operator()(X509 *p) const noexcept { X509_free(p); } };
using X509Ptr = std::unique_ptr<X509, X509Deleter>;

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

// One QUIC/h3 connection. RAII: the destructor releases the library objects and
// the socket in the required order, replacing the old manual cleanup + goto.
struct H3Conn {
  ngtcp2_crypto_conn_ref ref{};
  ngtcp2_conn *conn = nullptr;
  nghttp3_conn *h3 = nullptr;
  int fd = -1;
  ngtcp2_path path{};
  sockaddr_storage local_ss{}, remote_ss{};
  SSL_CTX *ssl_ctx = nullptr;
  SSL *ssl = nullptr;
  ngtcp2_crypto_ossl_ctx *ossl = nullptr;
  std::string authority;
  bool handshake_done = false;
  bool want_verify = false; // verify the peer certificate after the handshake (see below)
  bool has_abort = false;   // some stream's producer failed; reset it in send_step
  std::unordered_map<int64_t, Stream> streams;   // live streams by id
  // Self-pipe (RAII: closed with the connection) so another thread can interrupt the
  // pump's poll() to flush an outbound frame promptly -- the sync WebSocket's pump
  // thread. navi_h3_wake writes the write end; navi_h3_pump polls the read end.
  FdGuard wake_r, wake_w;

  // Teardown order is deliberate and load-bearing (which is why this is an explicit
  // destructor rather than per-member smart pointers whose order would follow
  // declaration order): each handle is released before the thing it depends on --
  // `conn` references `ossl` (ngtcp2_conn_set_tls_native_handle), `ossl` wraps `ssl`,
  // and `ssl` belongs to `ssl_ctx`. It also relies on the library `_del` functions not
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
  if (!c->h3) return 0;
  int fin = (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0;
  nghttp3_ssize n = nghttp3_conn_read_stream(c->h3, stream_id, data, datalen, fin);
  if (n < 0) return NGTCP2_ERR_CALLBACK_FAILURE;
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
      it->second.status = std::strtol(std::string(val).c_str(), nullptr, 10);
    } else if (!nm.empty() && nm.front() != ':') {  // a regular response field
      it->second.resp_headers.append(nm).append("\n").append(val).append("\n");
      if (nm == "content-length") {   // remember it for the end-of-stream length check
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
    it->second.headers_done = true;  // nghttp3 delivers all headers before any body
    it->second.body.append(reinterpret_cast<const char *>(data), datalen);
    it->second.body_total += datalen;   // total received (body is drained on streaming)
  } catch (...) {
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  // nghttp3_conn_read_stream's return (extended in on_recv_stream_data) does NOT
  // count the DATA payload delivered here, so credit these bytes back to QUIC flow
  // control now that we have buffered them. Without this the connection-level
  // receive window (initial_max_data) leaks ~body-size per stream and wedges after
  // ~1 MiB cumulative across a long-lived connection's streams (e.g. SSE reconnects).
  ngtcp2_conn_extend_max_stream_offset(c->conn, stream_id, datalen);
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
        std::fprintf(stderr, "nghttp3 writev: %s\n", nghttp3_strerror(static_cast<int>(sveccnt)));
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
      std::fprintf(stderr, "writev_stream: %s\n", ngtcp2_strerror(static_cast<int>(wrote)));
      return -1;
    }
    if (ndatalen > 0)
      nghttp3_conn_add_write_offset(c->h3, stream_id, static_cast<std::size_t>(ndatalen));
    return wrote;
  }
}

// Blocking driver for the sync wrappers: advance the step functions with a poll
// loop until *flag is set (handshake completed / request done).
int drive_until(H3Conn *c, const bool *flag);  // fwd decl (uses the extern "C" steps)

nghttp3_nv method_nv(const char *method) {  // :method value is a C string param
  return nghttp3_nv{reinterpret_cast<std::uint8_t *>(const_cast<char *>(":method")),
                    reinterpret_cast<std::uint8_t *>(const_cast<char *>(method)), 7,
                    std::strlen(method), NGHTTP3_NV_FLAG_NONE};
}

}  // namespace

extern "C" {

int navi_h3_fd(H3Conn *c) { return c->fd; }

int navi_h3_handshake_done(H3Conn *c) {
  return ngtcp2_conn_get_handshake_completed(c->conn);
}

ngtcp2_ssize navi_h3_send(H3Conn *c, std::uint8_t *buf, std::size_t buflen) {
  return send_step(c, {buf, buflen});
}

int navi_h3_recv(H3Conn *c, const std::uint8_t *pkt, std::size_t len) {
  ngtcp2_pkt_info pi{};
  int rv = ngtcp2_conn_read_pkt(c->conn, &c->path, &pi, pkt, len, now_ns());
  if (rv != 0) std::fprintf(stderr, "read_pkt: %s\n", ngtcp2_strerror(rv));
  return rv;
}

std::uint64_t navi_h3_timeout_ms(H3Conn *c) {
  ngtcp2_tstamp e = ngtcp2_conn_get_expiry(c->conn);
  if (e == UINT64_MAX) return 1000;
  ngtcp2_tstamp t = now_ns();
  return e <= t ? 0 : (e - t) / NGTCP2_MILLISECONDS;
}

int navi_h3_handle_timeout(H3Conn *c) {
  if (ngtcp2_conn_handle_expiry(c->conn, now_ns()) != 0) {
    std::fprintf(stderr, "handle_expiry failed\n");
    return -1;
  }
  return 0;
}

// One blocking I/O cycle: flush all pending datagrams, then wait (bounded by the
// QUIC timer) for one readable batch or the timer, and service it. The building
// block for the sync blocking loops (handshake, buffered request, and the sync
// streaming reader). Returns 0 on success, -1 on a transport error.
int navi_h3_pump(H3Conn *c) {
  std::array<std::uint8_t, 1500> buf{};
  ngtcp2_ssize n;
  while ((n = navi_h3_send(c, buf.data(), buf.size())) > 0)
    if (send(c->fd, buf.data(), static_cast<std::size_t>(n), 0) < 0) {
      std::perror("send");
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
        if (navi_h3_recv(c, buf.data(), static_cast<std::size_t>(r)) != 0)  // cycle -> crawls
          return -1;
      }
  }
  if (navi_h3_timeout_ms(c) == 0 && navi_h3_handle_timeout(c) != 0) return -1;
  return 0;
}

// Flush pending outgoing packets without waiting for input -- navi_h3_pump minus the
// poll/recv. Used at teardown to push a stream FIN / CONNECTION_CLOSE promptly
// instead of blocking on the QUIC timer.
int navi_h3_flush(H3Conn *c) {
  std::array<std::uint8_t, 1500> buf{};
  ngtcp2_ssize n;
  while ((n = navi_h3_send(c, buf.data(), buf.size())) > 0)
    if (send(c->fd, buf.data(), static_cast<std::size_t>(n), 0) < 0) return -1;
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
  if (c->want_verify) {
    X509Ptr cert{SSL_get1_peer_certificate(c->ssl)};   // freed on every path below
    if (!cert) {   // a TLS server always sends one; its absence is a failure
      std::fprintf(stderr, "h3 verify: peer presented no certificate\n");
      return -1;
    }
    long vr = SSL_get_verify_result(c->ssl);   // chain + hostname (SSL_set1_host)
    if (vr != X509_V_OK) {
      std::fprintf(stderr, "h3 verify: certificate verification failed (%ld)\n", vr);
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
  if (nghttp3_conn_client_new(&c->h3, &cb, &settings, nullptr, c) != 0) return -1;
  std::int64_t ctrl, qenc, qdec;
  if (ngtcp2_conn_open_uni_stream(c->conn, &ctrl, nullptr) != 0 ||
      ngtcp2_conn_open_uni_stream(c->conn, &qenc, nullptr) != 0 ||
      ngtcp2_conn_open_uni_stream(c->conn, &qdec, nullptr) != 0)
    return -1;
  if (nghttp3_conn_bind_control_stream(c->h3, ctrl) != 0 ||
      nghttp3_conn_bind_qpack_streams(c->h3, qenc, qdec) != 0)
    return -1;
  return 0;
}

// Create a connection and set up ngtcp2/nghttp3 + TLS, but do NOT drive the
// handshake (no I/O, non-blocking).
H3Conn *navi_h3_new(const char *host, const char *port, const char *sni,
                    const char *ca_file, int verify) {
  static bool crypto_inited = false;
  if (!crypto_inited) {
    if (ngtcp2_crypto_ossl_init() != 0) {
      std::fprintf(stderr, "ngtcp2_crypto_ossl_init failed\n");
      return nullptr;
    }
    crypto_inited = true;
  }
  try {
    // Owned locally so any early return / thrown exception frees it and the C
    // handles it has acquired; ownership is handed to the caller via release().
    auto c = std::make_unique<H3Conn>();
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
      std::fprintf(stderr, "udp connect failed\n");
      return nullptr;
    }

    c->ssl_ctx = SSL_CTX_new(TLS_method());
    if (!c->ssl_ctx) {
      std::fprintf(stderr, "SSL_CTX_new failed\n");
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
    SSL_CTX_set_verify(c->ssl_ctx, SSL_VERIFY_NONE, nullptr);
    c->want_verify = verify != 0;
    if (verify) {
      if (ca_file && ca_file[0]) {
        if (SSL_CTX_load_verify_locations(c->ssl_ctx, ca_file, nullptr) != 1) {
          std::fprintf(stderr, "failed to load CA file %s\n", ca_file);
          return nullptr;
        }
      } else {
        SSL_CTX_set_default_verify_paths(c->ssl_ctx);
      }
    }
    c->ssl = SSL_new(c->ssl_ctx);
    if (!c->ssl) {
      std::fprintf(stderr, "SSL_new failed\n");
      return nullptr;
    }
    if (verify) {
      SSL_set_hostflags(c->ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
      if (SSL_set1_host(c->ssl, sni) != 1) {
        std::fprintf(stderr, "SSL_set1_host failed\n");
        return nullptr;
      }
    }
    if (ngtcp2_crypto_ossl_ctx_new(&c->ossl, c->ssl) != 0) {
      std::fprintf(stderr, "ossl_ctx_new failed\n");
      return nullptr;
    }
    c->ref.get_conn = get_conn;
    c->ref.user_data = c.get();
    SSL_set_app_data(c->ssl, &c->ref);
    SSL_set_connect_state(c->ssl);
    if (ngtcp2_crypto_ossl_configure_client_session(c->ssl) != 0) {
      std::fprintf(stderr, "configure_client_session failed\n");
      return nullptr;
    }
    SSL_set_alpn_protos(c->ssl, reinterpret_cast<const unsigned char *>("\x02h3"), 3);
    SSL_set_tlsext_host_name(c->ssl, sni);

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

    ngtcp2_cid dcid, scid;
    dcid.datalen = 16;
    scid.datalen = 16;
    RAND_bytes(dcid.data, 16);
    RAND_bytes(scid.data, 16);

    if (ngtcp2_conn_client_new(&c->conn, &dcid, &scid, &c->path, NGTCP2_PROTO_VER_V1,
                               &cb, &settings, &params, nullptr, c.get()) != 0) {
      std::fprintf(stderr, "ngtcp2_conn_client_new failed\n");
      return nullptr;
    }
    ngtcp2_conn_set_tls_native_handle(c->conn, c->ossl);
    // Keepalive so the connection survives app-side idle -- essential for a
    // long-lived WebSocket, where the app may not send/receive for a while: ngtcp2
    // emits PINGs at this interval (reset the peer's idle timer), which the pump
    // flushes. Configurable (NAVI_H3_KEEPALIVE_MS) for tests; 15s is a sane default
    // comfortably under typical idle timeouts. 0 disables.
    {
      const char *kaEnv = std::getenv("NAVI_H3_KEEPALIVE_MS");
      unsigned long kaMs = kaEnv ? std::strtoul(kaEnv, nullptr, 10) : 15000UL;
      if (kaMs > 0)
        ngtcp2_conn_set_keep_alive_timeout(c->conn, kaMs * NGTCP2_MILLISECONDS);
    }
    return c.release();   // ownership passes to the caller (freed via navi_h3_close)
  } catch (...) {
    return nullptr;
  }
}

void navi_h3_close(H3Conn *c) { delete c; }

// Sync convenience: create, drive the handshake to completion with a blocking
// poll loop, and bind the h3 session. Returns nullptr on failure.
H3Conn *navi_h3_open(const char *host, const char *port, const char *sni,
                     const char *ca_file, int verify) {
  H3Conn *c = navi_h3_new(host, port, sni, ca_file, verify);
  if (!c) return nullptr;
  if (drive_until(c, &c->handshake_done) != 0 || navi_h3_bind(c) != 0) {
    navi_h3_close(c);
    return nullptr;
  }
  return c;
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
                            const char *req_trailers) {
  try {
    std::int64_t sid;
    if (ngtcp2_conn_open_bidi_stream(c->conn, &sid, nullptr) != 0) return -1;
    Stream &s = c->streams[sid];
    s.is_head = std::strcmp(method, "HEAD") == 0;   // its Content-Length has no body
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

// Open a WebSocket-over-h3 tunnel (RFC 9220 Extended CONNECT): a bidi stream whose
// request is :method=CONNECT + :protocol, left open (no END_STREAM) for full-duplex
// DATA. Returns the stream id; the caller waits for :status via
// navi_h3_response_headers, then uses navi_h3_tunnel_send / navi_h3_read_body.
std::int64_t navi_h3_open_connect(H3Conn *c, const char *path_, const char *req_headers,
                                  const char *protocol) {
  try {
    std::int64_t sid;
    if (ngtcp2_conn_open_bidi_stream(c->conn, &sid, nullptr) != 0) return -1;
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
    if (nghttp3_conn_submit_request(c->h3, sid, nva.data(), nva.size(), &dr, nullptr) != 0) {
      c->streams.erase(sid);
      return -1;
    }
    return sid;
  } catch (...) {
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
    std::size_t k = std::min(s.body.size(), out_cap);
    std::memcpy(out_body, s.body.data(), k);
    *out_len = k;
    std::size_t hk = std::min(s.resp_headers.size(), hdr_cap);
    std::memcpy(out_headers, s.resp_headers.data(), hk);
    *hdr_len = hk;
    std::size_t tk = std::min(s.resp_trailers.size(), trl_cap);
    std::memcpy(out_trailers, s.resp_trailers.data(), tk);
    *trl_len = tk;
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
    std::size_t tk = std::min(it->second.resp_trailers.size(), trl_cap);
    std::memcpy(out_trailers, it->second.resp_trailers.data(), tk);
    *trl_len = tk;
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
    std::size_t hk = std::min(s.resp_headers.size(), hdr_cap);
    std::memcpy(out_headers, s.resp_headers.data(), hk);
    *hdr_len = hk;
    *out_ready = 1;
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
    }
    *out_eof = (s.done && s.body.empty()) ? 1 : 0;
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

// Sync convenience: submit, drive to completion with a blocking poll loop, and
// take the response.
int navi_h3_request(H3Conn *c, const char *method, const char *path_,
                    const char *req_headers, const char *body, std::size_t body_len,
                    NaviBodyPull pull, void *pull_env, const char *req_trailers,
                    long *out_status, char *out_body,
                    std::size_t out_cap, std::size_t *out_len, char *out_headers,
                    std::size_t hdr_cap, std::size_t *hdr_len,
                    char *out_trailers, std::size_t trl_cap, std::size_t *trl_len) {
  std::int64_t sid = navi_h3_submit(c, method, path_, req_headers, body, body_len, pull,
                                    pull_env, req_trailers);
  if (sid < 0) return -1;
  // Look the stream up with find(), not at(): this is an extern "C" boundary with no
  // try/catch, so an at() miss would throw std::out_of_range into Nim-generated C.
  // (References into an unordered_map stay valid across inserts, so &done survives the
  // drive_until pump loop -- only erasing the element would invalidate it.)
  auto it = c->streams.find(sid);
  if (it == c->streams.end()) return -1;
  if (drive_until(c, &it->second.done) != 0) return -1;
  if (navi_h3_stream_reset(c, sid)) {   // reset/abort, not a real response
    c->streams.erase(sid);
    return -1;
  }
  if (navi_h3_stream_length_mismatch(c, sid)) {   // body != declared Content-Length
    c->streams.erase(sid);
    return -2;                                    // distinct: caller raises, no fallback
  }
  return navi_h3_take_response(c, sid, out_status, out_body, out_cap, out_len,
                               out_headers, hdr_cap, hdr_len,
                               out_trailers, trl_cap, trl_len);
}

}  // extern "C"

namespace {
// Drive the blocking loops (handshake, buffered request, streamed upload) until
// `*flag`. Bounded by wall-clock, not an iteration count: a large streamed upload
// legitimately needs many cycles, but a genuinely stuck connection must still fail
// rather than hang forever. 120s is a generous safety net (higher layers enforce
// navi's own timeouts).
int drive_until(H3Conn *c, const bool *flag) {
  std::uint64_t start = now_ns();
  while (!*flag) {
    if (navi_h3_pump(c) != 0) return -1;
    if (now_ns() - start > 120ULL * NGTCP2_SECONDS) return -1;
  }
  return 0;
}
}  // namespace
