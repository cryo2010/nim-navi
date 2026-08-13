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
// Scope: blocking sync wrappers over a non-blocking step-function core (the
// asyncdispatch backend drives the same core from its event loop). One request
// in flight at a time; cert + hostname verified by default. TODO: async/mux and
// streaming bodies.
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>
#include <ngtcp2/ngtcp2_crypto_ossl.h>
#include <nghttp3/nghttp3.h>
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/x509v3.h>

#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

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
  // per-request state (one request in flight at a time)
  int64_t req_stream = -1;
  bool req_done = false;
  long status = 0;
  std::string body;
  std::string resp_headers;                 // response fields as "name\nvalue\n"
  std::span<const std::uint8_t> req_body;    // borrowed for the in-flight request

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

int on_stream_close(ngtcp2_conn *, std::uint32_t, std::int64_t stream_id,
                    std::uint64_t app_error_code, void *ud, void *) {
  auto *c = static_cast<H3Conn *>(ud);
  if (c->h3) nghttp3_conn_close_stream(c->h3, stream_id, app_error_code);
  return 0;
}

int on_recv_header(nghttp3_conn *, std::int64_t, std::int32_t, nghttp3_rcbuf *name,
                   nghttp3_rcbuf *value, std::uint8_t, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name);
  nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
  std::string_view nm{reinterpret_cast<char *>(n.base), n.len};
  std::string_view val{reinterpret_cast<char *>(v.base), v.len};
  try {
    if (nm == ":status") {
      c->status = std::strtol(std::string(val).c_str(), nullptr, 10);
    } else if (!nm.empty() && nm.front() != ':') {  // a regular response field
      c->resp_headers.append(nm).append("\n").append(val).append("\n");
    }
  } catch (...) {
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  return 0;
}

int on_recv_data(nghttp3_conn *, std::int64_t, const std::uint8_t *data,
                 std::size_t datalen, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  try {
    c->body.append(reinterpret_cast<const char *>(data), datalen);
  } catch (...) {
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  return 0;
}

int on_end_stream(nghttp3_conn *, std::int64_t, void *cud, void *) {
  static_cast<H3Conn *>(cud)->req_done = true;
  return 0;
}

// Hand nghttp3 the whole buffered request body in one vec, with EOF, on the
// first call. The body is borrowed from the caller for the request's duration.
nghttp3_ssize read_body(nghttp3_conn *, std::int64_t, nghttp3_vec *vec, std::size_t,
                        std::uint32_t *pflags, void *cud, void *) {
  auto *c = static_cast<H3Conn *>(cud);
  *pflags |= NGHTTP3_DATA_FLAG_EOF;
  if (c->req_body.empty()) return 0;
  vec[0].base = const_cast<std::uint8_t *>(c->req_body.data());
  vec[0].len = c->req_body.size();
  return 1;
}

int udp_connect(const char *host, const char *port, H3Conn *c) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  addrinfo *res = nullptr;
  if (getaddrinfo(host, port, &hints, &res) != 0) return -1;
  int fd = -1;
  for (addrinfo *rp = res; rp; rp = rp->ai_next) {
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
  freeaddrinfo(res);
  if (fd < 0) return -1;
  socklen_t ll = sizeof c->local_ss;
  getsockname(fd, reinterpret_cast<sockaddr *>(&c->local_ss), &ll);
  c->path.local.addr = reinterpret_cast<ngtcp2_sockaddr *>(&c->local_ss);
  c->path.local.addrlen = ll;
  c->path.user_data = nullptr;
  return fd;
}

// Fill buf with the next datagram to send: its length, 0 = nothing, <0 = error.
ngtcp2_ssize send_step(H3Conn *c, std::span<std::uint8_t> buf) {
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

// Create the nghttp3 client session and bind the control + QPACK streams.
int navi_h3_bind(H3Conn *c) {
  nghttp3_settings settings;
  nghttp3_settings_default(&settings);
  nghttp3_callbacks cb{};
  cb.recv_header = on_recv_header;
  cb.recv_data = on_recv_data;
  cb.end_stream = on_end_stream;
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
    auto *c = new H3Conn();
    c->authority = std::string(sni) + ":" + port;
    c->fd = udp_connect(host, port, c);
    if (c->fd < 0) {
      std::fprintf(stderr, "udp connect failed\n");
      delete c;
      return nullptr;
    }

    c->ssl_ctx = SSL_CTX_new(TLS_method());
    // Verify the server certificate by default (matching navi's TlsConfig.verify).
    if (verify) {
      SSL_CTX_set_verify(c->ssl_ctx, SSL_VERIFY_PEER, nullptr);
      if (ca_file && ca_file[0]) {
        if (SSL_CTX_load_verify_locations(c->ssl_ctx, ca_file, nullptr) != 1) {
          std::fprintf(stderr, "failed to load CA file %s\n", ca_file);
          delete c;
          return nullptr;
        }
      } else {
        SSL_CTX_set_default_verify_paths(c->ssl_ctx);
      }
    } else {
      SSL_CTX_set_verify(c->ssl_ctx, SSL_VERIFY_NONE, nullptr);
    }
    c->ssl = SSL_new(c->ssl_ctx);
    if (verify) {
      SSL_set_hostflags(c->ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
      if (SSL_set1_host(c->ssl, sni) != 1) {
        std::fprintf(stderr, "SSL_set1_host failed\n");
        delete c;
        return nullptr;
      }
    }
    if (ngtcp2_crypto_ossl_ctx_new(&c->ossl, c->ssl) != 0) {
      std::fprintf(stderr, "ossl_ctx_new failed\n");
      delete c;
      return nullptr;
    }
    c->ref.get_conn = get_conn;
    c->ref.user_data = c;
    SSL_set_app_data(c->ssl, &c->ref);
    SSL_set_connect_state(c->ssl);
    if (ngtcp2_crypto_ossl_configure_client_session(c->ssl) != 0) {
      std::fprintf(stderr, "configure_client_session failed\n");
      delete c;
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

    ngtcp2_settings settings;
    ngtcp2_settings_default(&settings);
    settings.initial_ts = now_ns();

    ngtcp2_transport_params params;
    ngtcp2_transport_params_default(&params);
    params.initial_max_streams_uni = 3;
    params.initial_max_stream_data_bidi_local = 256 * 1024;
    params.initial_max_stream_data_uni = 256 * 1024;
    params.initial_max_data = 1024 * 1024;

    ngtcp2_cid dcid, scid;
    dcid.datalen = 16;
    scid.datalen = 16;
    RAND_bytes(dcid.data, 16);
    RAND_bytes(scid.data, 16);

    if (ngtcp2_conn_client_new(&c->conn, &dcid, &scid, &c->path, NGTCP2_PROTO_VER_V1,
                               &cb, &settings, &params, nullptr, c) != 0) {
      std::fprintf(stderr, "ngtcp2_conn_client_new failed\n");
      delete c;
      return nullptr;
    }
    ngtcp2_conn_set_tls_native_handle(c->conn, c->ossl);
    return c;
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

// req_headers: extra request fields as "name\nvalue\nname\nvalue\n" (lowercased
// and filtered by the caller), or nullptr/"" for none. body may be null. Response
// fields are written to out_headers in the same "name\nvalue\n" form.
int navi_h3_request(H3Conn *c, const char *method, const char *path_,
                    const char *req_headers, const char *body, std::size_t body_len,
                    long *out_status, char *out_body, std::size_t out_cap,
                    std::size_t *out_len, char *out_headers, std::size_t hdr_cap,
                    std::size_t *hdr_len) {
  try {
    c->req_done = false;
    c->status = 0;
    c->body.clear();
    c->resp_headers.clear();
    c->req_body = body && body_len ? std::span<const std::uint8_t>{
                                         reinterpret_cast<const std::uint8_t *>(body), body_len}
                                   : std::span<const std::uint8_t>{};

    // The nghttp3_nv values point into stable memory for the whole request: the
    // string literals, the C-string params (alive across the FFI call), and
    // req_headers itself (no copy needed since nv carries explicit lengths).
    std::vector<nghttp3_nv> nva;
    nva.reserve(8);
    nva.push_back(method_nv(method));
    nva.push_back(make_nv(":scheme", "https"));
    nva.push_back(make_nv(":authority", c->authority));
    nva.push_back(make_nv(":path", path_));

    if (req_headers) {
      std::string_view hs{req_headers};
      std::vector<std::string_view> toks;
      std::size_t start = 0;
      for (std::size_t i = 0; i < hs.size(); ++i)
        if (hs[i] == '\n') {
          toks.push_back(hs.substr(start, i - start));
          start = i + 1;
        }
      for (std::size_t i = 0; i + 1 < toks.size(); i += 2)
        nva.push_back(make_nv(toks[i], toks[i + 1]));
    }

    std::int64_t sid;
    if (ngtcp2_conn_open_bidi_stream(c->conn, &sid, nullptr) != 0) return -1;
    c->req_stream = sid;
    nghttp3_data_reader dr{read_body};
    const nghttp3_data_reader *drp = c->req_body.empty() ? nullptr : &dr;
    if (nghttp3_conn_submit_request(c->h3, sid, nva.data(), nva.size(), drp, nullptr) != 0)
      return -1;
    if (drive_until(c, &c->req_done) != 0) return -1;

    *out_status = c->status;
    std::size_t k = std::min(c->body.size(), out_cap);
    std::memcpy(out_body, c->body.data(), k);
    *out_len = k;
    std::size_t hk = std::min(c->resp_headers.size(), hdr_cap);
    std::memcpy(out_headers, c->resp_headers.data(), hk);
    *hdr_len = hk;
    return 0;
  } catch (...) {
    return -1;
  }
}

}  // extern "C"

namespace {
int drive_until(H3Conn *c, const bool *flag) {
  std::array<std::uint8_t, 1500> buf{};
  pollfd pfd{c->fd, POLLIN, 0};
  for (int loops = 0; loops < 2000 && !*flag; ++loops) {
    ngtcp2_ssize n;
    while ((n = navi_h3_send(c, buf.data(), buf.size())) > 0)
      if (send(c->fd, buf.data(), static_cast<std::size_t>(n), 0) < 0) {
        std::perror("send");
        return -1;
      }
    if (n < 0) return -1;
    int pr = poll(&pfd, 1, static_cast<int>(navi_h3_timeout_ms(c)));
    if (pr > 0 && (pfd.revents & POLLIN)) {
      ssize_t r = recv(c->fd, buf.data(), buf.size(), 0);
      if (r > 0 && navi_h3_recv(c, buf.data(), static_cast<std::size_t>(r)) != 0)
        return -1;
    } else if (navi_h3_handle_timeout(c) != 0) {
      return -1;
    }
  }
  return *flag ? 0 : -1;
}
}  // namespace
