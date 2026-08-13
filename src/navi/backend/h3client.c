/* navi HTTP/3 client driver (phase 2d): a persistent QUIC connection that serves
 * multiple HTTP/3 GETs, using ngtcp2 (transport + OpenSSL 3.5 crypto binding) and
 * nghttp3 (h3 + QPACK). navi's own code, compiled into navi by backend/quic.nim
 * ({.compile.}) and driven from Nim via navi_h3_open / navi_h3_request /
 * navi_h3_close. Verified against the tests/interop/http3 Caddy origin.
 *
 * Scope: blocking, one request in flight at a time; the server certificate and
 * hostname are verified by default (a custom CA is supported). TODO: async/mux
 * (concurrent streams), and streaming request/response bodies. */
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>
#include <ngtcp2/ngtcp2_crypto_ossl.h>
#include <nghttp3/nghttp3.h>
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/x509v3.h>

#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
  ngtcp2_crypto_conn_ref ref;
  ngtcp2_conn *conn;
  nghttp3_conn *h3;
  int fd;
  ngtcp2_path path;
  struct sockaddr_storage local_ss, remote_ss;
  SSL_CTX *ssl_ctx;
  SSL *ssl;
  ngtcp2_crypto_ossl_ctx *ossl;
  char authority[256];
  int handshake_done;
  /* per-request state (one request in flight at a time) */
  int64_t req_stream;
  int req_done;
  long status;
  char body[65536];
  size_t body_len;
  char resp_headers[16384]; /* response fields as "name\nvalue\n" (no pseudo) */
  size_t resp_headers_len;
  const char *req_body; /* request body for the in-flight request (borrowed) */
  size_t req_body_len;
} navi_h3_conn;

void navi_h3_close(navi_h3_conn *c); /* forward decl for navi_h3_open cleanup */

static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *r) {
  return ((navi_h3_conn *)r->user_data)->conn;
}

static uint64_t now_ns(void) {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (uint64_t)t.tv_sec * NGTCP2_SECONDS + (uint64_t)t.tv_nsec;
}

static void rand_cb(uint8_t *dest, size_t destlen, const ngtcp2_rand_ctx *ctx) {
  (void)ctx;
  RAND_bytes(dest, (int)destlen);
}

static int get_new_cid(ngtcp2_conn *c, ngtcp2_cid *cid, uint8_t *token,
                       size_t cidlen, void *ud) {
  (void)c;
  (void)ud;
  if (RAND_bytes(cid->data, (int)cidlen) != 1)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  cid->datalen = cidlen;
  if (RAND_bytes(token, NGTCP2_STATELESS_RESET_TOKENLEN) != 1)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

static int hs_done(ngtcp2_conn *c, void *ud) {
  (void)c;
  ((navi_h3_conn *)ud)->handshake_done = 1;
  return 0;
}

static int on_recv_stream_data(ngtcp2_conn *conn, uint32_t flags,
                               int64_t stream_id, uint64_t offset,
                               const uint8_t *data, size_t datalen, void *ud,
                               void *sud) {
  (void)offset;
  (void)sud;
  navi_h3_conn *c = ud;
  if (!c->h3)
    return 0;
  int fin = (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0;
  nghttp3_ssize n = nghttp3_conn_read_stream(c->h3, stream_id, data, datalen, fin);
  if (n < 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  ngtcp2_conn_extend_max_stream_offset(conn, stream_id, (uint64_t)n);
  ngtcp2_conn_extend_max_offset(conn, (uint64_t)n);
  return 0;
}

static int on_acked(ngtcp2_conn *conn, int64_t stream_id, uint64_t offset,
                    uint64_t datalen, void *ud, void *sud) {
  (void)conn;
  (void)offset;
  (void)sud;
  navi_h3_conn *c = ud;
  if (c->h3)
    nghttp3_conn_add_ack_offset(c->h3, stream_id, datalen);
  return 0;
}

static int on_stream_close(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                           uint64_t app_error_code, void *ud, void *sud) {
  (void)conn;
  (void)flags;
  (void)sud;
  navi_h3_conn *c = ud;
  if (c->h3)
    nghttp3_conn_close_stream(c->h3, stream_id, app_error_code);
  return 0;
}

static int on_recv_header(nghttp3_conn *h3, int64_t stream_id, int32_t token,
                          nghttp3_rcbuf *name, nghttp3_rcbuf *value,
                          uint8_t flags, void *cud, void *sud) {
  (void)h3;
  (void)stream_id;
  (void)token;
  (void)flags;
  (void)sud;
  navi_h3_conn *c = cud;
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name);
  nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
  if (n.len == 7 && memcmp(n.base, ":status", 7) == 0) {
    char tmp[8] = {0};
    size_t k = v.len < 7 ? v.len : 7;
    memcpy(tmp, v.base, k);
    c->status = atol(tmp);
  } else if (n.len > 0 && n.base[0] != ':') { /* a regular response field */
    size_t need = n.len + 1 + v.len + 1;
    if (c->resp_headers_len + need <= sizeof c->resp_headers) {
      memcpy(c->resp_headers + c->resp_headers_len, n.base, n.len);
      c->resp_headers_len += n.len;
      c->resp_headers[c->resp_headers_len++] = '\n';
      memcpy(c->resp_headers + c->resp_headers_len, v.base, v.len);
      c->resp_headers_len += v.len;
      c->resp_headers[c->resp_headers_len++] = '\n';
    }
  }
  return 0;
}

static int on_recv_data(nghttp3_conn *h3, int64_t stream_id,
                        const uint8_t *data, size_t datalen, void *cud,
                        void *sud) {
  (void)h3;
  (void)stream_id;
  (void)sud;
  navi_h3_conn *c = cud;
  size_t room = sizeof c->body - c->body_len;
  size_t k = datalen < room ? datalen : room;
  memcpy(c->body + c->body_len, data, k);
  c->body_len += k;
  return 0;
}

static int on_end_stream(nghttp3_conn *h3, int64_t stream_id, void *cud,
                         void *sud) {
  (void)h3;
  (void)stream_id;
  (void)sud;
  ((navi_h3_conn *)cud)->req_done = 1;
  return 0;
}

static int udp_connect(const char *host, const char *port, ngtcp2_path *path,
                       struct sockaddr_storage *local,
                       struct sockaddr_storage *remote) {
  struct addrinfo hints, *res, *rp;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  if (getaddrinfo(host, port, &hints, &res) != 0)
    return -1;
  int fd = -1;
  for (rp = res; rp; rp = rp->ai_next) {
    fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (fd < 0)
      continue;
    if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
      memcpy(remote, rp->ai_addr, rp->ai_addrlen);
      path->remote.addr = (ngtcp2_sockaddr *)remote;
      path->remote.addrlen = rp->ai_addrlen;
      break;
    }
    close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0)
    return -1;
  socklen_t ll = sizeof *local;
  getsockname(fd, (struct sockaddr *)local, &ll);
  path->local.addr = (ngtcp2_sockaddr *)local;
  path->local.addrlen = ll;
  path->user_data = NULL;
  return fd;
}

#define MAKE_NV(N, V)                                                           \
  {                                                                            \
    (uint8_t *)(N), (uint8_t *)(V), sizeof(N) - 1, strlen(V),                  \
        NGHTTP3_NV_FLAG_NONE                                                   \
  }

/* Body producer: hand nghttp3 the whole buffered request body in one vec, with
 * EOF, on the first call. The body is borrowed from the caller for the duration
 * of the request. */
static nghttp3_ssize read_body(nghttp3_conn *h3, int64_t stream_id,
                               nghttp3_vec *vec, size_t veccnt, uint32_t *pflags,
                               void *cud, void *sud) {
  (void)h3;
  (void)stream_id;
  (void)veccnt;
  (void)sud;
  navi_h3_conn *c = cud;
  *pflags |= NGHTTP3_DATA_FLAG_EOF;
  if (c->req_body_len == 0)
    return 0;
  vec[0].base = (uint8_t *)c->req_body;
  vec[0].len = c->req_body_len;
  return 1;
}

/* Pump reads and writes until *flag becomes nonzero, or an error occurs. */
static int run_until(navi_h3_conn *c, int *flag) {
  uint8_t buf[1500];
  struct pollfd pfd = {.fd = c->fd, .events = POLLIN};
  for (int loops = 0; loops < 2000 && !*flag; loops++) {
    for (;;) {
      int64_t stream_id = -1;
      int fin = 0;
      nghttp3_vec vec[16];
      nghttp3_ssize sveccnt = 0;
      if (c->h3) {
        sveccnt = nghttp3_conn_writev_stream(c->h3, &stream_id, &fin, vec, 16);
        if (sveccnt < 0) {
          fprintf(stderr, "nghttp3 writev: %s\n",
                  nghttp3_strerror((int)sveccnt));
          return -1;
        }
      }
      ngtcp2_ssize ndatalen = 0;
      uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE;
      if (fin)
        flags |= NGTCP2_WRITE_STREAM_FLAG_FIN;
      ngtcp2_pkt_info pi;
      ngtcp2_ssize wrote = ngtcp2_conn_writev_stream(
          c->conn, &c->path, &pi, buf, sizeof buf, &ndatalen, flags, stream_id,
          (const ngtcp2_vec *)vec, (size_t)sveccnt, now_ns());
      if (wrote == NGTCP2_ERR_WRITE_MORE) {
        nghttp3_conn_add_write_offset(c->h3, stream_id, (size_t)ndatalen);
        continue;
      }
      if (wrote < 0) {
        fprintf(stderr, "writev_stream: %s\n", ngtcp2_strerror((int)wrote));
        return -1;
      }
      if (ndatalen > 0)
        nghttp3_conn_add_write_offset(c->h3, stream_id, (size_t)ndatalen);
      if (wrote == 0)
        break;
      if (send(c->fd, buf, (size_t)wrote, 0) < 0) {
        perror("send");
        return -1;
      }
    }
    ngtcp2_tstamp expiry = ngtcp2_conn_get_expiry(c->conn);
    ngtcp2_tstamp t = now_ns();
    int timeout_ms = 1000;
    if (expiry != UINT64_MAX)
      timeout_ms = expiry <= t ? 0 : (int)((expiry - t) / NGTCP2_MILLISECONDS);
    int pr = poll(&pfd, 1, timeout_ms);
    if (pr > 0 && (pfd.revents & POLLIN)) {
      ssize_t r = recv(c->fd, buf, sizeof buf, 0);
      if (r > 0) {
        ngtcp2_pkt_info pi = {0};
        int rv = ngtcp2_conn_read_pkt(c->conn, &c->path, &pi, buf, (size_t)r,
                                      now_ns());
        if (rv != 0) {
          fprintf(stderr, "read_pkt: %s\n", ngtcp2_strerror(rv));
          return -1;
        }
      }
    } else if (ngtcp2_conn_handle_expiry(c->conn, now_ns()) != 0) {
      fprintf(stderr, "handle_expiry failed\n");
      return -1;
    }
  }
  return *flag ? 0 : -1;
}

/* Create the nghttp3 client session and bind the control + QPACK streams. */
static int bind_h3(navi_h3_conn *c) {
  nghttp3_settings settings;
  nghttp3_settings_default(&settings);
  nghttp3_callbacks cb;
  memset(&cb, 0, sizeof cb);
  cb.recv_header = on_recv_header;
  cb.recv_data = on_recv_data;
  cb.end_stream = on_end_stream;
  if (nghttp3_conn_client_new(&c->h3, &cb, &settings, NULL, c) != 0)
    return -1;
  int64_t ctrl, qenc, qdec;
  if (ngtcp2_conn_open_uni_stream(c->conn, &ctrl, NULL) != 0 ||
      ngtcp2_conn_open_uni_stream(c->conn, &qenc, NULL) != 0 ||
      ngtcp2_conn_open_uni_stream(c->conn, &qdec, NULL) != 0)
    return -1;
  if (nghttp3_conn_bind_control_stream(c->h3, ctrl) != 0 ||
      nghttp3_conn_bind_qpack_streams(c->h3, qenc, qdec) != 0)
    return -1;
  return 0;
}

navi_h3_conn *navi_h3_open(const char *host, const char *port, const char *sni,
                           const char *ca_file, int verify) {
  static int crypto_inited = 0;
  if (!crypto_inited) {
    if (ngtcp2_crypto_ossl_init() != 0) {
      fprintf(stderr, "ngtcp2_crypto_ossl_init failed\n");
      return NULL;
    }
    crypto_inited = 1;
  }

  navi_h3_conn *c = calloc(1, sizeof *c);
  if (!c)
    return NULL;
  snprintf(c->authority, sizeof c->authority, "%s:%s", sni, port);

  c->fd = udp_connect(host, port, &c->path, &c->local_ss, &c->remote_ss);
  if (c->fd < 0) {
    fprintf(stderr, "udp connect failed\n");
    free(c);
    return NULL;
  }

  c->ssl_ctx = SSL_CTX_new(TLS_method());
  /* Verify the server certificate by default (matching navi's TlsConfig.verify).
   * A ca_file adds a custom CA; otherwise the system trust store is used. */
  if (verify) {
    SSL_CTX_set_verify(c->ssl_ctx, SSL_VERIFY_PEER, NULL);
    if (ca_file && ca_file[0]) {
      if (SSL_CTX_load_verify_locations(c->ssl_ctx, ca_file, NULL) != 1) {
        fprintf(stderr, "failed to load CA file %s\n", ca_file);
        goto fail;
      }
    } else {
      SSL_CTX_set_default_verify_paths(c->ssl_ctx);
    }
  } else {
    SSL_CTX_set_verify(c->ssl_ctx, SSL_VERIFY_NONE, NULL);
  }
  c->ssl = SSL_new(c->ssl_ctx);
  if (verify) {
    SSL_set_hostflags(c->ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    if (SSL_set1_host(c->ssl, sni) != 1) {
      fprintf(stderr, "SSL_set1_host failed\n");
      goto fail;
    }
  }
  if (ngtcp2_crypto_ossl_ctx_new(&c->ossl, c->ssl) != 0) {
    fprintf(stderr, "ossl_ctx_new failed\n");
    goto fail;
  }
  c->ref.get_conn = get_conn;
  c->ref.user_data = c;
  SSL_set_app_data(c->ssl, &c->ref);
  SSL_set_connect_state(c->ssl);
  if (ngtcp2_crypto_ossl_configure_client_session(c->ssl) != 0) {
    fprintf(stderr, "configure_client_session failed\n");
    goto fail;
  }
  SSL_set_alpn_protos(c->ssl, (const unsigned char *)"\x02h3", 3);
  SSL_set_tlsext_host_name(c->ssl, sni);

  ngtcp2_callbacks cb;
  memset(&cb, 0, sizeof cb);
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

  if (ngtcp2_conn_client_new(&c->conn, &dcid, &scid, &c->path,
                             NGTCP2_PROTO_VER_V1, &cb, &settings, &params, NULL,
                             c) != 0) {
    fprintf(stderr, "ngtcp2_conn_client_new failed\n");
    goto fail;
  }
  ngtcp2_conn_set_tls_native_handle(c->conn, c->ossl);

  if (run_until(c, &c->handshake_done) != 0)
    goto fail;
  if (bind_h3(c) != 0)
    goto fail;
  return c;

fail:
  navi_h3_close(c);
  return NULL;
}

/* req_headers: extra request fields as "name\nvalue\nname\nvalue\n" (already
 * lowercased and filtered by the caller), or NULL/"" for none. Response fields
 * are written to out_headers in the same "name\nvalue\n" form. */
#define H3_MAX_NV 128

int navi_h3_request(navi_h3_conn *c, const char *method, const char *path_,
                    const char *req_headers, const char *body, size_t body_len,
                    long *out_status, char *out_body, size_t out_cap,
                    size_t *out_len, char *out_headers, size_t hdr_cap,
                    size_t *hdr_len) {
  c->req_done = 0;
  c->status = 0;
  c->body_len = 0;
  c->resp_headers_len = 0;
  c->req_body = body;
  c->req_body_len = body_len;

  nghttp3_nv nva[4 + H3_MAX_NV];
  nva[0] = (nghttp3_nv){(uint8_t *)":method", (uint8_t *)method, 7,
                        strlen(method), NGHTTP3_NV_FLAG_NONE};
  nva[1] = (nghttp3_nv)MAKE_NV(":scheme", "https");
  nva[2] = (nghttp3_nv){(uint8_t *)":authority", (uint8_t *)c->authority, 10,
                        strlen(c->authority), NGHTTP3_NV_FLAG_NONE};
  nva[3] = (nghttp3_nv){(uint8_t *)":path", (uint8_t *)path_, 5, strlen(path_),
                        NGHTTP3_NV_FLAG_NONE};
  size_t nvlen = 4;

  /* Tokenize a mutable copy of req_headers on '\n' into name/value pairs. The
   * buffer must outlive submit (nghttp3 copies during QPACK encoding). */
  char *hdrbuf = strdup(req_headers ? req_headers : "");
  if (!hdrbuf)
    return -1;
  char *toks[2 * H3_MAX_NV];
  int ntok = 0;
  char *start = hdrbuf;
  for (char *p = hdrbuf; *p && ntok < 2 * H3_MAX_NV; p++) {
    if (*p == '\n') {
      *p = '\0';
      toks[ntok++] = start;
      start = p + 1;
    }
  }
  for (int i = 0; i + 1 < ntok && nvlen < 4 + H3_MAX_NV; i += 2) {
    nva[nvlen].name = (uint8_t *)toks[i];
    nva[nvlen].namelen = strlen(toks[i]);
    nva[nvlen].value = (uint8_t *)toks[i + 1];
    nva[nvlen].valuelen = strlen(toks[i + 1]);
    nva[nvlen].flags = NGHTTP3_NV_FLAG_NONE;
    nvlen++;
  }

  int rv = 0;
  int64_t sid;
  if (ngtcp2_conn_open_bidi_stream(c->conn, &sid, NULL) != 0) {
    rv = -1;
    goto done;
  }
  c->req_stream = sid;
  nghttp3_data_reader dr = {read_body};
  const nghttp3_data_reader *drp = body_len > 0 ? &dr : NULL;
  if (nghttp3_conn_submit_request(c->h3, sid, nva, nvlen, drp, NULL) != 0) {
    rv = -1;
    goto done;
  }
  if (run_until(c, &c->req_done) != 0) {
    rv = -1;
    goto done;
  }

  *out_status = c->status;
  size_t k = c->body_len < out_cap ? c->body_len : out_cap;
  memcpy(out_body, c->body, k);
  *out_len = k;
  size_t hk = c->resp_headers_len < hdr_cap ? c->resp_headers_len : hdr_cap;
  memcpy(out_headers, c->resp_headers, hk);
  *hdr_len = hk;

done:
  free(hdrbuf);
  return rv;
}

void navi_h3_close(navi_h3_conn *c) {
  if (!c)
    return;
  if (c->h3)
    nghttp3_conn_del(c->h3);
  if (c->conn)
    ngtcp2_conn_del(c->conn);
  if (c->ossl)
    ngtcp2_crypto_ossl_ctx_del(c->ossl);
  if (c->ssl)
    SSL_free(c->ssl);
  if (c->ssl_ctx)
    SSL_CTX_free(c->ssl_ctx);
  if (c->fd >= 0)
    close(c->fd);
  free(c);
}
