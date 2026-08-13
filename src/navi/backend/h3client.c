/* navi HTTP/3 client shim (phase 2b): a single blocking HTTP/3 GET over QUIC
 * using ngtcp2 (transport + OpenSSL 3.5 crypto binding) and nghttp3 (h3 +
 * QPACK). Compiled into navi by backend/quic.nim ({.compile.}) and driven from
 * Nim via navi_h3_get(). Verified against the tests/interop/http3 Caddy origin.
 *
 * Scope: blocking, one request per call, sync path; the server certificate and
 * hostname are verified by default (a custom CA is supported). TODO: a
 * persistent connection object, async/mux, and streaming bodies. */
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
  int64_t req_stream;
  int handshake_done;
  int req_done;
  long status;
  char body[65536];
  size_t body_len;
} client;

static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *r) {
  return ((client *)r->user_data)->conn;
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
  ((client *)ud)->handshake_done = 1;
  return 0;
}

static int on_recv_stream_data(ngtcp2_conn *conn, uint32_t flags,
                               int64_t stream_id, uint64_t offset,
                               const uint8_t *data, size_t datalen, void *ud,
                               void *sud) {
  (void)offset;
  (void)sud;
  client *c = ud;
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
  client *c = ud;
  if (c->h3)
    nghttp3_conn_add_ack_offset(c->h3, stream_id, datalen);
  return 0;
}

static int on_stream_close(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                           uint64_t app_error_code, void *ud, void *sud) {
  (void)conn;
  (void)flags;
  (void)sud;
  client *c = ud;
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
  client *c = cud;
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name);
  nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
  if (n.len == 7 && memcmp(n.base, ":status", 7) == 0) {
    char tmp[8] = {0};
    size_t k = v.len < 7 ? v.len : 7;
    memcpy(tmp, v.base, k);
    c->status = atol(tmp);
  }
  return 0;
}

static int on_recv_data(nghttp3_conn *h3, int64_t stream_id,
                        const uint8_t *data, size_t datalen, void *cud,
                        void *sud) {
  (void)h3;
  (void)stream_id;
  (void)sud;
  client *c = cud;
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
  ((client *)cud)->req_done = 1;
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

static int setup_h3(client *c, const char *authority, const char *path_) {
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
  int64_t sid;
  if (ngtcp2_conn_open_bidi_stream(c->conn, &sid, NULL) != 0)
    return -1;
  c->req_stream = sid;
  nghttp3_nv nva[] = {MAKE_NV(":method", "GET"), MAKE_NV(":scheme", "https"),
                      {(uint8_t *)":authority", (uint8_t *)authority, 10,
                       strlen(authority), NGHTTP3_NV_FLAG_NONE},
                      {(uint8_t *)":path", (uint8_t *)path_, 5, strlen(path_),
                       NGHTTP3_NV_FLAG_NONE}};
  return nghttp3_conn_submit_request(c->h3, sid, nva, 4, NULL, NULL);
}

int navi_h3_get(const char *host, const char *port, const char *sni,
                const char *path_, const char *ca_file, int verify,
                long *out_status, char *out_body, size_t out_cap,
                size_t *out_len) {
  static int crypto_inited = 0;
  char authority[256];
  snprintf(authority, sizeof authority, "%s:%s", sni, port);

  if (!crypto_inited) {
    if (ngtcp2_crypto_ossl_init() != 0) {
      fprintf(stderr, "ngtcp2_crypto_ossl_init failed\n");
      return -1;
    }
    crypto_inited = 1;
  }

  client c;
  memset(&c, 0, sizeof c);

  ngtcp2_path path;
  struct sockaddr_storage local_ss, remote_ss;
  int fd = udp_connect(host, port, &path, &local_ss, &remote_ss);
  if (fd < 0) {
    fprintf(stderr, "udp connect failed\n");
    return 1;
  }

  SSL_CTX *ssl_ctx = SSL_CTX_new(TLS_method());
  /* Verify the server certificate by default (secure by default, matching
   * navi's TlsConfig.verify). A ca_file adds a custom CA (TlsConfig.caFile);
   * otherwise the system trust store is used. verify=0 disables checking. */
  if (verify) {
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);
    if (ca_file && ca_file[0]) {
      if (SSL_CTX_load_verify_locations(ssl_ctx, ca_file, NULL) != 1) {
        fprintf(stderr, "failed to load CA file %s\n", ca_file);
        return -1;
      }
    } else {
      SSL_CTX_set_default_verify_paths(ssl_ctx);
    }
  } else {
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_NONE, NULL);
  }
  SSL *ssl = SSL_new(ssl_ctx);
  /* Hostname verification: a cert valid for a different host must fail. */
  if (verify) {
    SSL_set_hostflags(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    if (SSL_set1_host(ssl, sni) != 1) {
      fprintf(stderr, "SSL_set1_host failed\n");
      return -1;
    }
  }
  ngtcp2_crypto_ossl_ctx *ossl;
  if (ngtcp2_crypto_ossl_ctx_new(&ossl, ssl) != 0) {
    fprintf(stderr, "ossl_ctx_new failed\n");
    return 1;
  }
  c.ref.get_conn = get_conn;
  c.ref.user_data = &c;
  SSL_set_app_data(ssl, &c.ref);
  SSL_set_connect_state(ssl);
  if (ngtcp2_crypto_ossl_configure_client_session(ssl) != 0) {
    fprintf(stderr, "configure_client_session failed\n");
    return 1;
  }
  SSL_set_alpn_protos(ssl, (const unsigned char *)"\x02h3", 3);
  SSL_set_tlsext_host_name(ssl, sni);

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

  if (ngtcp2_conn_client_new(&c.conn, &dcid, &scid, &path, NGTCP2_PROTO_VER_V1,
                             &cb, &settings, &params, NULL, &c) != 0) {
    fprintf(stderr, "ngtcp2_conn_client_new failed\n");
    return 1;
  }
  ngtcp2_conn_set_tls_native_handle(c.conn, ossl);

  uint8_t buf[1500];
  struct pollfd pfd = {.fd = fd, .events = POLLIN};

  for (int loops = 0; loops < 500 && !c.req_done; loops++) {
    if (c.handshake_done && !c.h3) {
      if (setup_h3(&c, authority, path_) != 0) {
        fprintf(stderr, "h3 setup failed\n");
        return 1;
      }
    }
    /* drain outgoing: pump nghttp3 stream data through ngtcp2 packets */
    for (;;) {
      int64_t stream_id = -1;
      int fin = 0;
      nghttp3_vec vec[16];
      nghttp3_ssize sveccnt = 0;
      if (c.h3) {
        sveccnt = nghttp3_conn_writev_stream(c.h3, &stream_id, &fin, vec, 16);
        if (sveccnt < 0) {
          fprintf(stderr, "nghttp3 writev: %s\n",
                  nghttp3_strerror((int)sveccnt));
          return 1;
        }
      }
      ngtcp2_ssize ndatalen = 0;
      uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE;
      if (fin)
        flags |= NGTCP2_WRITE_STREAM_FLAG_FIN;
      ngtcp2_pkt_info pi;
      ngtcp2_ssize wrote = ngtcp2_conn_writev_stream(
          c.conn, &path, &pi, buf, sizeof buf, &ndatalen, flags, stream_id,
          (const ngtcp2_vec *)vec, (size_t)sveccnt, now_ns());
      if (wrote == NGTCP2_ERR_WRITE_MORE) {
        nghttp3_conn_add_write_offset(c.h3, stream_id, (size_t)ndatalen);
        continue;
      }
      if (wrote < 0) {
        fprintf(stderr, "writev_stream: %s\n", ngtcp2_strerror((int)wrote));
        return 1;
      }
      if (ndatalen > 0)
        nghttp3_conn_add_write_offset(c.h3, stream_id, (size_t)ndatalen);
      if (wrote == 0)
        break;
      if (send(fd, buf, (size_t)wrote, 0) < 0) {
        perror("send");
        return 1;
      }
    }
    ngtcp2_tstamp expiry = ngtcp2_conn_get_expiry(c.conn);
    ngtcp2_tstamp t = now_ns();
    int timeout_ms = 1000;
    if (expiry != UINT64_MAX)
      timeout_ms = expiry <= t ? 0 : (int)((expiry - t) / NGTCP2_MILLISECONDS);
    int pr = poll(&pfd, 1, timeout_ms);
    if (pr > 0 && (pfd.revents & POLLIN)) {
      ssize_t r = recv(fd, buf, sizeof buf, 0);
      if (r > 0) {
        ngtcp2_pkt_info pi = {0};
        int rv = ngtcp2_conn_read_pkt(c.conn, &path, &pi, buf, (size_t)r,
                                      now_ns());
        if (rv != 0) {
          fprintf(stderr, "read_pkt: %s\n", ngtcp2_strerror(rv));
          return 1;
        }
      }
    } else if (ngtcp2_conn_handle_expiry(c.conn, now_ns()) != 0) {
      fprintf(stderr, "handle_expiry failed\n");
      return 1;
    }
  }

  if (!c.req_done) {
    fprintf(stderr, "request did not complete\n");
    return -1;
  }
  *out_status = c.status;
  size_t k = c.body_len < out_cap ? c.body_len : out_cap;
  memcpy(out_body, c.body, k);
  *out_len = k;

  if (c.h3)
    nghttp3_conn_del(c.h3);
  ngtcp2_conn_del(c.conn);
  ngtcp2_crypto_ossl_ctx_del(ossl);
  SSL_free(ssl);
  SSL_CTX_free(ssl_ctx);
  close(fd);
  return 0;
}
