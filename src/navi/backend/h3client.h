// TLS policy handed across the HTTP/3 FFI boundary: navi's `TlsConfig`
// (backend/api.nim) flattened into a POD so the QUIC SSL_CTX built in
// h3client.cpp honours the same configuration the TCP backends do, instead of
// receiving only a CA file and a verify flag (#419).
//
// Shared by h3client.cpp and backend/quic.nim, which imports this struct by name
// so the two sides can never drift out of layout.
//
// Every `const char *` is a NUL-terminated buffer owned by the Nim caller and
// borrowed only for the duration of the navi_h3_new / navi_h3_open call; a null
// pointer and an empty string both mean "unset".
#ifndef NAVI_H3CLIENT_H
#define NAVI_H3CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  const char *ca_file;       // custom CA bundle path ("" = the system trust store)
  const char *ca_bundle;     // extra trusted roots as an in-memory PEM string
  const char *pkcs12_file;   // client credential: PKCS#12 bundle (highest precedence)
  const char *cert_pem;      // client credential: in-memory PEM chain
  const char *key_pem;       // private key PEM ("" reuses cert_pem)
  const char *cert_file;     // client credential: certificate file (PEM or DER)
  const char *key_file;      // private key file ("" reuses cert_file)
  const char *password;      // key passphrase / PKCS#12 password
  const char *ciphers;       // TLS <= 1.2 cipher list (informational for QUIC)
  const char *cipher_suites; // TLS 1.3 ciphersuites
  int verify;                // verify the chain + hostname after the handshake
  int min_version;           // 0 unset, else 10/11/12/13 for TLS 1.0 .. 1.3
  int max_version;           // 0 unset, else 10/11/12/13 for TLS 1.0 .. 1.3
  unsigned long long handshake_timeout_ms;  // 0 = unset (ngtcp2's UINT64_MAX default)
  // Identity of the client that owns this policy: the address of its
  // TlsConfig.contextStore, or 0 for a bare TlsConfig with no store. Part of the
  // SSL_CTX cache key, and what navi_h3_ctx_release names when the client is
  // closed, so one client's contexts are never handed to another and are dropped
  // with it (#454). Never dereferenced on this side.
  unsigned long long ctx_owner;
} NaviH3Tls;

// Drop every cached SSL_CTX built for `owner` (see NaviH3Tls.ctx_owner); a no-op
// for 0. Called when a navi client's TLS context store is freed. Connections still
// using a released context hold their own reference and are unaffected.
void navi_h3_ctx_release(unsigned long long owner);

// SSL_CTX cache counters, for the interop probe: contexts actually built, cache
// hits, and entries currently held. Any out-pointer may be null.
void navi_h3_ctx_cache_stats(unsigned long long *builds, unsigned long long *reuses,
                             unsigned long long *entries);

// --- last-error reporting (#446) ---------------------------------------------
// What kind of failure the driver recorded, so the Nim side can raise the right
// exception without parsing the text: a TLS code becomes a `QuicTlsError` (a
// `QuicError` subtype, so existing h2/h1 fallback logic is unaffected), everything
// else a plain `QuicError`. Keep in step with backend/quic.nim, which imports these
// names from this header.
typedef enum {
  NAVI_H3_ERR_NONE = 0,        // nothing recorded
  NAVI_H3_ERR_INTERNAL = 1,    // a driver/library setup call failed unexpectedly
  NAVI_H3_ERR_NETWORK = 2,     // socket, datagram or QUIC transport failure
  NAVI_H3_ERR_TLS = 3,         // the TLS policy could not be applied (trust store,
                               // client credential, ciphers, expected identity)
  NAVI_H3_ERR_TLS_VERIFY = 4,  // the peer's certificate or identity was rejected
  NAVI_H3_ERR_PROTOCOL = 5     // the peer is not a usable HTTP/3 endpoint
} NaviH3ErrCode;

// The reason recorded for the most recent failure on the CALLING thread, or "" when
// there is none, plus its NaviH3ErrCode. The driver never writes to stderr; this is
// how a failure explains itself. The pointer stays valid until the next failure on
// that thread, so a caller copies it before doing anything else.
const char *navi_h3_last_error(void);
int navi_h3_last_error_code(void);

#ifdef __cplusplus
}
#endif

#endif  // NAVI_H3CLIENT_H
