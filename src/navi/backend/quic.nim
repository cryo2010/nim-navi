## HTTP/3 QUIC transport seam (phase 2, work in progress).
##
## Compiled ONLY in a `-d:naviHttp3` build; no CI job enables it yet. This file
## establishes the module boundary described in docs/http3.md so the transport can
## be filled in against a real toolchain (ngtcp2 + nghttp3 + the OpenSSL >= 3.5
## QUIC crypto binding) and a live h3 interop server. It deliberately contains no
## fabricated FFI: the C bindings and the ~15 ngtcp2 callbacks are large and must
## be written against the installed headers and validated by an actual handshake,
## not guessed here. The procs below define the seam navi's engine will call and
## fail loudly until that work lands.

when not defined(naviHttp3):
  {.error: "navi/backend/quic is a -d:naviHttp3-only module (HTTP/3 WIP).".}

import ../core/altsvc
export altsvc.AltSvcEndpoint

const naviHttp3MinOpenSsl* = "3.5.0"
  ## Minimum OpenSSL for the QUIC crypto binding (ngtcp2_crypto_ossl). Pinned per
  ## the design decision; older OpenSSL/quictls/BoringSSL paths are out of scope.

type
  QuicConn* = ref object
    ## Owns one QUIC connection to an origin: the connected UDP socket, the
    ## ngtcp2 connection, the nghttp3 h3 session, the OpenSSL QUIC crypto context,
    ## and the loss-recovery timer. Fields are added as phase 2 lands; kept opaque
    ## for now so the seam is stable.
    host*: string
    port*: int

  QuicError* = object of CatchableError
    ## Raised for QUIC/h3 transport failures (handshake, stream reset, timeout).
    ## The engine treats it as a signal to fall back to h2/h1 for the origin.

template notYet(): untyped =
  raise newException(QuicError,
    "navi HTTP/3: QUIC transport not yet implemented (see docs/http3.md phase 2)")

proc connectQuic*(endpoint: AltSvcEndpoint, serverName: string): QuicConn =
  ## Open a QUIC connection to a discovered h3 endpoint and complete the handshake
  ## (ALPN "h3", TLS verification against `serverName`). Phase 2.
  notYet()

proc close*(c: QuicConn) =
  ## Close the QUIC connection and release the UDP socket and library state.
  discard   # nothing to release until the connection owns resources

# Request submission, response/stream reading, and the async multiplexer
# (backend/h3mux.nim) and request/response mapping (core/h3glue.nim) follow in
# phase 2, once connectQuic drives a real handshake.
