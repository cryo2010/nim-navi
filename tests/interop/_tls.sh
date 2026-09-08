#!/usr/bin/env bash
# Shared TLS helpers for the interop scripts: throwaway cert generation and a
# TLS-readiness poll. Sourced transitively by every script via _win.sh, so the
# ~4-line `openssl req -x509` block and the hand-rolled readiness loops live in
# one place instead of being copy-pasted (and drifting: some cert-gen sites had
# hardcoded `/CN=...` subjects that break under MSYS, and readiness polls used
# differing sleeps/probes). Depends on navi_subj/navi_rmtree from _win.sh.

navi_certgen() {
  ## Generate a throwaway self-signed cert (RSA-2048, valid 1 day) for the interop
  ## TLS servers. Routes the subject through navi_subj so it is correct under
  ## MSYS/Windows too. Usage:
  ##   navi_certgen <keyout> <certout> <CN> [subjectAltName]
  ## e.g. navi_certgen "$work/key.pem" "$work/cert.pem" 127.0.0.1 "IP:127.0.0.1"
  local key="$1" cert="$2" cn="$3" san="${4:-}"
  if [ -n "$san" ]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
      -keyout "$key" -out "$cert" -subj "$(navi_subj "CN=$cn")" \
      -addext "subjectAltName=$san" >/dev/null 2>&1
  else
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
      -keyout "$key" -out "$cert" -subj "$(navi_subj "CN=$cn")" >/dev/null 2>&1
  fi
}

navi_wait_tls() {
  ## Poll until a TLS server answers on host:port, or give up. Usage:
  ##   navi_wait_tls <host:port> [openssl s_client flags...]
  ## Returns 0 once the server presents a certificate, non-zero if it never did.
  local hostport="$1"; shift
  local _
  for _ in $(seq 1 60); do
    if echo | openssl s_client -connect "$hostport" "$@" 2>/dev/null | grep -q "BEGIN CERT"; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}
