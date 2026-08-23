#!/usr/bin/env bash
# Portability shim for the interop/demo scripts, sourced near the top of each one.
#
# The scripts run under Git Bash on Windows CI, where bash is an MSYS program but
# openssl, node and the binaries nim produces are all native Windows ones. MSYS
# rewrites paths as they cross that boundary, which is helpful for real paths and
# actively wrong for arguments that only look like paths. Everything here is a
# no-op on Linux and macOS.

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) navi_on_windows=1 ;;
  *)                    navi_on_windows="" ;;
esac

navi_subj() {
  ## Build an openssl `-subj` argument. MSYS rewrites a leading-slash argument
  ## into a Windows path before it reaches native openssl.exe, so "/CN=x" arrives
  ## as "C:/Program Files/Git/CN=x" and openssl rejects it. A leading "//" is
  ## MSYS's escape and arrives as a single slash; elsewhere it would parse as an
  ## empty first RDN, so only double it where the rewriting happens.
  ##
  ##   openssl req ... -subj "$(navi_subj CN=127.0.0.1)"
  if [ -n "$navi_on_windows" ]; then printf '//%s' "$1"; else printf '/%s' "$1"; fi
}

navi_path() {
  ## Convert a shell path into one a native binary can open. MSYS converts paths
  ## in *arguments* but not in *environment variables*, so a $(mktemp -d) path
  ## exported for a nim-built binary to read arrives as an unusable "/tmp/...".
  ## Use this whenever a path is exported rather than passed as an argument.
  ##
  ##   export NAVI_FOO_CERT="$(navi_path "$work/cert.pem")"
  if [ -n "$navi_on_windows" ]; then cygpath -m "$1"; else printf '%s' "$1"; fi
}

navi_bin() {
  ## The on-disk name of a binary nim built at `$1` -- nim appends .exe on
  ## Windows. Git Bash often retries a bare name with .exe, but not when the path
  ## is quoted into a variable first, so be explicit.
  if [ -n "$navi_on_windows" ]; then printf '%s.exe' "$1"; else printf '%s' "$1"; fi
}

navi_rmtree() {
  ## Remove a temp tree, tolerating Windows' asynchronous handle release: for a
  ## moment after a killed child exits its handles are still open, so a straight
  ## `rm -rf` races it and reports "Device or resource busy". Retry briefly, and
  ## never fail a cleanup trap over a temp directory the OS will reclaim anyway.
  rm -rf "$1" 2>/dev/null && return 0
  if [ -n "$navi_on_windows" ]; then
    for _ in $(seq 1 10); do
      sleep 0.3
      rm -rf "$1" 2>/dev/null && return 0
    done
  fi
  return 0
}
