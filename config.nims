# navi build configuration that needs logic nim.cfg cannot express. The static
# part (-d:ssl, --path:src, the Windows DLL names) stays in nim.cfg; both files
# are read for every build in this repo.

import std/strutils   # join, for the warning below

when defined(macosx):
  # Link OpenSSL at build time instead of letting std/openssl dlopen it by name.
  # The by-name load resolves to Apple's LibreSSL from the dyld shared cache, and
  # it mixes generations (libssl.48 over libcrypto.46 for the libssl calls,
  # libcrypto.41 for navi's direct libcrypto calls), while a Homebrew or MacPorts
  # OpenSSL 3 is never considered: its lib dir is off dyld's search path and SIP
  # strips DYLD_* under `nim c -r`. Linking records the library's full path in
  # the binary, so it needs no environment at run time.
  #
  # Only when the library is actually there, though: a machine without one
  # keeps the by-name load it always had (and a warning that says what to
  # install) rather than failing at link time. The probe order is an explicit
  # NAVI_OPENSSL_DIR, then the package manager prefix for this CPU (Homebrew
  # puts arm64 under /opt/homebrew and x86_64 under /usr/local, so a Rosetta
  # toolchain finds its own architecture), then MacPorts.
  const sslLib = "libssl.3.dylib"
  var candidates: seq[string]
  let explicit = getEnv("NAVI_OPENSSL_DIR")
  if explicit.len > 0: candidates.add explicit
  when defined(arm64):
    candidates.add "/opt/homebrew/opt/openssl@3/lib"
  else:
    candidates.add "/usr/local/opt/openssl@3/lib"
  candidates.add "/opt/local/lib"
  var found = ""
  for dir in candidates:
    if fileExists(dir & "/" & sslLib):
      found = dir
      break
  if found.len > 0:
    # sslVersion=3 picks the OpenSSL 3 entry-point names (SSL_get1_peer_certificate).
    switch("define", "sslVersion=3")
    switch("dynlibOverride", "ssl")
    switch("dynlibOverride", "crypto")
    switch("passL", "-L" & found & " -lssl -lcrypto")
  else:
    # NimScript's `warning` is the per-warning on/off switch, so this is an echo.
    echo "config.nims: Warning: navi: no OpenSSL 3 found (" &
         candidates.join(", ") & "); https will bind Apple's LibreSSL by name. " &
         "Install it (brew install openssl@3, or sudo port install openssl3) " &
         "or set NAVI_OPENSSL_DIR to the directory holding " & sslLib & "."
