## One-shot OpenSSL EVP digests (MD5 / SHA-1 / SHA-256) as a *hardware*
## acceleration for the low-volume Digest-auth and WebSocket-accept hashes.
##
## navi links libcrypto for TLS, so these can route through the CPU's SHA
## extensions (SHA-NI) via libcrypto instead of the pure-Nim `checksums`
## fallback. The probe is **best-effort** and finds libcrypto two ways:
##
## - `--dynlibOverride:crypto` (the macOS default from nim.cfg, or any static
##   link): libcrypto is part of the process image, so the symbols come from
##   `loadLib()`, a dlopen of the program itself. Any platform.
## - otherwise, on Linux only, a dlopen of the standard sonames. Elsewhere the
##   by-name route is off: on macOS the unversioned `libcrypto.dylib` is an Apple
##   stub that aborts the process ("loading libcrypto in an unsafe way"), and the
##   gain (a hash per 401 / per ws handshake) never justified platform hacks.
##
## When neither applies, `evpAvailable()` is false and callers keep using
## `checksums`. This is strictly an optimization, never a new hard runtime
## dependency.

const evpProbe = compileOption("dynlibOverride", "crypto") or defined(linux)
  ## Whether this build has a way to reach libcrypto at all (see above).

when evpProbe:
  import std/dynlib

type
  MdFn = proc(): pointer {.cdecl, gcsafe, raises: [].}
  NewFn = proc(): pointer {.cdecl, gcsafe, raises: [].}
  FreeFn = proc(ctx: pointer) {.cdecl, gcsafe, raises: [].}
  InitFn = proc(ctx, typ, engine: pointer): cint {.cdecl, gcsafe, raises: [].}
  UpdateFn = proc(ctx: pointer, d: pointer, cnt: csize_t): cint
              {.cdecl, gcsafe, raises: [].}
  FinalFn = proc(ctx: pointer, md: pointer, s: ptr cuint): cint
              {.cdecl, gcsafe, raises: [].}

type
  ## Tri-state guard for the lazy FFI probe.
  ##
  ## `lsUnloaded` covers both "never attempted" and "attempted but failed": in
  ## either case the next call re-runs the probe. Only `lsLoaded` (all symbols
  ## resolved) is a terminal, cached success. A failed probe is deliberately
  ## *not* cached as terminal so a later call can retry -- the previous code
  ## flipped a `loaded` flag unconditionally, which pinned a one-off resolution
  ## failure as permanent and never re-probed.
  LoadState = enum
    lsUnloaded   ## not yet loaded, or the last probe failed -- retry on next call
    lsLoaded     ## all EVP symbols resolved once; cached, never re-probed

var
  state {.threadvar.}: LoadState     ## per-thread: each thread resolves its own copy,
  ctxNew {.threadvar.}: NewFn        ## so first-use resolution never races (works under
  ctxFree {.threadvar.}: FreeFn      ## plain orc, one navi client per thread)
  md5Md {.threadvar.}: MdFn
  sha1Md {.threadvar.}: MdFn
  sha256Md {.threadvar.}: MdFn
  digestInit {.threadvar.}: InitFn
  digestUpdate {.threadvar.}: UpdateFn
  digestFinal {.threadvar.}: FinalFn

proc ensureLoaded() {.gcsafe.} =
  ## Resolve the EVP entry points once per thread. Idempotent after a success;
  ## on failure it leaves `state == lsUnloaded` so a subsequent call retries the
  ## probe rather than caching the failure forever.
  {.cast(gcsafe).}:
    if state == lsLoaded: return
    when evpProbe:
      var lib: LibHandle
      when compileOption("dynlibOverride", "crypto"):
        # libcrypto is linked in: resolve from the process image. A by-name
        # dlopen could miss it (macOS) or map a second copy beside it.
        lib = loadLib()
      else:
        # Standard libcrypto sonames; the first that loads wins. On Linux loadLib
        # of a missing name just returns nil (no abort), and libcrypto.so.3 is
        # standard.
        const names = ["libcrypto.so.3", "libcrypto.so.1.1", "libcrypto.so"]
        for name in names:
          lib = loadLib(name)
          if lib != nil: break
      if lib == nil: return          # stays lsUnloaded -> retried next call
      ctxNew = cast[NewFn](lib.symAddr("EVP_MD_CTX_new"))
      ctxFree = cast[FreeFn](lib.symAddr("EVP_MD_CTX_free"))
      md5Md = cast[MdFn](lib.symAddr("EVP_md5"))
      sha1Md = cast[MdFn](lib.symAddr("EVP_sha1"))
      sha256Md = cast[MdFn](lib.symAddr("EVP_sha256"))
      digestInit = cast[InitFn](lib.symAddr("EVP_DigestInit_ex"))
      digestUpdate = cast[UpdateFn](lib.symAddr("EVP_DigestUpdate"))
      digestFinal = cast[FinalFn](lib.symAddr("EVP_DigestFinal_ex"))
      if ctxNew != nil and ctxFree != nil and md5Md != nil and sha1Md != nil and
         sha256Md != nil and digestInit != nil and digestUpdate != nil and
         digestFinal != nil:
        state = lsLoaded             # cached success; never re-probed
      # else: leave lsUnloaded so a partial resolution is retried next call

proc evpAvailable*(): bool {.gcsafe.} =
  ## True once libcrypto's EVP digest entry points are resolved (a linked
  ## libcrypto anywhere, or the system soname on Linux).
  ## Cheap after the first successful call. Callers gate on this and fall back
  ## to `checksums`; a probe that has never succeeded is retried each call.
  ensureLoaded()
  {.cast(gcsafe).}: state == lsLoaded

proc evpDigest(md: MdFn, s: string): string {.gcsafe.} =
  {.cast(gcsafe).}:
    let ctx = ctxNew()
    if ctx.isNil: raise newException(IOError, "navi: EVP_MD_CTX_new failed")
    var buf: array[64, uint8]       # holds any supported digest (SHA-512 = 64B)
    var n: cuint
    try:
      if digestInit(ctx, md(), nil) != 1:
        raise newException(IOError, "navi: EVP_DigestInit_ex failed")
      if s.len > 0 and digestUpdate(ctx, unsafeAddr s[0], csize_t(s.len)) != 1:
        raise newException(IOError, "navi: EVP_DigestUpdate failed")
      if digestFinal(ctx, addr buf[0], addr n) != 1:
        raise newException(IOError, "navi: EVP_DigestFinal_ex failed")
    finally:
      ctxFree(ctx)
    result = newString(int(n))
    for i in 0 ..< int(n): result[i] = char(buf[i])

const hexDigits = "0123456789abcdef"
proc toHex(raw: string): string =
  result = newStringOfCap(raw.len * 2)
  for c in raw:
    let b = uint8(c)
    result.add hexDigits[int(b shr 4)]
    result.add hexDigits[int(b and 0x0f)]

proc evpMd5Hex*(s: string): string {.gcsafe.} =
  ## Lowercase-hex MD5 of `s` (Digest auth). Requires `evpAvailable()`.
  {.cast(gcsafe).}: toHex(evpDigest(md5Md, s))
proc evpSha256Hex*(s: string): string {.gcsafe.} =
  ## Lowercase-hex SHA-256 of `s` (Digest auth). Requires `evpAvailable()`.
  {.cast(gcsafe).}: toHex(evpDigest(sha256Md, s))
proc evpSha1Raw*(s: string): string {.gcsafe.} =
  ## Raw 20-byte SHA-1 of `s` (WebSocket accept). Requires `evpAvailable()`.
  {.cast(gcsafe).}: evpDigest(sha1Md, s)
