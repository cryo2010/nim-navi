#!/usr/bin/env bash
# Packaging check: build a consumer against the INSTALLED navi package (#465).
#
# `installExt` in navi.nimble decides which non-.nim files nimble copies into an
# installed package. A file the build needs but that list omits stays invisible
# here -- every build in this repo compiles --path:src, where the whole checkout
# is present -- and is missing for everyone who installs navi. That is exactly
# how src/navi/backend/h3client.h went missing: installExt shipped h3client.cpp
# without the header it includes, so any -d:naviHttp3 build against the
# installed package died in the C++ step with
#   h3client.cpp:30:10: fatal error: h3client.h: No such file or directory
# while the same program built with --path:<checkout>/src was fine (#465).
#
# So this installs the WORKING TREE as a nimble package into a throwaway nimble
# dir, asserts every non-.nim build input under srcDir arrived, and then compiles
# and runs a hello world against that install -- never --path:src.
#
#   bash tests/packaging/installed_build.sh          # install + manifest + -d:ssl consumers
#   bash tests/packaging/installed_build.sh --h3     # the same, plus -d:naviHttp3
#
# --h3 is the leg that reaches the C++ compile of h3client.cpp, so it needs the
# HTTP/3 toolchain (ngtcp2 + nghttp3 + OpenSSL >= 3.5 on PKG_CONFIG_PATH) and a
# C++20 compiler. The h3 interop image already has all of it:
#   docker build -f tests/interop/http3/Dockerfile -t navi-h3 .
#   docker run --rm --entrypoint bash navi-h3 tests/packaging/installed_build.sh --h3
set -euo pipefail

h3=0
case "${1:-}" in
  "")   ;;
  --h3) h3=1 ;;
  *)    echo "usage: $(basename "$0") [--h3]" >&2; exit 2 ;;
esac

# pwd -P, not pwd: the "never read the checkout" grep below compares $root
# against paths nim prints with --listFullPaths, and those are physical (nim
# resolves them), so a checkout reached through a symlink -- /tmp -> /private/tmp
# on macOS, a symlinked CI workspace -- would give a logical $root that never
# matches and silently turn that assertion off.
root="$(cd "$(dirname "$0")/../.." && pwd -P)"

# nimble ships exactly srcDir, so read it from navi.nimble rather than assuming
# "src": the manifest check below then asks the same question nimble answers.
srcdir="$(sed -nE 's@^srcDir[[:space:]]*=[[:space:]]*"([^"]+)".*@\1@p' "$root/navi.nimble" | head -1)"
[ -n "$srcdir" ] || { echo "FAIL: could not read srcDir from $root/navi.nimble" >&2; exit 1; }

work="$(mktemp -d)"
cleanup() { cd /; rm -rf "$work"; }
trap cleanup EXIT

stage="$work/pkg"
nb="$work/nimble"
consumer="$work/consumer"
mkdir -p "$stage" "$nb" "$consumer"

# Install from a copy rather than from $root: nimble 0.22 drops its own scratch
# (tagged_versions.json, a githubcom_* clone of each dependency) into the CWD,
# and a check must not dirty the working tree it is checking. Dropping .git also
# makes "the working tree" literal -- no chance of nimble packaging HEAD instead
# of the edit under test.
tar -c --exclude=.git -C "$root" . | tar -x -C "$stage"

echo "== installing the working tree as a nimble package into $nb =="
# No path argument. `nimble install .` (and the file:// form) puts a package
# literally named "." into the dependency graph on nimble 0.22 and then cannot
# solve it ("Dependency . not found in the graph"); with no argument nimble
# installs the package in the CWD, which is what we want and also the form that
# copies installExt files.
#
# --useSystemNim keeps nimble 0.22 from reading navi's `requires "nim >= ..."`
# as one more package to solve and downloading a whole Nim toolchain into this
# empty scratch nimbleDir. It needs --nim pointed at the real toolchain binary:
# on a choosenim host the `nim` on PATH is a thin shim with no lib/ beside it and
# nimble rejects it ("No system nim found"). The compiler can name its own
# toolchain, so derive it from that and fall back to a plain install if either
# the probe or the install does not take. Downloading a toolchain is slow, not
# wrong, so the fallback must not fail the check.
nimbin=""
if libpath="$(nim --hints:off --eval:'import std/compilesettings
echo querySetting(SingleValueSetting.libPath)' 2>/dev/null | tail -1)"; then
  case "$libpath" in
    /*/lib) [ -x "${libpath%/lib}/bin/nim" ] && nimbin="${libpath%/lib}/bin/nim" ;;
  esac
fi

# nimble 0.22 exits 0 even when it gave up ("No system nim found" is printed as
# an Error and still returns 0), so whether an install worked is decided by
# looking for the package it should have produced, never by the exit status.
installed_navi() {
  local found
  shopt -s nullglob
  found=( "$nb"/pkgs2/navi-* )
  shopt -u nullglob
  [ "${#found[@]}" -eq 1 ]
}

if [ -n "$nimbin" ]; then
  ( cd "$stage" && nimble install -y --useSystemNim --nim:"$nimbin" --nimbleDir:"$nb" ) || true
fi
if installed_navi; then
  echo "   installed with the system Nim at $nimbin (no toolchain download)"
else
  [ -z "$nimbin" ] || echo "   --useSystemNim did not take; retrying without it"
  rm -rf "$nb"
  mkdir -p "$nb"
  ( cd "$stage" && nimble install -y --nimbleDir:"$nb" )
fi

shopt -s nullglob
installed=( "$nb"/pkgs2/navi-* )
shopt -u nullglob
if [ "${#installed[@]}" -ne 1 ]; then
  echo "FAIL: expected exactly one installed navi in $nb/pkgs2, found ${#installed[@]}" >&2
  exit 1
fi
pkg="${installed[0]}"
echo "   installed package: $pkg"

# --- 1. every non-.nim build input under srcDir must have arrived ------------
#
# Derived from the tree instead of a hand-written list, so a future .cpp/.h
# split, a second header or a staticRead'd data file cannot quietly go missing
# the way h3client.h did:
#   (a) what a .nim pragma names. Every spelling Nim accepts for the file
#       argument counts: {.compile: "x.c".}, {.compile("x.c").},
#       {.compile: ("x.cpp", "-flags").}, staticRead("x"), staticRead "x",
#       slurp("x"), slurp "x", a compile pragma that follows another pragma in
#       the same {. .}, and any of those wrapped across lines.
#   (b) what those C/C++ sources #include with quotes, transitively
#   (c) every file under srcDir carrying a build-input extension, referenced yet
#       or not (a header a later commit starts including is already shipped)
#
# (a) is a text scan, so it errs in one direction on purpose: whole-line `#`
# comments are dropped, but a pragma or staticRead written out in a trailing
# comment is still collected and then has to name a real shipped file. It cannot
# miss a build input, which is the failure mode that matters here.
need="$work/need"
: > "$need"

# Collapse `.` and `..` components. This has to happen before a path is
# recorded: a `..` left inside a reference rides along into $rel below, still
# matches the srcDir case and gets reported as a missing install file instead of
# the build input outside srcDir that it is. The paths here are always absolute
# (a pragma or #include reference is resolved against the directory of the file
# naming it), so looping over "/<name>/../" is enough.
norm() {
  printf '%s\n' "$1" | sed -e 's@//*@/@g' -e 's@/\./@/@g' \
    -e ':a' -e 's@/[^/][^/]*/\.\./@/@' -e 'ta'
}

while IFS= read -r nimfile; do
  dir="$(dirname "$nimfile")"
  # Drop whole-line comments, flatten the file so a pragma or call split across
  # lines still matches, then keep the first quoted argument of each hit (the
  # tuple form's second element is compiler flags, not a file). grep exits 1 on
  # a file with no pragma at all, which pipefail would turn into a failure.
  sed -e 's@^[[:space:]]*#.*@@' "$nimfile" | tr '\n' ' ' |
    { grep -oE '\{\.[^".{}]*compile[[:space:]]*[:(][[:space:]]*\(?[[:space:]]*"[^"]+"|(staticRead|slurp)[[:space:]]*\(?[[:space:]]*"[^"]+"' || true; } |
    sed -E 's@.*"([^"]+)"$@\1@' |
    while IFS= read -r ref; do
      case "$ref" in /*) norm "$ref" ;; *) norm "$dir/$ref" ;; esac
    done
done < <(find "$root/$srcdir" -type f -name '*.nim') >> "$need"

sort -u "$need" -o "$need"
scan="$work/scan"
cp "$need" "$scan"
while [ -s "$scan" ]; do
  found="$work/found"
  : > "$found"
  while IFS= read -r f; do
    case "$f" in *.h|*.hh|*.hpp|*.hxx|*.c|*.cc|*.cpp|*.cxx) ;; *) continue ;; esac
    [ -f "$f" ] || continue
    d="$(dirname "$f")"
    sed -nE 's@^[[:space:]]*#[[:space:]]*include[[:space:]]*"([^"]+)".*@\1@p' "$f" |
      while IFS= read -r inc; do
        case "$inc" in /*) norm "$inc" ;; *) norm "$d/$inc" ;; esac
      done >> "$found"
  done < "$scan"
  sort -u "$found" -o "$found"
  # Only follow what is new, or a header pair that includes each other spins.
  comm -13 "$need" "$found" > "$scan"
  sort -u "$need" "$scan" -o "$need"
done

find "$root/$srcdir" -type f \( -name '*.h' -o -name '*.hh' -o -name '*.hpp' \
  -o -name '*.hxx' -o -name '*.c' -o -name '*.cc' -o -name '*.cpp' \
  -o -name '*.cxx' -o -name '*.inc' -o -name '*.S' -o -name '*.s' \) >> "$need"
sort -u "$need" -o "$need"

echo "== checking every non-.nim build input under $srcdir/ is in the install =="
bad=0
while IFS= read -r f; do
  [ -n "$f" ] || continue
  rel="${f#"$root"/}"
  case "$rel" in
    "$srcdir"/*) ;;
    *) echo "   FAIL  $rel  (a build input outside $srcdir/; nimble only ships srcDir)"
       bad=1; continue ;;
  esac
  if [ -f "$pkg/${rel#"$srcdir"/}" ]; then
    echo "   ok    $rel"
  else
    echo "   FAIL  $rel  (not in the installed package: add its extension to installExt)"
    bad=1
  fi
done < "$need"
if [ "$bad" -ne 0 ]; then
  echo "FAIL: the installed package is missing a file the build needs (see above); fix installExt/installFiles in navi.nimble" >&2
  exit 1
fi

# --- 2. compile (and run) a consumer against the install --------------------
#
# The consumer lives outside the checkout on purpose: nim.cfg at the repo root
# carries --path:"src" and nim reads every nim.cfg up the directory tree, so a
# consumer built inside the tree would compile the checkout and prove nothing.
build() {   # build <label> <entry module> [extra nim flags...]
            # <label> names the generated consumer module, so it has to be a
            # valid Nim identifier.
  local label="$1" entry="$2"
  shift 2
  local src="$consumer/$label.nim" log="$work/$label.log"
  cat > "$src" <<NIM
import $entry
let api = newNavi(initNaviConfig())
echo api.config.http
NIM
  echo "-- $label: import $entry (nim c -d:ssl $*)"
  # --nimblePath points nim at the throwaway install the way the stock config
  # points it at ~/.nimble. --listFullPaths + --processing:filenames make the
  # compile name every module file it reads, which is what the two greps below
  # turn into proof that it read the install and not the checkout.
  if ! ( cd "$consumer" && nim c --mm:orc --threads:on -d:ssl "$@" \
           --nimblePath:"$nb/pkgs2" --nimcache:"$work/cache/$label" \
           --listFullPaths --processing:filenames \
           --out:"$consumer/$label.bin" "$src" ) > "$log" 2>&1; then
    if [ "$entry" = "navi/chronos" ] && grep -Fq "cannot open file: chronos" "$log"; then
      echo "   skipped: the chronos package is not installed here"
      return 0
    fi
    # --processing:filenames makes these logs thousands of lines long; the
    # compiler's own error is at the end of it.
    tail -40 "$log" | sed -e 's/^/   | /'
    echo "FAIL: $label did not build against the installed package (full log: $log)" >&2
    return 1
  fi
  grep -Fq "$pkg/navi/" "$log" || {
    echo "FAIL: $label never read $pkg (nothing resolved from the installed package)" >&2
    return 1
  }
  if grep -Fq "$root/$srcdir/navi/" "$log"; then
    echo "FAIL: $label read $root/$srcdir -- the checkout, not the install" >&2
    return 1
  fi
  # A run is the cheap bonus: the h3 entries resolve OpenSSL symbols at module
  # init, so even printing config.http exercises more than the compile did.
  local out
  out="$("$consumer/$label.bin")"
  [ -n "$out" ] || { echo "FAIL: $label ran but printed nothing" >&2; return 1; }
  echo "   ok: config.http = $out"
}

echo "== compiling consumers against the installed package =="
build ssl_sync navi
build ssl_async navi/asyncdispatch
build ssl_chronos navi/chronos

if [ "$h3" -eq 1 ]; then
  echo "== compiling -d:naviHttp3 consumers (this compiles h3client.cpp) =="
  command -v pkg-config >/dev/null || { echo "FAIL: pkg-config is needed for -d:naviHttp3" >&2; exit 1; }
  pkg-config --exists libngtcp2 libngtcp2_crypto_ossl libnghttp3 libssl || {
    echo "FAIL: --h3 needs ngtcp2 + nghttp3 + OpenSSL >= 3.5 on PKG_CONFIG_PATH" >&2
    exit 1
  }
  build h3_sync navi -d:naviHttp3
  build h3_async navi/asyncdispatch -d:naviHttp3
  build h3_chronos navi/chronos -d:naviHttp3
fi

echo "== packaging check: all consumers built against the installed package =="
