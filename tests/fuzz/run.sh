#!/usr/bin/env bash
# Build and run a navi sans-io fuzz target.
#
#   run.sh <target> [seconds]   coverage-guided libFuzzer run (needs clang + the
#                               fuzzer runtime; default 30s)
#   run.sh <target> replay      portable standalone replay of tests/fuzz/seeds/
#                               <target> under ASan/UBSan (PR CI, any compiler)
#
# <target> is one of: hpack h1 frame huffman
set -euo pipefail

target="${1:?usage: run.sh <hpack|h1|frame|huffman> [seconds|replay]}"
mode="${2:-30}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
src="$root/tests/fuzz/fuzz_${target}.nim"
seeds="$root/tests/fuzz/seeds/$target"
common="--mm:orc -d:useMalloc --panics:on --hints:off --path:$root/src"

export ASAN_OPTIONS="detect_leaks=0:abort_on_error=1"
export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"

if [ "$mode" = "replay" ]; then
  # Portable: any C compiler with ASan; no libFuzzer runtime required.
  bin="$(mktemp -d)/replay_${target}"
  nim c $common \
    --passc:"-fsanitize=address,undefined -fno-sanitize=function -g -fno-omit-frame-pointer" \
    --passl:"-fsanitize=address,undefined" \
    -o:"$bin" "$src"
  "$bin" "$seeds"
  echo "replay ok: $target"
else
  corpus="$root/tests/fuzz/corpus/$target"
  mkdir -p "$corpus"
  bin="$(mktemp -d)/fuzz_${target}"
  nim c --cc:clang $common --noMain:on -d:libfuzzer \
    --passc:"-fsanitize=fuzzer,address,undefined -fno-sanitize=function -g -fno-omit-frame-pointer" \
    --passl:"-fsanitize=fuzzer,address,undefined" \
    -o:"$bin" "$src"
  # Persist the growing corpus in the first dir; seed from the committed set.
  "$bin" -max_total_time="$mode" -timeout=10 -print_final_stats=1 "$corpus" "$seeds"
fi
