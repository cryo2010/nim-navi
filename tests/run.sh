#!/usr/bin/env bash
# Run the navi test suite directly, WITHOUT `nimble test`.
#
# `nimble` does not reliably propagate a task's exit code (nim-lang/nimble#1802):
# on stable nimble a failing task can even exit 0, so `nimble test` in CI reports
# green on red. Running `nim c -r` per suite here, under `set -e`, makes a failing
# suite propagate its non-zero exit code so CI actually fails.
#
# Env (matching the nimble `test` task, which delegates to this script):
#   NAVI_MM=orc|arc     memory manager (default orc)
#   NAVI_SANITIZE=1     build the suite under AddressSanitizer + UBSan
set -euo pipefail

mm="${NAVI_MM:-orc}"
opts=(--hints:off "--mm:$mm")

if [ -n "${NAVI_SANITIZE:-}" ]; then
  # -d:useMalloc routes Nim allocations through malloc so ASan can see them; -g
  # and frame pointers give symbolized reports.
  san="-fsanitize=address,undefined -fno-omit-frame-pointer -g"
  opts+=(-d:useMalloc "--passC:$san" "--passL:-fsanitize=address,undefined")
fi

suites=(
  test_h1 test_h2_frame test_h2_hpack test_h2_hpack_corpus test_h2_huffman
  test_h2_conn test_cookies test_digest test_entries test_stream_decompress
  test_ws test_ws_async test_async test_chronos test_ws_chronos
)

cd "$(dirname "$0")/.."   # repo root, so tests/config.nims resolves the src path
for s in "${suites[@]}"; do
  echo "== $s =="
  nim c -r "${opts[@]}" "tests/$s.nim"
done
