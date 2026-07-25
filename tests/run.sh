#!/usr/bin/env bash
# Run the navi test suite directly, WITHOUT `nimble test`.
#
# `nimble` does not propagate a task's exit code (nim-lang/nimble#1802): on
# nimble v0.22.2 a failing test still makes `nimble test` exit 0, so CI reports
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

cd "$(dirname "$0")/.."   # repo root, so tests/config.nims resolves the src path

# Discover the suites so the list never drifts as tests are added. `nullglob`
# makes a no-match expand to nothing (not the literal pattern); we then fail if
# empty, so a broken glob can't pass CI with zero suites run. Bash sorts the
# glob, so the order is deterministic (the suites are independent).
shopt -s nullglob
suites=(tests/test_*.nim)
shopt -u nullglob
if [ ${#suites[@]} -eq 0 ]; then
  echo "error: no test suites found (tests/test_*.nim)" >&2
  exit 1
fi

for f in "${suites[@]}"; do
  echo "== $(basename "$f" .nim) =="
  nim c -r "${opts[@]}" "$f"
done
