#!/usr/bin/env bash
# Native leak check for one (MODE, TARGET, SCENARIO) cell.
#   MODE     = valgrind | sanitize
#   TARGET   = sync | asyncdispatch | chronos
#   SCENARIO = http1 http1s http2s streamup streamupc streamdown streamdownc sse ws wss
# Builds the runner once for TARGET, then runs SCENARIO for NAVI_LEAK_ITERS
# iterations under Valgrind (memcheck + --track-fds for descriptor leaks) or
# ASan/UBSan. A definite/indirect leak, a surviving fd, or a sanitizer error fails
# the run. navi-owned leaks must be fixed, not suppressed (see navi.supp).
set -euo pipefail

MODE="${1:?usage: run.sh <valgrind|sanitize> <target> <scenario>}"
TARGET="${2:?missing target}"
SCENARIO="${3:?missing scenario}"

LEAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$LEAK_DIR/../.." && pwd)"
BIN="/tmp/navileak/runner-$TARGET"

case "$TARGET" in
  sync)          define="" ;;
  asyncdispatch) define="-d:useAsync" ;;
  chronos)       define="-d:useChronos" ;;
  *) echo "unknown target: $TARGET" >&2; exit 2 ;;
esac

opts=(c --path:"$ROOT/src" -d:ssl --mm:orc -g --debugger:native
      --hints:off --stackTrace:on --lineTrace:on -o:"$BIN")

if [ "$MODE" = "sanitize" ]; then
  # ASan/UBSan via the default gcc, matching tests/run.sh. Leak detection is off
  # here: Valgrind owns leaks; ASan owns memory-safety + UB.
  san="-fsanitize=address,undefined -fno-omit-frame-pointer"
  opts+=(-d:useMalloc "--passC:$san" "--passL:-fsanitize=address,undefined")
elif [ "$MODE" = "valgrind" ]; then
  opts+=(-d:useMalloc)
else
  echo "unknown mode: $MODE" >&2; exit 2
fi

echo ">>> building runner ($TARGET) for $MODE / $SCENARIO"
nim "${opts[@]}" $define "$LEAK_DIR/runner.nim"

# shellcheck source=tests/leakcheck/run-server.sh
source "$LEAK_DIR/run-server.sh"
start_servers

export NAVI_LEAK_SCENARIO="$SCENARIO"
export NAVI_LEAK_ITERS="${NAVI_LEAK_ITERS:-30}"

if [ "$MODE" = "valgrind" ]; then
  log="/tmp/navileak/valgrind-$TARGET-$SCENARIO.log"
  # --track-fds=yes makes a descriptor still open at exit a CoreError, and
  # --errors-for-leak-kinds makes a definite/indirect leak an error, so
  # --error-exitcode=1 fails the run on either. navi.supp suppresses only the
  # event loop's own control fds (epoll/eventfd/timer/signal); a leaked navi
  # socket or cert file does not match and still fails.
  set +e
  valgrind \
    --leak-check=full --show-leak-kinds=definite,indirect \
    --errors-for-leak-kinds=definite,indirect \
    --track-fds=yes --error-exitcode=1 \
    --suppressions="$LEAK_DIR/navi.supp" \
    "$BIN" 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  set -e
  grep -E "FILE DESCRIPTORS: [0-9]+ open" "$log" | tail -1 || true
  if [ "$rc" -eq 0 ]; then
    echo "PASS: valgrind $TARGET $SCENARIO clean"
  else
    echo "FAIL: valgrind $TARGET $SCENARIO (leak, fd, or memory error above)" >&2
  fi
  exit "$rc"
else
  export ASAN_OPTIONS="detect_leaks=0:abort_on_error=1:print_stacktrace=1"
  export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
  "$BIN"
  echo "PASS: sanitize $TARGET $SCENARIO clean"
fi
