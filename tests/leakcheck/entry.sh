#!/usr/bin/env bash
# Docker entrypoint dispatch for the leak-check matrix.
#   js <scenario>                      -> run-js.sh
#   <valgrind|sanitize> <target> <sc>  -> run.sh
set -euo pipefail
LEAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "${1:-}" = "js" ]; then
  exec bash "$LEAK_DIR/run-js.sh" "${2:?missing scenario}"
fi
exec bash "$LEAK_DIR/run.sh" "$@"
