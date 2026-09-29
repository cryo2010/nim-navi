#!/usr/bin/env bash
# Readiness waits on descriptors above FD_SETSIZE (issue #429), sync backend.
#
# Starts an OpenSSL HTTPS server signed by a throwaway CA, then runs a client
# that burns ~1100 descriptors with dup(2) so its socket lands above 1024 and
# issues a request with a read timeout armed. With the pre-fix select()-based
# wait this aborted the process (fortified glibc) or raised a bogus read
# timeout; with poll() it just works.
#
# POSIX only (Windows fd_sets are counted arrays, so the ceiling never existed
# there). The process needs room above 1024 descriptors: the script raises its
# own soft limit and the client raises it again via setrlimit, but the HARD
# limit has to allow it. Under Docker pass --ulimit nofile=4096:4096 if the
# image's default hard limit is lower.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/interop/_win.sh"

case "$(uname -s)" in
  Linux|Darwin|*BSD) ;;
  *) echo "highfd: POSIX only, skipping on $(uname -s)"; exit 0 ;;
esac
command -v openssl >/dev/null || { echo "openssl not found"; exit 127; }

ulimit -n 4096 2>/dev/null || true
hard="$(ulimit -Hn)"
if [ "$hard" != "unlimited" ] && [ "$hard" -lt 1300 ] 2>/dev/null; then
  echo "highfd: hard nofile limit is $hard (<1300); rerun with --ulimit nofile=4096:4096"
  exit 0
fi

work="$(mktemp -d)"
srv=""
cleanup() {
  [ -n "$srv" ] && kill "$srv" 2>/dev/null || true
  cd "$root"          # Windows cannot remove the shell's own cwd
  navi_rmtree "$work"
}
trap cleanup EXIT
cd "$work"

port=9467

navi_certgen ca.key ca.pem navi-test-CA

openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "$(navi_subj CN=127.0.0.1)" >/dev/null 2>&1
printf "subjectAltName=DNS:127.0.0.1,DNS:localhost,IP:127.0.0.1" > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
  -extfile san.ext -out server.pem >/dev/null 2>&1

# -www answers a 200 HTML page on each connection.
openssl s_server -accept 127.0.0.1:"$port" -cert server.pem -key server.key \
  -www -quiet >"$work/s_server.log" 2>&1 &
srv=$!
disown 2>/dev/null || true

navi_wait_tls "127.0.0.1:$port" -CAfile ca.pem \
  || { echo "s_server did not become ready on :$port"; cat "$work/s_server.log"; exit 1; }

export NAVI_HIGHFD_URL="https://127.0.0.1:$port"
export NAVI_HIGHFD_CA="$(navi_path "$work/ca.pem")"

echo "== high-fd readiness wait: request over a socket above FD_SETSIZE =="
# Optimized and fortified on purpose: glibc's _FORTIFY_SOURCE FD_SET bounds-checks
# the descriptor (__fdelt_chk) and aborts on an out-of-range one, which is what
# turns the old select()-based wait from silent memory corruption into a hard,
# reproducible failure. Distro gcc defaults fortify on at -O2 anyway.
fortify=""
if [ "$(uname -s)" = "Linux" ]; then
  fortify="--passC:-U_FORTIFY_SOURCE --passC:-D_FORTIFY_SOURCE=2"
fi
# shellcheck disable=SC2086
nim c -r --hints:off -d:ssl -d:release $fortify --path:"$root/src" \
  -o:"$work/highfd" "$root/tests/interop/highfd.nim"
