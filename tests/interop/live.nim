## Live interop against real public servers and CDNs.
##
## This hits the open internet, so it is a nightly / on-demand job (see
## .github/workflows/live.yml), never a per-PR gate. It exists to catch h2/TLS
## bugs that only real, independent server stacks provoke: Google, Cloudflare,
## Fastly (api.github.com), Go's net/http2, nghttp2, Apache Traffic Server
## (Wikipedia), and a JSON request-echo (postman-echo).
##
## Tolerance model: a run must not go red because a network blipped. We split
## outcomes three ways:
##   * OK    - navi got a well-formed 2xx (and valid JSON / h2 where expected).
##   * SKIP  - a clean network condition (timeout, DNS, connection reset/refused)
##             or a server-side rejection (4xx/5xx, e.g. rate limiting). Tolerated.
##   * FAIL  - navi itself misbehaved: an h2/HPACK/frame/decompress parse error, a
##             malformed response, empty body, or unparseable JSON. Fails the run.
## Only FAIL sets a nonzero exit code.

import std/[strutils, json]
import navi

type Target = object
  name: string
  url: string
  expectJson: bool

const targets = [
  Target(name: "google",       url: "https://www.google.com/"),
  Target(name: "cloudflare",   url: "https://www.cloudflare.com/"),
  Target(name: "github-api",   url: "https://api.github.com/", expectJson: true),
  Target(name: "go-net-http2", url: "https://http2.golang.org/"),
  Target(name: "nghttp2",      url: "https://nghttp2.org/"),
  Target(name: "fastly",       url: "https://www.fastly.com/"),
  Target(name: "wikipedia",    url: "https://en.wikipedia.org/wiki/HTTP/2"),
  Target(name: "postman-echo", url: "https://postman-echo.com/get", expectJson: true),
]

# Clean network / server-side conditions we tolerate as SKIP. A navi protocol
# bug never matches these (it says "navi: http/2 ...", "hpack", "frame", etc.),
# so it falls through to FAIL.
const skipMarkers = [
  "timed out", "timeout", "connection refused", "connection reset",
  "reset by peer", "unreachable", "could not resolve", "name or service",
  "temporary failure in name resolution", "no route to host", "try again",
  "network is unreachable", "broken pipe",
  # A peer that drops the connection during/after the handshake is network noise
  # (overloaded host, WAF, idle-conn reap), not a navi parse bug.
  "closed prematurely", "connection closed", "eof",
]

proc isNetworkNoise(msg: string): bool =
  let m = msg.toLowerAscii
  for marker in skipMarkers:
    if marker in m: return true
  false

var failures: seq[string]
var skips: seq[string]

proc client(): Navi =
  var cfg = newNaviConfig()
  cfg.timeout = 20_000            # bound hangs so a stuck target can't wedge CI
  cfg.retry.limit = 0            # one attempt/target; flakes become SKIP, not
                                 # three stacked 20s timeouts
  # A descriptive UA with a Mozilla token: the GitHub API 403s a request without
  # one, and Wikimedia/Akamai WAFs block empty/generic agents. Without this most
  # targets would answer 403 (a SKIP) instead of exercising navi's h2 read path.
  cfg.headers["user-agent"] =
    "Mozilla/5.0 (compatible; navi-interop/1.0; +https://github.com/)"
  newNavi(cfg)

proc say(line: string) =
  # Flush per line: Nim block-buffers stdout to a pipe, so without this the CI
  # log (and any redirect) would show nothing until the whole run exits.
  echo line
  flushFile(stdout)

for t in targets:
  let api = client()
  try:
    let r = api.get(t.url)
    if r.body.len == 0:
      failures.add t.name & ": empty body (status " & $r.status & ")"
      say "FAIL " & t.name & ": empty body"
      continue
    if t.expectJson:
      try:
        discard parseJson(r.body)
      except JsonParsingError:
        failures.add t.name & ": response is not valid JSON"
        say "FAIL " & t.name & ": invalid JSON body"
        continue
    let h2 = if r.httpVersion == "HTTP/2": "h2" else: r.httpVersion & " (no h2!)"
    say "OK   " & t.name & " " & $r.status & " " & h2 & " " & $r.body.len & "B"
  except HttpError as e:
    # The server answered but chose a non-2xx (rate limit, geo-block, WAF). Not a
    # navi bug; record and move on.
    skips.add t.name & ": HTTP " & $e.response.status
    say "SKIP " & t.name & ": server returned " & $e.response.status
  except TimeoutError:
    skips.add t.name & ": timeout"
    say "SKIP " & t.name & ": timeout"
  except CatchableError as e:
    if isNetworkNoise(e.msg):
      skips.add t.name & ": " & e.msg
      say "SKIP " & t.name & ": " & e.msg
    else:
      failures.add t.name & ": " & e.msg
      say "FAIL " & t.name & ": " & e.msg

echo ""
echo "live interop: ", targets.len - failures.len - skips.len, " OK, ",
  skips.len, " skipped, ", failures.len, " failed"
if failures.len > 0:
  echo "failures:"
  for f in failures: echo "  - ", f
  quit(1)
