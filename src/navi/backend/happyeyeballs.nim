## Happy Eyeballs (RFC 8305) address helpers shared by the connecting backends.
##
## The address resolution and family interleaving are framework-agnostic (plain
## numeric-address strings), so the sync, asyncdispatch, and chronos backends
## share them; each backend then races the attempts with its own connect
## primitive (blocking select, async fd, or chronos transport).

import std/nativesockets

const heAttemptDelayMs* = 250   ## RFC 8305 Connection Attempt Delay

proc interleaveFamilies*(ips: seq[string]): seq[string] =
  ## Alternate address families (RFC 8305 §4) so a down family (e.g. every IPv6
  ## address) is not exhausted before the other is tried. Leads with the family
  ## the resolver put first.
  var v6, v4: seq[string]
  for ip in ips:
    if ':' in ip: v6.add ip else: v4.add ip
  let (a, b) = if ips.len > 0 and ':' in ips[0]: (v6, v4) else: (v4, v6)
  var i = 0
  while i < a.len or i < b.len:
    if i < a.len: result.add a[i]
    if i < b.len: result.add b[i]
    inc i

proc resolveAddrs*(host: string, port: int): seq[string] =
  ## Numeric addresses for `host` in the resolver's order (RFC 6724), interleaved
  ## by family for Happy Eyeballs. We resolve ourselves so the racer can iterate
  ## them one at a time.
  var ai = getAddrInfo(host, Port(port), AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP)
  var it = ai
  var raw: seq[string]
  while it != nil:
    raw.add getAddrString(it.ai_addr)
    it = it.ai_next
  freeAddrInfo(ai)
  interleaveFamilies(raw)
