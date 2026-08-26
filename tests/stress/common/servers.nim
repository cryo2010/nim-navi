## Round-robin over the N server instances the harness stands up. Instance i
## listens on `basePort + i`; requests are spread across them so no single origin
## is the bottleneck and the client's per-origin pools/muxes are all exercised.
## Backend-agnostic (no navi import).

import ./config

type ServerPool* = object
  bases: seq[string]     ## e.g. @["https://127.0.0.1:9443", ...]
  next: int

proc initServerPool*(c: Config): ServerPool =
  ## Build "https://host:port" bases for the N instances.
  for i in 0 ..< c.servers:
    result.bases.add "https://" & c.host & ":" & $(c.basePort + i)

proc pick*(p: var ServerPool): string =
  ## The next base URL, round-robin.
  result = p.bases[p.next]
  p.next = (p.next + 1) mod p.bases.len

proc all*(p: ServerPool): seq[string] = p.bases
