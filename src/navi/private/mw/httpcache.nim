## Backend-agnostic HTTP response cache core (an RFC 9111 subset for a private
## client cache). No I/O, no async, no `NaviContext` -- pure decisions over
## `Request`/`Response` so the sync and async middleware factories share it.
##
## Scope: caches safe methods (GET/HEAD) with cacheable statuses; computes
## freshness from `Cache-Control: max-age` / `Expires` (honoring `no-store`,
## `no-cache`, `private`); revalidates stale entries with `If-None-Match` /
## `If-Modified-Since` and refreshes them on a 304. `Vary` is honored by keying on
## the named request headers (a `Vary: *` response is never stored). Not a shared
## proxy cache: `s-maxage` and request-directive nuances beyond `no-store` are out
## of scope.

import std/[times, tables, strutils]
import ../../core/[request, response, headers, url]

type
  CacheEntry = object
    status: int
    reason, httpVersion: string
    headers: Headers
    body: string
    storedAt: Time          ## when we cached (for age)
    lifetime: int           ## freshness lifetime in seconds (0 = must revalidate)
    noCache: bool           ## response `Cache-Control: no-cache`: always revalidate
    etag: string            ## validator for If-None-Match
    lastModified: string    ## validator for If-Modified-Since
    vary: seq[string]       ## request header names this entry varies on
    varyKey: string         ## the values of those headers at store time

  CacheStore* = ref object
    ## Per-client cache, captured by the `cache()` middleware factory. In-memory;
    ## not persisted. Keyed by "VERB url"; a stored entry remembers its `Vary`.
    entries: Table[string, CacheEntry]

  Freshness* = enum fMiss, fFresh, fStale
  Lookup* = object
    ## Result of `lookup`: whether a usable/validatable entry exists.
    case kind*: Freshness
    of fFresh, fStale:
      entry: CacheEntry
    of fMiss: discard

proc newCacheStore*(): CacheStore = CacheStore(entries: initTable[string, CacheEntry]())

const cacheableStatuses = [200, 203, 204, 300, 301, 308, 404, 410]

# --- Cache-Control / header parsing ------------------------------------------

type CacheControl = object
  noStore: bool
  noCache: bool
  private: bool
  maxAge: int      ## -1 when unset

proc parseCacheControl(h: Headers): CacheControl =
  result.maxAge = -1
  for raw in h.getAll("cache-control"):
    for part in raw.split(','):
      let d = part.strip
      if d.len == 0: continue
      let eq = d.find('=')
      let name = (if eq < 0: d else: d[0 ..< eq]).toLowerAscii
      let val = (if eq < 0: "" else: d[eq + 1 .. ^1].strip(chars = {'"', ' '}))
      case name
      of "no-store": result.noStore = true
      of "no-cache": result.noCache = true
      of "private": result.private = true
      of "max-age":
        try: result.maxAge = parseInt(val)
        except ValueError: discard

const httpDateFormats = [
  "ddd, dd MMM yyyy HH:mm:ss 'GMT'",
  "ddd, dd-MMM-yyyy HH:mm:ss 'GMT'",
  "dddd, dd-MMM-yy HH:mm:ss 'GMT'",
]

proc parseHttpDate(s: string): Option[Time] =
  let t = s.strip
  if t.len == 0: return
  for fmt in httpDateFormats:
    try: return some(parse(t, fmt, utc()).toTime)
    except CatchableError: discard

# --- keying / vary ------------------------------------------------------------

proc primaryKey(req: Request): string = $req.verb & " " & $req.url

proc varyNames(resp: Response): seq[string] =
  for raw in resp.headers.getAll("vary"):
    for n in raw.split(','):
      let name = n.strip.toLowerAscii
      if name.len > 0: result.add name

proc varyKeyFor(req: Request, names: seq[string]): string =
  ## The request's values for the `Vary` header names, joined -- two requests
  ## share a cached entry only when these match.
  for name in names:
    result.add name & "=" & req.headers.get(name) & "\x00"

# --- freshness ----------------------------------------------------------------

proc freshnessLifetime(resp: Response, cc: CacheControl): int =
  ## Seconds the response may be served without revalidation. `max-age` wins;
  ## otherwise `Expires - Date` when both are present; otherwise 0 (revalidate).
  if cc.maxAge >= 0: return cc.maxAge
  let expires = parseHttpDate(resp.headers.get("expires"))
  if expires.isSome:
    let dateHdr = parseHttpDate(resp.headers.get("date"))
    let base = if dateHdr.isSome: dateHdr.get else: getTime()
    let secs = (expires.get - base).inSeconds
    return max(0, secs.int)
  0

proc currentAge(entry: CacheEntry, now: Time): int =
  max(0, (now - entry.storedAt).inSeconds.int)

# --- public decisions ---------------------------------------------------------

proc isCacheableRequest*(req: Request): bool =
  ## Only safe methods, and not when the caller asked to bypass the cache.
  if req.verb notin {GET, HEAD}: return false
  parseCacheControl(req.headers).noStore == false

proc lookup*(store: CacheStore, req: Request, now = getTime()): Lookup =
  ## Find a stored entry for `req` and classify it fresh / stale / miss. A stale
  ## entry with a validator can be revalidated (see `revalidationHeaders`).
  if not isCacheableRequest(req): return Lookup(kind: fMiss)
  let key = primaryKey(req)
  if not store.entries.hasKey(key): return Lookup(kind: fMiss)
  let e = store.entries[key]
  if varyKeyFor(req, e.vary) != e.varyKey: return Lookup(kind: fMiss)
  if not e.noCache and currentAge(e, now) < e.lifetime:
    Lookup(kind: fFresh, entry: e)
  elif e.etag.len > 0 or e.lastModified.len > 0:
    Lookup(kind: fStale, entry: e)      # revalidatable
  else:
    Lookup(kind: fMiss)                 # stale and no validator: not usable

proc toResponse*(lk: Lookup): Response =
  ## The cached response to serve on a fresh hit (or after a 304 refresh).
  doAssert lk.kind in {fFresh, fStale}
  initResponse(lk.entry.status, lk.entry.reason, lk.entry.httpVersion,
               lk.entry.headers, lk.entry.body)

proc revalidationHeaders*(lk: Lookup): seq[(string, string)] =
  ## Conditional headers to add to the outgoing request for a stale entry.
  if lk.kind != fStale: return
  if lk.entry.etag.len > 0: result.add ("if-none-match", lk.entry.etag)
  if lk.entry.lastModified.len > 0:
    result.add ("if-modified-since", lk.entry.lastModified)

proc storeResponse*(store: CacheStore, req: Request, resp: Response,
                    now = getTime()) =
  ## Store `resp` for `req` if it is cacheable; otherwise evict any stale entry
  ## for the key (a `no-store`/`private`/uncacheable response invalidates it).
  let key = primaryKey(req)
  let cc = parseCacheControl(resp.headers)
  let names = varyNames(resp)
  if resp.status notin cacheableStatuses or cc.noStore or cc.private or
     "*" in names or not isCacheableRequest(req):
    store.entries.del(key)
    return
  store.entries[key] = CacheEntry(
    status: resp.status, reason: resp.reason, httpVersion: resp.httpVersion,
    headers: resp.headers, body: resp.body, storedAt: now,
    lifetime: freshnessLifetime(resp, cc), noCache: cc.noCache,
    etag: resp.headers.get("etag"), lastModified: resp.headers.get("last-modified"),
    vary: names, varyKey: varyKeyFor(req, names))

proc refreshOn304*(store: CacheStore, req: Request, resp304: Response,
                   now = getTime()): Response =
  ## Apply a 304 Not Modified: update the stored entry's freshness/validators from
  ## the 304's headers and return the stored response to serve. Falls back to the
  ## 304 itself if the entry vanished (should not happen in normal flow).
  let key = primaryKey(req)
  if not store.entries.hasKey(key):
    return resp304
  var e = store.entries[key]
  let cc = parseCacheControl(resp304.headers)
  e.storedAt = now
  e.lifetime = freshnessLifetime(resp304, cc)
  e.noCache = cc.noCache
  # A 304 may carry updated headers (e.g. a new ETag / Cache-Control); apply them.
  for (k, v) in resp304.headers.pairs:
    e.headers[k] = v
  if resp304.headers.contains("etag"): e.etag = resp304.headers.get("etag")
  store.entries[key] = e
  initResponse(e.status, e.reason, e.httpVersion, e.headers, e.body)
