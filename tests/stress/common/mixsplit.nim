## The worker split for the mixed workload, as a pure function of the worker
## total. Its own module (no imports, no navi, no std/os) so the native clients
## and `nim js` share one rule and it can be checked on its own.

proc mixSplit*(total: int): tuple[req, ws, sse, up, down: int] =
  ## Split `total` workers across the five slices: requests 40%, ws 20%, sse 20%,
  ## streamUpload 10%, streamDownload 10%. Each share is rounded to the nearest
  ## worker, every slice gets at least one, and every worker left over goes to
  ## requests -- so the percentages are a floor on the four small slices and
  ## requests absorbs the rounding. At the harness defaults (3 clients x 8
  ## concurrency = 24) that is req=10 ws=5 sse=5 up=2 down=2.
  ##
  ## Below five workers the "at least one per slice" rule wins over the total and
  ## the mix runs five workers: a cell that silently dropped a slice would pass
  ## that slice's zero-work check vacuously, which is worse than one extra
  ## worker. The resolved split is printed at cell start, so it is never implicit.
  result.ws = max(1, (total * 20 + 50) div 100)
  result.sse = max(1, (total * 20 + 50) div 100)
  result.up = max(1, (total * 10 + 50) div 100)
  result.down = max(1, (total * 10 + 50) div 100)
  result.req = max(1, total - result.ws - result.sse - result.up - result.down)
