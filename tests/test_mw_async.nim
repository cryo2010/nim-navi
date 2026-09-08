## Batteries middleware on the asyncdispatch client: cache end to end and
## instantiation of every async factory (proving the shared async source compiles
## and runs on this backend). The spec body is shared with test_mw_chronos.nim via
## mw_batteries_spec.nim, so a test added there runs under both backends.

import unittest
import navi/asyncdispatch
import navi/asyncdispatch/mw
import ./support                     # CacheSrv / startCache

const
  mwBackendName = "asyncdispatch"
  mwCachePort = 9130

include ./mw_batteries_spec
