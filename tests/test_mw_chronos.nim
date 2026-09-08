## Batteries middleware on the chronos client: cache end to end and instantiation
## of every async factory (chronos enforces gcsafe/strict-raises, so this proves
## the shared async source is accepted there too). The spec body is shared with
## test_mw_async.nim via mw_batteries_spec.nim, so a test added there runs here too.

import unittest
import navi/chronos
import navi/chronos/mw
import ./support                     # CacheSrv / startCache

const
  mwBackendName = "chronos"
  mwCachePort = 9140

include ./mw_batteries_spec
