## What the connection pool stores: a backend transport plus, for HTTP/2, the
## persistent connection state that must be reused alongside it.

import ../proto/h2/conn

type
  PooledConn*[C] = object
    transport*: C
    h2*: H2Conn   ## nil for HTTP/1.1; the shared h2 connection for HTTP/2
