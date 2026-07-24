## Request cancellation.
##
## A `CancelToken` is created by the caller, passed to a request, and tripped by
## `cancel()`. On the async backends (asyncdispatch/chronos/js) tripping it
## aborts the in-flight request; on the sync backend it is cooperative -- checked
## at request checkpoints, it cannot interrupt a socket read already blocked in a
## syscall (use `timeout` for that). One token may guard several requests.

type
  RequestCancelledError* = object of CatchableError
    ## Raised by a request whose `CancelToken` was cancelled.

  CancelToken* = ref object
    fired: bool
    hook: proc() {.closure, gcsafe, raises: [].}  ## async wakeup; nil on the sync backend

proc newCancelToken*(): CancelToken = CancelToken()

proc cancel*(t: CancelToken) =
  ## Trip the token. Idempotent; safe to call from a middleware, a timer, or
  ## (for the async backends) another task in the same event loop.
  if t == nil or t.fired: return
  t.fired = true
  let h = t.hook
  if h != nil: h()

proc cancelled*(t: CancelToken): bool =
  ## Whether the token has been tripped.
  t != nil and t.fired

proc throwIfCancelled*(t: CancelToken) =
  ## Cooperative checkpoint: raise `RequestCancelledError` if the token is tripped.
  if t != nil and t.fired:
    raise newException(RequestCancelledError, "navi: request cancelled")

proc armHook*(t: CancelToken, h: proc() {.closure, gcsafe, raises: [].}) =
  ## Backend-internal: register the wakeup that aborts an in-flight request. If
  ## the token is already tripped, fires it at once so no cancellation is missed.
  if t == nil: return
  t.hook = h
  if t.fired and h != nil: h()

proc disarmHook*(t: CancelToken) =
  ## Backend-internal: drop the wakeup once the request has settled, so the token
  ## holds no reference to the finished request's future.
  if t != nil: t.hook = nil
