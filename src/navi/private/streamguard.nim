## A tiny guard that closes a streaming handle's connection if the handle is
## dropped without being drained or closed.
##
## The stream handles (`StreamResponse`) carry many managed fields (the header
## snapshot with its headers/strings, the parser, the origin key). A custom
## `=destroy` on the handle itself would suppress the compiler's field destruction
## and leak them. So instead the handle holds one of these guards as a plain field:
## the compiler destroys the handle's own fields normally, and only this minimal
## guard has a destructor. The guard runs a backend-supplied `dispose` closure (a
## synchronous, best-effort close/reset) if it is still `armed` when destroyed;
## `drain`/`close` `disarm` it once they have taken over the teardown themselves.

type
  StreamGuardObj = object
    armed: bool
    dispose: proc() {.gcsafe, raises: [].}
  StreamGuard* = ref StreamGuardObj

proc `=destroy`(g: var StreamGuardObj) =
  if g.armed and g.dispose != nil: g.dispose()
  # A custom `=destroy` suppresses the compiler's field destruction, so free the
  # closure environment (which captured the connection) explicitly.
  `=destroy`(g.dispose)

proc newStreamGuard*(dispose: proc() {.gcsafe, raises: [].}): StreamGuard =
  ## Arm a guard with the teardown to run if the handle is dropped undrained.
  StreamGuard(armed: true, dispose: dispose)

proc disarm*(g: StreamGuard) =
  ## The connection was handed back to the pool or the stream finished cleanly:
  ## the guard must not close it. Used by backends whose `drain`/`close` do their
  ## own (awaitable) teardown.
  if g != nil: g.armed = false

proc closeNow*(g: StreamGuard) =
  ## Run the teardown now (if still armed) and disarm, so the destructor will not
  ## run it again. Used by the sync backend, whose disposal is synchronous.
  if g != nil and g.armed:
    if g.dispose != nil: g.dispose()
    g.armed = false
