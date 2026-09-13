# Shared, backend-agnostic scaffolding for the per-backend request `guard`.
#
# Both the asyncdispatch and chronos guards bound a request by a total timeout and
# a CancelToken. Their timeout/cancel CORE differs fundamentally and MUST stay
# per-backend: asyncdispatch uses `await fut or cancelFut or sleepAsync(ms)` and
# orphans a timed-out future (it drains in the background, as it has no true
# cancellation), while chronos uses `race(...)` + `await fut.cancelAndWait()`
# (structured cancellation). Only the pieces that are IDENTICAL and carry no
# scheduling semantics live here: arming the cancel hook and the cancel-vs-timeout
# error to raise once the core has settled. See asyncdispatch_impl / chronos_impl.

import navi/core/cancel
import navi/core/response  # navi's TimeoutError

template armCancelHook*(cancel: CancelToken; cancelFut: untyped) =
  ## Register the wakeup that completes `cancelFut` when the token trips. The
  ## `{.cast(raises: []).}` matches the CancelToken hook signature (`raises: []`);
  ## `complete()` only raises if already finished, which the `finished` guard rules
  ## out. (asyncdispatch's `complete` carries a raises effect the cast silences;
  ## chronos's does not, so the cast is a harmless no-op there.)
  if cancel != nil:
    cancel.armHook(proc() {.gcsafe, raises: [].} =
      {.cast(raises: []).}:
        if not cancelFut.finished: cancelFut.complete())

proc raiseGuardExpiry*(totalMs: int; cancel: CancelToken) =
  ## The core has settled without the guarded future finishing: raise the
  ## cancel error if the token tripped, otherwise the timeout error. Identical
  ## across backends (same errors, same messages).
  if cancel != nil and cancel.cancelled:
    raise newException(RequestCancelledError, "navi: request cancelled")
  raise newException(TimeoutError, "navi: request timed out after " & $totalMs & " ms")
