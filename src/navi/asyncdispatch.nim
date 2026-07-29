## navi — asyncdispatch entry point.
##
##   import navi/asyncdispatch
##   let api = initNavi()
##   let res = await api.get("http://example.com")
##
## Under `nim js` this transparently falls back to navi/js (fetch), since
## std/asyncdispatch has no JavaScript backend. Library code that only writes
## `{.async.}` procs and `await`s ports unchanged; an application's entry point is
## the one target-specific line -- `waitFor main()` natively vs `discard main()`
## under js, because JS cannot block. See navi/js for the js capability notes.
when defined(js):
  import navi/js
  export js
else:
  include navi/private/asyncdispatch_impl
