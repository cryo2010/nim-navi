## navi — chronos entry point.
##
##   import navi/chronos
##   let api = newNavi()
##   let res = await api.get("http://example.com")
##
## Requires the `chronos` package on native targets. Under `nim js` this
## transparently falls back to navi/js (fetch), since chronos has no JavaScript
## backend. Library code that only writes `{.async.}` procs and `await`s ports
## unchanged; an application's entry point is the one target-specific line --
## `waitFor main()` natively vs `discard main()` under js, because JS cannot
## block. See navi/js for the js capability notes.
when defined(js):
  import navi/js
  export js
else:
  include navi/private/chronos_impl
