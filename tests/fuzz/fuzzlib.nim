## Shared glue for the sans-io decoder fuzz targets, with two build modes:
##
## - standalone (default): a portable driver that replays input files (or a
##   directory of them, or stdin) through the target. Builds with any C compiler,
##   runs in PR CI and locally, and pairs with ASan/UBSan for a memory-safe
##   corpus replay.
## - libFuzzer (`-d:libfuzzer`): exports LLVMFuzzerTestOneInput for
##   coverage-guided fuzzing (nightly CI on Linux, where the runtime ships).
##
## Each target `include`s this and calls `fuzzMain:` with a body that feeds the
## injected `input` string into a decoder. Malformed input is expected to raise a
## CatchableError (the target swallows it); a Defect, out-of-bounds read, hang,
## or ASan/UBSan report is a real finding.

import std/os

template fuzzMain*(body: untyped) =
  proc runOne(fuzzInput: string) =
    let input {.inject.} = fuzzInput
    body

  when defined(libfuzzer):
    proc NimMain() {.importc.}

    proc fuzzInitialize(argc: ptr cint, argv: ptr ptr ptr cchar): cint
        {.exportc: "LLVMFuzzerInitialize", cdecl, used.} =
      NimMain()  # libFuzzer owns main; initialise Nim's runtime here
      0

    proc fuzzTestOneInput(data: ptr UncheckedArray[byte], len: csize_t): cint
        {.exportc: "LLVMFuzzerTestOneInput", cdecl, used.} =
      var s = newString(len.int)
      if len > 0:
        copyMem(addr s[0], data, len.int)
      runOne(s)
      0
  else:
    proc main =
      if paramCount() == 0:
        runOne(readAll(stdin))
      else:
        for i in 1 .. paramCount():
          let p = paramStr(i)
          if dirExists(p):
            for f in walkDirRec(p): runOne(readFile(f))
          elif fileExists(p):
            runOne(readFile(p))
    main()
