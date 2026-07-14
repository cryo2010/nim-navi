# Fuzzing the sans-io decoders

The protocol cores are pure byte-in / event-out state machines, which makes them
ideal fuzz targets: feed arbitrary bytes, assert nothing crashes. Targets:

| target    | decoder under test                          |
| --------- | ------------------------------------------- |
| `hpack`   | HPACK header-block decoder                  |
| `h1`      | HTTP/1.1 response parser                    |
| `frame`   | HTTP/2 frame decoder                        |
| `huffman` | HPACK Huffman string decoder                |

Each target feeds the input into its decoder. Malformed input must raise a
`CatchableError` (the target swallows it); a `Defect`, out-of-bounds read, hang,
or ASan/UBSan report is a real bug. The build enables Nim's runtime checks
(`--panics:on`) plus ASan/UBSan.

## Run

```
# coverage-guided libFuzzer run (needs clang + the fuzzer runtime), 60 seconds:
tests/fuzz/run.sh hpack 60

# portable ASan replay of the committed seed corpus (what PR CI runs):
tests/fuzz/run.sh hpack replay
```

libFuzzer writes discovered inputs to `tests/fuzz/corpus/<target>/` (gitignored)
and any crash to `crash-*` in the working directory. `tests/fuzz/seeds/<target>/`
holds the small committed starting corpus.

## CI

`.github/workflows/fuzz.yml` replays the seed corpus on every PR (portable, fast)
and runs each target under libFuzzer nightly, uploading any crash as an artifact.

## History

The `hpack` target found an `IndexDefect`/`OverflowDefect` on truncated and
oversized integer/string fields (a hostile peer could crash the client); fixed
by bounds-guarding the HPACK integer and string decoders. Regression coverage
lives in `tests/test_h2_hpack.nim`.
