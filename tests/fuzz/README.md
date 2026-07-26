# Fuzzing the sans-io decoders

The protocol cores are pure byte-in / event-out state machines, which makes them
ideal fuzz targets: feed arbitrary bytes, assert nothing crashes. Targets:

| target    | code under test                             |
| --------- | ------------------------------------------- |
| `hpack`   | HPACK header-block decoder                  |
| `h1`      | HTTP/1.1 response parser                    |
| `frame`   | HTTP/2 frame decoder                        |
| `huffman` | HPACK Huffman string decoder                |
| `h2conn`  | HTTP/2 connection state machine (`H2Conn`)  |

The first four feed the input into a decoder. Malformed input must raise a
`CatchableError` (the target swallows it); a `Defect`, out-of-bounds read, hang,
or ASan/UBSan report is a real bug. The build enables Nim's runtime checks
(`--panics:on`) plus ASan/UBSan.

`h2conn` is different: **structure-aware and differential.** The frame decoder
already survives arbitrary bytes, but the padding / interim-1xx / trailers /
flow-control bugs lived in `H2Conn`'s *semantics* -- it parsed frames fine, then
mishandled them. So this target reads the input as a script to build a *valid*
server response on one stream -- randomizing DATA/HEADERS padding, a HEADERS
priority prefix, interim 1xx blocks, trailers, how the body is chunked, and how
the wire is split across `feed()` calls -- then asserts `H2Conn` reassembles the
exact status, headers, and body. Any mismatch (padding leaking in, an interim
block polluting the final headers, a trailer surfacing) crashes the target and
is a finding.

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

### From any host (Docker)

libFuzzer's runtime ships with clang on Linux but not with macOS's clang, so on
macOS use the Docker image via `nimble fuzz`:

```
nimble fuzz                                  # 60s libFuzzer run of h2conn
NAVI_FUZZ_TARGET=frame NAVI_FUZZ_TIME=120 nimble fuzz
NAVI_FUZZ_TIME=replay nimble fuzz            # portable ASan seed replay
```

`NAVI_FUZZ_TARGET` (default `h2conn`) and `NAVI_FUZZ_TIME` (seconds, or `replay`;
default `60`) select the target and mode. The corpus is bind-mounted, so coverage
and any crash reproducer persist on the host under `tests/fuzz/corpus/<target>/`.

## CI

`.github/workflows/fuzz.yml` replays the seed corpus on every PR (portable, fast)
and runs each target under libFuzzer nightly, uploading any crash as an artifact.

## History

The `hpack` target found an `IndexDefect`/`OverflowDefect` on truncated and
oversized integer/string fields (a hostile peer could crash the client); fixed
by bounds-guarding the HPACK integer and string decoders. Regression coverage
lives in `tests/test_h2_hpack.nim`.
