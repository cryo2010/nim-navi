# HPACK conformance corpus

Vendored from [http2jp/hpack-test-case](https://github.com/http2jp/hpack-test-case)
(MIT license, see `LICENSE`), pinned to commit
`8a1406e7d14bfcb6c046021f13cc15cfb162726d`.

Each `story_NN.json` is a sequence of HPACK-encoded header blocks (`wire`, hex)
that share one compression context (the dynamic table), paired with the expected
decoded `headers`. Every directory is a different encoder, so decoding all of
them cross-checks navi's HPACK decoder against several independent
implementations:

- `nghttp2` — Huffman-coded output from the reference C implementation.
- `go-hpack` — Go's encoder (a second, independent implementation).
- `nghttp2-change-table-size` — exercises in-band dynamic table size updates.

`tests/test_h2_hpack_corpus.nim` walks these directories and decodes every case.
To refresh or extend the corpus, copy more directories from the upstream repo at
the pinned commit.
