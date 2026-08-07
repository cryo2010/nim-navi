#!/bin/sh
# Run both streaming examples; each exits non-zero on a hash mismatch, so the
# compose run fails loudly if a transfer was corrupted.
set -e
./download
./upload
