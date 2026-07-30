#!/usr/bin/env python3
# Benchmark client: Python `requests` (Session). requests pools connections and
# auto-decompresses gzip when the body is read, so accessing `.content` does the
# same decompression work as the other clients. NAVI_BENCH_COLD=1 sends
# `Connection: close` so every request reconnects (fresh TCP+TLS).
import os
import sys
import time

import requests

try:  # self-signed target; silence the per-request InsecureRequestWarning
    import urllib3
    urllib3.disable_warnings()
except Exception:
    pass

url = os.environ.get("NAVI_BENCH_URL", "https://127.0.0.1:8443")
iters = int(os.environ.get("NAVI_BENCH_ITERS", "3000"))
cold = os.environ.get("NAVI_BENCH_COLD", "0") == "1"
warmup = min(20, iters) if cold else max(100, iters // 10)
body = "x" * 256

sess = requests.Session()
sess.verify = False
sess.headers.update({"Accept-Encoding": "gzip"})
if cold:
    sess.headers["Connection"] = "close"


def one():
    sess.get(url + "/get").content
    sess.post(url + "/post", data=body).content
    sess.put(url + "/put", data=body).content
    sess.patch(url + "/patch", data=body).content
    sess.delete(url + "/delete").content
    sess.head(url + "/get")
    sess.options(url + "/get").content


for _ in range(warmup):
    one()
t0 = time.perf_counter()
for _ in range(iters):
    one()
secs = time.perf_counter() - t0
reqs = iters * 7
sys.stdout.write("RESULT\tpython-requests\t%d\t%.3f\t%.0f\n" % (reqs, secs, reqs / secs))
