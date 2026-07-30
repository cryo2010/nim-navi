#!/usr/bin/env python3
# Benchmark client: Python `requests` (Session). requests pools connections and
# auto-decompresses gzip when the body is read, so accessing `.content` does the
# same decompression work as the other clients. NAVI_BENCH_COLD=1 sends
# `Connection: close` so every request reconnects (fresh TCP+TLS). NAVI_BENCH_CONC>1
# runs that many worker threads, each its own Session -- requests is blocking, so
# threads are how it does concurrency.
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
conc = max(1, int(os.environ.get("NAVI_BENCH_CONC", "1")))
warmup = min(20, iters) if cold else max(100, iters // 10)
body = "x" * 256


def make_session():
    s = requests.Session()
    s.verify = False
    s.headers.update({"Accept-Encoding": "gzip"})
    if cold:
        s.headers["Connection"] = "close"
    return s


def one(s):
    s.get(url + "/get").content
    s.post(url + "/post", data=body).content
    s.put(url + "/put", data=body).content
    s.patch(url + "/patch", data=body).content
    s.delete(url + "/delete").content
    s.head(url + "/get")
    s.options(url + "/get").content


def report(reqs, secs):
    sys.stdout.write("RESULT\tpython-requests\t%d\t%.3f\t%.0f\n" % (reqs, secs, reqs / secs))


if conc > 1:
    from concurrent.futures import ThreadPoolExecutor

    per = max(1, iters // conc)

    def worker(_):
        s = make_session()
        for _ in range(per):
            one(s)

    w = make_session()  # warmup on one session
    for _ in range(min(20, per)):
        one(w)
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=conc) as ex:
        list(ex.map(worker, range(conc)))
    report(per * conc * 7, time.perf_counter() - t0)
else:
    sess = make_session()
    for _ in range(warmup):
        one(sess)
    t0 = time.perf_counter()
    for _ in range(iters):
        one(sess)
    report(iters * 7, time.perf_counter() - t0)
