#!/usr/bin/env python3
"""benchRequests reference client: Python httpx + asyncio (name: python).

Benchmarks the buffered requests workload (GET/POST/PUT) over HTTP/1.1 or
HTTP/2, time-boxed, recording latency, and prints ONE tab-separated RESULT
line. Same contract as the other-language reference clients. HTTP/3 is skipped.
"""

import asyncio
import math
import os
import sys
import time

import httpx


# ---------------------------------------------------------------------------
# Config (all env vars optional, with defaults)
# ---------------------------------------------------------------------------
def env_str(name, default):
    return os.environ.get(name, default)


def env_int(name, default):
    return int(os.environ.get(name, default))


def env_float(name, default):
    return float(os.environ.get(name, default))


HOST = env_str("NAVI_HOST", "127.0.0.1")
BASE_PORT = env_int("NAVI_BASE_PORT", 9443)
SERVERS = env_int("NAVI_SERVERS", 5)
PROTO = env_str("NAVI_PROTO", "h2")
SECONDS = env_float("NAVI_SECONDS", 20)
WARMUP_SECONDS = env_float("NAVI_WARMUP_SECONDS", 2)
MODE = env_str("NAVI_MODE", "pooled")
CLIENTS = env_int("NAVI_CLIENTS", 3)
CONCURRENCY = env_int("NAVI_CONCURRENCY", 8)

COLD = MODE == "cold"
VERBS = ("GET", "POST", "PUT")


# ---------------------------------------------------------------------------
# Latency histogram (MUST match the cross-language scheme exactly)
# ---------------------------------------------------------------------------
BUCKETS_PER_DOUBLING = 64
MAX_BUCKETS = int(math.floor(math.log2(300000000.0) * 64)) + 1

# single shared histogram; asyncio is single-threaded so no lock is needed
counts = [0] * MAX_BUCKETS
total = 0


def record(us):
    global total
    v = max(1.0, us)
    idx = int(math.floor(math.log2(v) * 64))
    if idx < 0:
        idx = 0
    elif idx >= MAX_BUCKETS:
        idx = MAX_BUCKETS - 1
    counts[idx] += 1
    total += 1


def percentile(p):
    """Return the p-th percentile latency in MILLISECONDS."""
    if total == 0:
        return 0.0
    target = max(1, math.ceil(p / 100 * total))
    cum = 0
    for idx in range(MAX_BUCKETS):
        cum += counts[idx]
        if cum >= target:
            return (2 ** ((idx + 0.5) / 64)) / 1000.0
    return (2 ** ((MAX_BUCKETS - 0.5) / 64)) / 1000.0


# ---------------------------------------------------------------------------
# Workload
# ---------------------------------------------------------------------------
BASES = ["https://{0}:{1}".format(HOST, BASE_PORT + i) for i in range(SERVERS)]
_rr = [0]


def pick():
    b = BASES[_rr[0] % len(BASES)]
    _rr[0] += 1
    return b + "/echo"


# gate the very first successful response against the negotiated protocol
_version_checked = [False]


def check_version(resp):
    if _version_checked[0]:
        return
    _version_checked[0] = True
    want = "HTTP/1.1" if PROTO == "h1" else "HTTP/2"
    if resp.http_version != want:
        sys.stderr.write(
            "[python] protocol mismatch: got {0} want {1}\n".format(
                resp.http_version, want
            )
        )
        sys.exit(1)


async def worker(client, i, measure_start, deadline):
    n = i
    while time.perf_counter() < deadline:
        verb = VERBS[n % len(VERBS)]
        n += 1
        url = pick()
        kwargs = {}
        if verb in ("POST", "PUT"):
            kwargs["content"] = b"payload-x"
            kwargs["headers"] = {"content-type": "text/plain"}
        if COLD:
            hdrs = dict(kwargs.get("headers", {}))
            hdrs["connection"] = "close"
            kwargs["headers"] = hdrs
        t0 = time.perf_counter()
        try:
            resp = await client.request(verb, url, **kwargs)
            _ = resp.content  # ensure fully read (httpx reads it already)
        except Exception as e:  # noqa: BLE001
            sys.stderr.write("[python] FAIL: {0}\n".format(e))
            sys.exit(1)
        check_version(resp)
        if time.perf_counter() >= measure_start:
            record((time.perf_counter() - t0) * 1e6)


async def main():
    if PROTO == "h3":
        sys.stdout.write("SKIP\tpython\tno reference-client HTTP/3\n")
        sys.exit(0)

    n_workers = CLIENTS * CONCURRENCY
    max_keepalive = 0 if COLD else n_workers + SERVERS
    limits = httpx.Limits(
        max_connections=n_workers + SERVERS,
        max_keepalive_connections=max_keepalive,
    )
    async with httpx.AsyncClient(
        http2=(PROTO == "h2"),
        verify=False,
        limits=limits,
    ) as client:
        start = time.perf_counter()
        measure_start = start + WARMUP_SECONDS
        deadline = measure_start + SECONDS
        tasks = [
            asyncio.create_task(worker(client, i, measure_start, deadline))
            for i in range(n_workers)
        ]
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - measure_start

    ops = total
    secs = elapsed
    rps = round(ops / secs) if secs > 0 else 0
    line = "\t".join(
        [
            "RESULT",
            "python",
            str(ops),
            "{0:.3f}".format(secs),
            str(rps),
            "{0:.3f}".format(percentile(50)),
            "{0:.3f}".format(percentile(99)),
            "{0:.3f}".format(percentile(99.9)),
            "0.0",
        ]
    )
    sys.stdout.write(line + "\n")


if __name__ == "__main__":
    asyncio.run(main())
