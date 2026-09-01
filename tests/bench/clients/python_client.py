#!/usr/bin/env python3
"""bench reference client: Python httpx + asyncio (name: python).

Dispatches on NAVI_WORKLOAD (default "requests"):
  - requests:        buffered GET/POST/PUT, latency per request.
  - streamDownload:  streamed GET, sha1-verified, time per transfer.
  - streamUpload:    streamed POST, sha1-verified, time per transfer.
  - other (ws/sse/...): SKIP.
Time-boxed with warmup/measure windows; prints ONE tab-separated RESULT line.
Same contract as the other-language reference clients. HTTP/3 is skipped.
"""

import asyncio
import hashlib
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
WORKLOAD = env_str("NAVI_WORKLOAD", "requests")
STREAM_BYTES = env_int("NAVI_STREAM_BYTES", 1073741824)

COLD = MODE == "cold"
VERBS = ("GET", "POST", "PUT")
MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# Latency histogram (MUST match the cross-language scheme exactly)
# ---------------------------------------------------------------------------
BUCKETS_PER_DOUBLING = 64
MAX_BUCKETS = int(math.floor(math.log2(300000000.0) * 64)) + 1

# single shared histogram; asyncio is single-threaded so no lock is needed
counts = [0] * MAX_BUCKETS
total = 0
total_bytes = 0


def record(us, nbytes=0):
    global total, total_bytes
    total_bytes += nbytes
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
    return b


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


async def requests_worker(client, i, measure_start, deadline):
    n = i
    while time.perf_counter() < deadline:
        verb = VERBS[n % len(VERBS)]
        n += 1
        url = pick() + "/echo"
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


def fail(msg):
    sys.stderr.write("[python] " + msg + "\n")
    sys.exit(1)


async def stream_download_worker(client, i, measure_start, deadline):
    while time.perf_counter() < deadline:
        url = pick() + "/download?size={0}".format(STREAM_BYTES)
        t0 = time.perf_counter()
        h = hashlib.sha1()
        got = 0
        try:
            async with client.stream("GET", url) as r:
                if r.status_code != 200:
                    fail("download status {0}".format(r.status_code))
                check_version(r)
                async for chunk in r.aiter_bytes():
                    h.update(chunk)
                    got += len(chunk)
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            fail("FAIL: {0}".format(e))
        want = r.headers.get("x-sha1")
        if h.hexdigest() != want:
            fail("download sha1: got {0} want {1}".format(h.hexdigest(), want))
        if time.perf_counter() >= measure_start:
            record((time.perf_counter() - t0) * 1e6, got)


def upload_body(h):
    """Async generator yielding STREAM_BYTES from a reused 1 MiB block."""
    block = b"\x5a" * MIB

    async def gen():
        remaining = STREAM_BYTES
        while remaining > 0:
            n = MIB if remaining >= MIB else remaining
            piece = block if n == MIB else block[:n]
            h.update(piece)
            remaining -= n
            yield piece

    return gen()


async def stream_upload_worker(client, i, measure_start, deadline):
    while time.perf_counter() < deadline:
        url = pick() + "/upload"
        t0 = time.perf_counter()
        h = hashlib.sha1()
        try:
            resp = await client.post(
                url,
                content=upload_body(h),
                headers={"content-type": "application/octet-stream"},
            )
            if resp.status_code != 200:
                fail("upload status {0}".format(resp.status_code))
            check_version(resp)
            body = resp.json()
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            fail("FAIL: {0}".format(e))
        if body.get("sha1") != h.hexdigest():
            fail("upload sha1: got {0} want {1}".format(
                body.get("sha1"), h.hexdigest()))
        if int(body.get("size", -1)) != STREAM_BYTES:
            fail("upload size: got {0} want {1}".format(
                body.get("size"), STREAM_BYTES))
        if time.perf_counter() >= measure_start:
            record((time.perf_counter() - t0) * 1e6, STREAM_BYTES)


WORKERS = {
    "requests": requests_worker,
    "streamDownload": stream_download_worker,
    "streamUpload": stream_upload_worker,
}


async def main():
    if PROTO == "h3":
        sys.stdout.write("SKIP\tpython\tno reference-client HTTP/3\n")
        sys.exit(0)
    worker = WORKERS.get(WORKLOAD)
    if worker is None:
        sys.stdout.write(
            "SKIP\tpython\t{0} not implemented\n".format(WORKLOAD))
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
    mbps = total_bytes / secs / 1e6 if secs > 0 else 0.0
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
            "{0:.1f}".format(mbps),
        ]
    )
    sys.stdout.write(line + "\n")


if __name__ == "__main__":
    asyncio.run(main())
