"""Bench D: concurrent search load.

Assumes the cluster is already up and 4 × 50 MB files have been uploaded.
Fires N concurrent search queries (mix of SearchFiles + FileSearch) for each
concurrency level, and reports p50/p95/p99 latency + queries-per-second.

Run:
    python scripts/bench_concurrent_search.py --files <space sep names> --out <results.txt>
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
from common.config import GRPC_OPTIONS
from proto import master_pb2, master_pb2_grpc


MASTER = "localhost:5050"

# Words sampled from the wiki corpora (a mix of common + rare to exercise both paths)
ALL_FILE_QUERIES = [
    "history", "war", "italy", "physics", "chemistry", "music",
    "Mussolini", "Roosevelt", "the world", "computer science",
    "absent_zzzz_term",
]

PER_FILE_QUERIES = [
    "history", "war", "italy", "Mussolini", "computer", "science",
    "the rain", "absent_zzzz_term",
]


def make_stub():
    ch = grpc.insecure_channel(MASTER, options=GRPC_OPTIONS)
    return master_pb2_grpc.MasterServiceStub(ch)


def search_files(stub, q):
    t0 = time.perf_counter()
    resp = stub.SearchFiles(master_pb2.SearchFilesRequest(query=q), timeout=30)
    return (time.perf_counter() - t0) * 1000, len(resp.results)


def file_search(stub, filename, q):
    t0 = time.perf_counter()
    resp = stub.FileSearch(master_pb2.FileSearchRequest(filename=filename, query=q), timeout=30)
    if not resp.success:
        raise RuntimeError(resp.message)
    return (time.perf_counter() - t0) * 1000, len(resp.matches)


def warm_up(files):
    """One pass of each query so first-touch effects don't dominate level-1 measurements."""
    stub = make_stub()
    for q in ALL_FILE_QUERIES:
        try:
            search_files(stub, q)
        except Exception:
            pass
    for f in files:
        for q in PER_FILE_QUERIES:
            try:
                file_search(stub, f, q)
            except Exception:
                pass


def workload_unit(files):
    """Return a closure that performs one search (random pick among modes)."""
    rng = random.Random(os.getpid() ^ time.perf_counter_ns())
    def call():
        stub = make_stub()  # one channel per call — realistic for short-lived clients
        if rng.random() < 0.5:
            q = rng.choice(ALL_FILE_QUERIES)
            ms, _ = search_files(stub, q)
            return ("SearchFiles", q, ms)
        else:
            f = rng.choice(files)
            q = rng.choice(PER_FILE_QUERIES)
            ms, _ = file_search(stub, f, q)
            return ("FileSearch", q, ms)
    return call


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p / 100 * (len(s) - 1)))))
    return s[k]


def run_level(files, concurrency, queries_per_worker, out_lines):
    call = workload_unit(files)
    timings = []
    errors = 0
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(call) for _ in range(concurrency * queries_per_worker)]
        for fut in as_completed(futures):
            try:
                _, _, ms = fut.result()
                timings.append(ms)
            except Exception:
                errors += 1
    wall = time.perf_counter() - t0
    n = len(timings)
    qps = n / wall if wall > 0 else 0.0
    p50 = percentile(timings, 50)
    p95 = percentile(timings, 95)
    p99 = percentile(timings, 99)
    avg = statistics.mean(timings) if timings else 0.0
    out_lines.append(
        f"  concurrency={concurrency:>3}  N={n:>4}  errors={errors:>3}  "
        f"avg={avg:>7.1f}ms  p50={p50:>7.1f}  p95={p95:>7.1f}  p99={p99:>7.1f}  "
        f"qps={qps:>6.1f}  wall={wall:>5.2f}s"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", nargs="+", required=True,
                    help="filenames already uploaded to the cluster")
    ap.add_argument("--out", required=True)
    ap.add_argument("--levels", nargs="+", type=int, default=[1, 4, 16, 64])
    ap.add_argument("--per-worker", type=int, default=8,
                    help="queries each worker fires per level")
    args = ap.parse_args()

    out_lines = []
    out_lines.append("Bench D · Concurrent search load")
    out_lines.append(f"timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    out_lines.append(f"corpus: {len(args.files)} file(s) — " + ", ".join(args.files))
    out_lines.append(f"workload: 50/50 mix of SearchFiles (TF-IDF, all files) "
                     f"and FileSearch (substring, single file)")
    out_lines.append(f"per concurrency level, each worker fires {args.per_worker} queries")
    out_lines.append("")
    out_lines.append("Warm-up pass (one of each query)...")
    warm_up(args.files)
    out_lines.append("Warm-up done.")
    out_lines.append("")

    out_lines.append("Results:")
    for c in args.levels:
        run_level(args.files, c, args.per_worker, out_lines)

    Path(args.out).write_text("\n".join(out_lines) + "\n")
    print("\n".join(out_lines))
    print(f"\n[wrote {args.out}]")


if __name__ == "__main__":
    main()
