"""Bench B: linear regex scan vs NGramIndex-accelerated scan, across sizes.

Builds a single per-size NGramIndex (treating the whole file as one chunk),
then runs a fixed query set through both the naive regex path and the
trigram-then-verify path used by ScanChunk.

Run:
    python scripts/bench_scan_sizes.py --size 50 --out results.txt
"""

from __future__ import annotations

import argparse
import bisect
import gc
import os
import re
import sys
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chunkserver.ngram import NGramIndex


CODEBASE = Path(__file__).resolve().parents[1]
SRC_DIRS = {
    50:  CODEBASE / "data" / "wiki" / "big-text-files",
    500: CODEBASE / "data" / "wiki" / "bigger-text-files",
}
SYNTH_100 = CODEBASE / "data" / "wiki" / "synthetic" / "100mb-concat.txt"

QUERIES = [
    ("Mussolini",         "rare term"),
    ("history",           "common term"),
    ("the",               "very common term  (trigram falls back)"),
    ("and the",           "common phrase"),
    ("absent_zzzz_term",  "absent term  (early-out)"),
    ("computer science",  "phrase"),
    ("distributed",       "rare-ish term"),
]
MAX_MATCHES = 1000


def linearScan(rawLines, query):
    pattern = re.compile(re.escape(query), re.IGNORECASE)
    matches = []
    for ln, line in enumerate(rawLines):
        if pattern.search(line):
            matches.append((ln, line))
            if len(matches) >= MAX_MATCHES:
                break
    return matches


def trigramScan(ng, lower, lineStarts, lines, query):
    """Mirror what ScanChunk does on the chunkserver."""
    candidates = ng.candidatePositions("c", query)
    if candidates is None:
        return linearScan(lines, query)
    if not candidates:
        return []
    qlow = query.lower()
    qlen = len(qlow)
    seen = set()
    matches = []
    for p in candidates:
        if p + qlen > len(lower) or lower[p:p + qlen] != qlow:
            continue
        ln = bisect.bisect_right(lineStarts, p) - 1
        if ln in seen:
            continue
        seen.add(ln)
        matches.append((ln, lines[ln]))
        if len(matches) >= MAX_MATCHES:
            break
    return matches


def fmtBytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def best_of(fn, reps):
    timings = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        timings.append((time.perf_counter() - t0) * 1000)
    return min(timings)


def loadCorpus(sizeMb: int):
    if sizeMb == 100:
        path = SYNTH_100
    else:
        d = SRC_DIRS[sizeMb]
        files = sorted(p for p in d.glob("*.txt") if p.stat().st_size > 40 * 1024 * 1024)
        path = files[0]
    with path.open("r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    return str(path), text


def runOne(sizeMb: int, out_lines: list[str]):
    path, text = loadCorpus(sizeMb)
    actualBytes = len(text.encode("utf-8"))

    out_lines.append("")
    out_lines.append(f"=== {sizeMb} MB corpus  ({path}) ===")
    out_lines.append(f"actual size: {actualBytes:,} bytes  ({actualBytes / (1024*1024):.1f} MB)")

    gc.collect()
    tracemalloc.start()
    t0 = time.perf_counter()
    ng = NGramIndex()
    ng.indexChunk("c", text)
    buildSec = time.perf_counter() - t0
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    uniqueTri = len(ng.index["c"])
    out_lines.append(f"NGramIndex build: {buildSec:.2f}s   unique trigrams: {uniqueTri:,}   "
                     f"resident: {fmtBytes(cur)}   peak: {fmtBytes(peak)}")

    # Pre-compute line offsets once
    lower = text.lower()
    linesWithEnds = text.splitlines(keepends=True)
    lineStarts = []
    off = 0
    for ln in linesWithEnds:
        lineStarts.append(off)
        off += len(ln)
    rawLines = text.splitlines()
    lines = [ln.rstrip("\r\n") for ln in linesWithEnds]

    reps = 3 if sizeMb >= 100 else 5
    out_lines.append(f"\nUsing best-of-{reps}; cap = {MAX_MATCHES} matches per scan.")
    out_lines.append(
        f"{'query':<24}  {'desc':<28}  {'regex (ms)':>11}  "
        f"{'trigram (ms)':>13}  {'speedup':>9}  {'hits':>6}"
    )
    out_lines.append("-" * 99)
    for query, desc in QUERIES:
        regex_ms = best_of(lambda: linearScan(rawLines, query), reps)
        tri_ms = best_of(
            lambda: trigramScan(ng, lower, lineStarts, lines, query), reps
        )
        speedup = (regex_ms / tri_ms) if tri_ms > 0 else float("inf")
        # final hit count (one-shot)
        hits = len(trigramScan(ng, lower, lineStarts, lines, query))
        out_lines.append(
            f"{query!r:<24}  {desc:<28}  {regex_ms:>11.2f}  {tri_ms:>13.2f}  "
            f"{speedup:>8.1f}x  {hits:>6,}"
        )

    del ng
    gc.collect()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", type=int, required=True, choices=[50, 100, 500])
    ap.add_argument("--out", type=str, required=True)
    args = ap.parse_args()

    out_lines: list[str] = []
    out_lines.append("Bench B · Linear regex vs NGramIndex (trigram-accelerated)")
    out_lines.append(f"timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    out_lines.append("methodology: best-of-N per query; both paths capped at 1 000 matches; "
                     "trigram path mirrors ScanChunk (intersect → regex verify).")

    runOne(args.size, out_lines)

    Path(args.out).write_text("\n".join(out_lines) + "\n")
    print("\n".join(out_lines))
    print(f"\n[wrote {args.out}]")


if __name__ == "__main__":
    main()
