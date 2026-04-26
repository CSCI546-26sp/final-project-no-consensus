"""
Benchmark linear regex scan vs. NGramIndex-accelerated scan over the same
chunk text. The substring-search path used by ScanChunk.

Run from the codebase root:
    python scripts/bench_scan.py
"""

import bisect
import gc
import os
import re
import sys
import time
import tracemalloc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chunkserver.ngram import NGramIndex


CORPUS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "wiki",
    "bigger-text-files",
    "4xo3y9xm0n.txt",
)

SIZES_MB = [1, 10, 50]
MAX_MATCHES = 1000  # cap collected results so pathological queries don't dominate

QUERIES = [
    ("Mussolini", "rare term"),
    ("history", "common term"),
    ("the", "very common term"),
    ("and the", "common phrase"),
    ("zzzzznotfound", "absent term (early-out)"),
]


def repetitionsFor(sizeMb):
    if sizeMb >= 50:
        return 2
    if sizeMb >= 10:
        return 3
    return 5


def linearScan(text, lines, query):
    pattern = re.compile(re.escape(query), re.IGNORECASE)
    matches = []
    for lineNumber, line in enumerate(lines):
        if pattern.search(line):
            matches.append((lineNumber, line))
            if len(matches) >= MAX_MATCHES:
                break
    return matches


def trigramScan(ngramIdx, chunkHandle, lower, qlow_cache, lineStarts, lines, query):
    candidates = ngramIdx.candidatePositions(chunkHandle, query)
    if candidates is None:
        return linearScan(lower, lines, query)  # short query fallback
    if not candidates:
        return []

    qlow = qlow_cache
    qlen = len(qlow)

    matches = []
    seen = set()
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


def timeBestOf(fn, repetitions):
    timings = []
    for _ in range(repetitions):
        t0 = time.perf_counter()
        fn()
        timings.append((time.perf_counter() - t0) * 1000)
    return min(timings)


def formatBytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def runSize(fullText, sizeMb):
    chars = sizeMb * 1024 * 1024
    text = fullText[:chars]
    actualBytes = len(text.encode("utf-8"))
    print(f"\n=== Corpus slice: {actualBytes / (1024 * 1024):.1f} MB ({len(text):,} chars) ===")

    gc.collect()
    tracemalloc.start()
    t0 = time.perf_counter()
    ng = NGramIndex()
    ng.indexChunk("c", text)
    buildTime = time.perf_counter() - t0
    currentMem, peakMem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    uniqueTrigrams = len(ng.index["c"])
    print(f"NGramIndex build:  {buildTime:.2f}s   "
          f"unique trigrams: {uniqueTrigrams:,}   "
          f"memory: resident={formatBytes(currentMem)} peak={formatBytes(peakMem)}")

    # Pre-compute once per size — these were dominating per-call cost at 50 MB.
    print("Preparing line offsets...", flush=True)
    t0 = time.perf_counter()
    lower = text.lower()
    linesWithEnds = text.splitlines(keepends=True)
    lineStarts = []
    offset = 0
    for line in linesWithEnds:
        lineStarts.append(offset)
        offset += len(line)
    lines = [line.rstrip("\r\n") for line in linesWithEnds]
    rawLines = text.splitlines()
    prepTime = time.perf_counter() - t0
    print(f"  done in {prepTime:.2f}s   {len(lines):,} lines")

    reps = repetitionsFor(sizeMb)
    print(f"\nUsing best-of-{reps} (capped at {MAX_MATCHES} matches per scan)")
    print(f"{'query':<22} {'desc':<26} {'linear (ms)':>14} {'trigram (ms)':>14} {'speedup':>10}")
    print("-" * 90, flush=True)
    for query, desc in QUERIES:
        qlow = query.lower()
        linearMs = timeBestOf(lambda: linearScan(text, rawLines, query), reps)
        trigramMs = timeBestOf(
            lambda: trigramScan(ng, "c", lower, qlow, lineStarts, lines, query), reps
        )
        speedup = linearMs / trigramMs if trigramMs > 0 else float("inf")
        print(
            f"{query!r:<22} {desc:<26} {linearMs:>14.2f} {trigramMs:>14.2f} {speedup:>9.1f}x",
            flush=True,
        )

    del ng
    gc.collect()


def main():
    if not os.path.exists(CORPUS_PATH):
        print(f"ERROR: corpus not found at {CORPUS_PATH}")
        sys.exit(1)

    with open(CORPUS_PATH, "r", encoding="utf-8", errors="replace") as f:
        fullText = f.read()

    fullMb = len(fullText.encode("utf-8")) / (1024 * 1024)
    print(f"Corpus: {CORPUS_PATH}")
    print(f"Full size: {fullMb:.1f} MB ({len(fullText):,} chars)")

    for sizeMb in SIZES_MB:
        if sizeMb * 1024 * 1024 > len(fullText):
            print(f"\nSkipping {sizeMb} MB slice — corpus is only {fullMb:.1f} MB")
            continue
        runSize(fullText, sizeMb)


if __name__ == "__main__":
    main()
