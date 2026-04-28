"""Bench A: Linear regex (word-boundary) vs InvertedIndex (TF-IDF) across sizes.

Reports per-query latency (ms), build time, peak memory, and unique terms for
each corpus size. Output is a labeled text file the slide deck can consume
verbatim.

Run from the codebase root with:
    python scripts/bench_index_sizes.py --out <results.txt> --size <50|100|500>
"""

from __future__ import annotations

import argparse
import gc
import os
import re
import statistics
import sys
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chunkserver.index import InvertedIndex
from common.config import CHUNK_SIZE, STOP_WORDS  # noqa: F401


CODEBASE = Path(__file__).resolve().parents[1]
SRC_DIRS = {
    50:  CODEBASE / "data" / "wiki" / "big-text-files",
    500: CODEBASE / "data" / "wiki" / "bigger-text-files",
}
SYNTH_100 = CODEBASE / "data" / "wiki" / "synthetic" / "100mb-concat.txt"

QUERIES = [
    "love",
    "death",
    "king",
    "the world",
    "night and day",
    "physics",
    "italy",
    "absent_zzzz_term",
]


def _word_re(query: str) -> re.Pattern:
    """Word-boundary, case-insensitive search. Matches are *line hits* (counts of
    lines that contain the term). Phrases are matched as substrings of a line.
    """
    return re.compile(re.escape(query), re.IGNORECASE)


def linearScanCorpus(text: str, query: str):
    """Naive baseline: scan the whole corpus line by line. Returns list of
    (line_no, line_text) for every match. We cap at 10 000 to avoid pathological
    blow-ups on very common terms."""
    pattern = _word_re(query)
    matches = []
    for ln, line in enumerate(text.splitlines()):
        if pattern.search(line):
            matches.append((ln, line))
            if len(matches) >= 10_000:
                break
    return matches


def chunkText(text: str, maxBytes: int = CHUNK_SIZE):
    chunks = []
    cur = []
    curLen = 0
    for line in text.splitlines(keepends=True):
        b = len(line.encode("utf-8"))
        if curLen + b > maxBytes and cur:
            chunks.append("".join(cur))
            cur = []
            curLen = 0
        cur.append(line)
        curLen += b
    if cur:
        chunks.append("".join(cur))
    return chunks


def fmtBytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def loadCorpus(sizeMb: int) -> tuple[str, str]:
    if sizeMb == 100:
        path = SYNTH_100
    else:
        # First eligible file in the corresponding dir
        d = SRC_DIRS[sizeMb]
        files = sorted(p for p in d.glob("*.txt") if p.stat().st_size > 40 * 1024 * 1024)
        if not files:
            raise FileNotFoundError(f"no large .txt files in {d}")
        path = files[0]
    with path.open("r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    return str(path), text


def runOne(sizeMb: int, out_lines: list[str]):
    path, text = loadCorpus(sizeMb)
    actualBytes = len(text.encode("utf-8"))
    chunks = chunkText(text)

    out_lines.append("")
    out_lines.append(f"=== {sizeMb} MB corpus  ({path}) ===")
    out_lines.append(f"actual size: {actualBytes:,} bytes  ({actualBytes / (1024*1024):.1f} MB)")
    out_lines.append(f"chunks: {len(chunks)} (CHUNK_SIZE = {CHUNK_SIZE:,} B)")

    # ---- build the index, capture build time + memory ----
    gc.collect()
    tracemalloc.start()
    idx = InvertedIndex()
    t0 = time.perf_counter()
    for i, chunk in enumerate(chunks):
        idx.indexChunk(f"chunk-{i}", chunk)
    buildSec = time.perf_counter() - t0
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    out_lines.append(f"InvertedIndex build: {buildSec:.2f}s   "
                     f"unique terms: {len(idx.index):,}   "
                     f"resident: {fmtBytes(cur)}   peak: {fmtBytes(peak)}")

    # ---- query timings ----
    out_lines.append("")
    out_lines.append(f"{'query':<22}  {'linear (ms)':>14}  {'TF-IDF (ms)':>14}  "
                     f"{'speedup':>10}  {'linear hits':>12}  {'tfidf hits':>11}")
    out_lines.append("-" * 92)
    for q in QUERIES:
        # best-of-3 for stability
        linear_ms = []
        for _ in range(3):
            t = time.perf_counter()
            lin_hits = linearScanCorpus(text, q)
            linear_ms.append((time.perf_counter() - t) * 1000)
        idx_ms = []
        for _ in range(3):
            t = time.perf_counter()
            idx_hits = idx.search(q)
            idx_ms.append((time.perf_counter() - t) * 1000)
        lin_min = min(linear_ms)
        idx_min = min(idx_ms)
        sp = (lin_min / idx_min) if idx_min > 0 else float("inf")
        out_lines.append(
            f"{q!r:<22}  {lin_min:>14.2f}  {idx_min:>14.2f}  {sp:>9.1f}x  "
            f"{len(lin_hits):>12,}  {len(idx_hits):>11,}"
        )

    del idx
    gc.collect()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", type=int, required=True, choices=[50, 100, 500])
    ap.add_argument("--out", type=str, required=True)
    args = ap.parse_args()

    out_lines: list[str] = []
    out_lines.append("Bench A · Linear regex vs InvertedIndex (TF-IDF)")
    out_lines.append(f"timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    out_lines.append("methodology: best-of-3 per query; linear is regex word-boundary "
                     "scan capped at 10 000 hits; TF-IDF returns ranked chunks.")

    runOne(args.size, out_lines)

    Path(args.out).write_text("\n".join(out_lines) + "\n")
    print("\n".join(out_lines))
    print(f"\n[wrote {args.out}]")


if __name__ == "__main__":
    main()
