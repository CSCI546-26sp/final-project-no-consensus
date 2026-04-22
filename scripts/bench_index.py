"""
Benchmark the restructured InvertedIndex against a legacy shim that mirrors
the pre-refactor design (per-(term, line) postings, list[int] positions,
lineText stored in every posting).

Run from the codebase root:
    python scripts/bench_index.py
"""

import gc
import math
import os
import re
import sys
import time
import tracemalloc
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chunkserver.index import InvertedIndex
from common.config import STOP_WORDS


CHUNK_SIZE = 4 * 1024 * 1024
CORPUS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "wiki",
    "big-text-files",
    "2pptmlurtb.txt",
)


class LegacyPosting:
    def __init__(self, chunkHandle, lineNumber, frequency, lineText, positions=None):
        self.chunkHandle = chunkHandle
        self.lineNumber = lineNumber
        self.frequency = frequency
        self.lineText = lineText
        self.positions = positions or []


class LegacyInvertedIndex:
    def __init__(self):
        self.index = defaultdict(list)
        self.totalChunks = 0
        self.chunkTerms = defaultdict(set)
        self._TOKEN_PATTERN = re.compile(r"\W+")

    def tokenize(self, text):
        return list(filter(
            lambda w: w and w not in STOP_WORDS,
            self._TOKEN_PATTERN.split(text.lower()),
        ))

    def indexChunk(self, chunkHandle, text):
        lines = text.split("\n")
        chunkTermfrequency = defaultdict(int)
        termLines = defaultdict(list)

        for lineNumber, line in enumerate(lines):
            tokens = self.tokenize(line)
            linePositions = defaultdict(list)
            for position, token in enumerate(tokens):
                chunkTermfrequency[token] += 1
                linePositions[token].append(position)
            for token, positions in linePositions.items():
                termLines[token].append((lineNumber, line, positions))

        for term, locations in termLines.items():
            for lineNumber, lineText, positions in locations:
                self.index[term].append(
                    LegacyPosting(
                        chunkHandle, lineNumber,
                        chunkTermfrequency[term], lineText, positions,
                    )
                )
        self.chunkTerms[chunkHandle] = set(termLines.keys())
        self.totalChunks += 1

    def search(self, query):
        tokens = self.tokenize(query)
        result = {}
        for token in tokens:
            postings = self.index.get(token, [])
            if not postings:
                continue
            df = len(set(p.chunkHandle for p in postings))
            idf = math.log(1 + self.totalChunks / df)
            for posting in postings:
                score = posting.frequency * idf
                if posting.chunkHandle not in result:
                    result[posting.chunkHandle] = [0, posting.lineNumber, posting.lineText]
                result[posting.chunkHandle][0] += score
        return sorted(result.items(), key=lambda x: x[1][0], reverse=True)


def chunkText(text, maxBytes=CHUNK_SIZE):
    chunks = []
    current = []
    currentLen = 0
    for line in text.splitlines(keepends=True):
        lineBytes = len(line.encode("utf-8"))
        if currentLen + lineBytes > maxBytes and current:
            chunks.append("".join(current))
            current = []
            currentLen = 0
        current.append(line)
        currentLen += lineBytes
    if current:
        chunks.append("".join(current))
    return chunks


def formatBytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def benchmark(label, IndexCls, chunks, queries):
    gc.collect()
    tracemalloc.start()

    idx = IndexCls()

    t0 = time.perf_counter()
    for i, chunk in enumerate(chunks):
        idx.indexChunk(f"chunk-{i}", chunk)
    indexTime = time.perf_counter() - t0

    currentMem, peakMem = tracemalloc.get_traced_memory()

    searchTimes = []
    for q in queries:
        t0 = time.perf_counter()
        idx.search(q)
        searchTimes.append((time.perf_counter() - t0) * 1000)

    termCount = len(idx.index)
    tracemalloc.stop()
    del idx
    gc.collect()

    return {
        "label": label,
        "indexTime": indexTime,
        "peakMem": peakMem,
        "currentMem": currentMem,
        "searchTimes": searchTimes,
        "termCount": termCount,
    }


def main():
    if not os.path.exists(CORPUS_PATH):
        print(f"ERROR: corpus not found at {CORPUS_PATH}")
        sys.exit(1)

    with open(CORPUS_PATH, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    sizeMb = len(text.encode("utf-8")) / (1024 * 1024)
    chunks = chunkText(text)
    print(f"\nCorpus: {CORPUS_PATH}")
    print(f"Size: {sizeMb:.2f} MB in {len(chunks)} chunk(s)\n")

    queries = ["love", "death", "king", "the world", "night and day"]

    legacy = benchmark("Legacy (per-line postings)", LegacyInvertedIndex, chunks, queries)
    current = benchmark("Current (per-chunk + varint)", InvertedIndex, chunks, queries)

    header = f"{'Metric':<32} {'Legacy':>18} {'Current':>18}"
    print(header)
    print("-" * len(header))
    print(f"{'indexChunk wall (s)':<32} {legacy['indexTime']:>18.3f} {current['indexTime']:>18.3f}")
    print(f"{'Index memory (resident)':<32} {formatBytes(legacy['currentMem']):>18} {formatBytes(current['currentMem']):>18}")
    print(f"{'Index memory (peak)':<32} {formatBytes(legacy['peakMem']):>18} {formatBytes(current['peakMem']):>18}")
    print(f"{'Unique terms':<32} {legacy['termCount']:>18} {current['termCount']:>18}")
    for i, q in enumerate(queries):
        label = f"search({q!r}) (ms)"
        print(f"{label:<32} {legacy['searchTimes'][i]:>18.2f} {current['searchTimes'][i]:>18.2f}")
    print()

    if current["currentMem"]:
        print(f"Resident memory reduction: {legacy['currentMem'] / current['currentMem']:.1f}x")
    if current["peakMem"]:
        print(f"Peak memory reduction:     {legacy['peakMem'] / current['peakMem']:.1f}x")
    if current["indexTime"]:
        print(f"Indexing speedup:          {legacy['indexTime'] / current['indexTime']:.2f}x")


if __name__ == "__main__":
    main()
