"""Prepare a 100 MB test corpus by concatenating two 50 MB wiki files.

Idempotent: skips work if the output file already exists at the right size.
Picks the first two .txt files in big-text-files (deterministic by filename).
"""
import os
import sys
from pathlib import Path

CODEBASE = Path(__file__).resolve().parents[1]
SRC_DIR = CODEBASE / "data" / "wiki" / "big-text-files"
OUT_DIR = CODEBASE / "data" / "wiki" / "synthetic"
OUT_FILE = OUT_DIR / "100mb-concat.txt"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if OUT_FILE.exists() and OUT_FILE.stat().st_size > 95 * 1024 * 1024:
        print(f"  [prep] {OUT_FILE} already exists ({OUT_FILE.stat().st_size:,} B), skipping")
        return 0

    files = sorted(p for p in SRC_DIR.glob("*.txt") if p.stat().st_size > 40 * 1024 * 1024)
    if len(files) < 2:
        print(f"ERROR: need >=2 50-MB files in {SRC_DIR}", file=sys.stderr)
        return 1
    a, b = files[0], files[1]
    print(f"  [prep] concatenating {a.name} + {b.name} -> {OUT_FILE.name}")
    with OUT_FILE.open("wb") as out:
        for src in (a, b):
            with src.open("rb") as f:
                while True:
                    buf = f.read(1024 * 1024)
                    if not buf:
                        break
                    out.write(buf)
    size = OUT_FILE.stat().st_size
    print(f"  [prep] {OUT_FILE} = {size:,} B ({size / (1024*1024):.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
