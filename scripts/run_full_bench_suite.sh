#!/usr/bin/env bash
# Full benchmark suite. Tears down + rebuilds the Docker cluster between every
# test so each measurement is independent.
#
# Usage:
#     ./scripts/run_full_bench_suite.sh
#
# Optional env:
#     SKIP_INDEX_VAR=1   (skip the INDEXER_WORKERS variation pass)
#     ONLY="A B C D"     (run only listed phases)
#     UPLOAD_SIZES="50 100 500"
#     UPLOAD_WORKERS="1 4 8"
#     INDEX_WORKERS_LIST="1 4 8"

set -euo pipefail
SDIR="$(cd "$(dirname "$0")" && pwd)"
source "$SDIR/_bench_lib.sh"

ONLY="${ONLY:-A B C D}"
UPLOAD_SIZES="${UPLOAD_SIZES:-50 100 500}"
UPLOAD_WORKERS="${UPLOAD_WORKERS:-1 4 8}"
INDEX_WORKERS_LIST="${INDEX_WORKERS_LIST:-1 4 8}"
SKIP_INDEX_VAR="${SKIP_INDEX_VAR:-0}"

TS="$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="${RESULTS_DIR:-$PROJECT_ROOT/bench_results/$TS}"
mkdir -p "$RESULTS_DIR"
SUMMARY="$RESULTS_DIR/00_SUMMARY.txt"

{
    echo "DFS bench suite"
    echo "timestamp: $(date)"
    echo "host: $(uname -a)"
    echo "phases: $ONLY"
    echo "results dir: $RESULTS_DIR"
    echo
} | tee "$SUMMARY"

phase_in_only() { [[ " $ONLY " == *" $1 "* ]]; }

# Make the 100 MB synthetic corpus once
PYTHONPATH="$CODEBASE" python "$CODEBASE/scripts/bench_prep_corpus.py" 2>&1 | tee -a "$SUMMARY"

# ---------------- A: index search ----------------
if phase_in_only A; then
    echo -e "\n========== Phase A: linear regex vs TF-IDF index ==========\n" | tee -a "$SUMMARY"
    for SIZE in 50 100 500; do
        echo -e "\n[A.${SIZE}MB] tearing down + rebuilding cluster (independence)..." | tee -a "$SUMMARY"
        cluster_down
        cluster_up
        cluster_wait_healthy 120
        OUT="$RESULTS_DIR/A_index_${SIZE}MB.txt"
        (cd "$CODEBASE" && PYTHONPATH=. python scripts/bench_index_sizes.py --size "$SIZE" --out "$OUT") 2>&1 | tee -a "$SUMMARY"
    done
    cluster_down
fi

# ---------------- B: scan ----------------
if phase_in_only B; then
    echo -e "\n========== Phase B: regex vs trigram-accelerated scan ==========\n" | tee -a "$SUMMARY"
    for SIZE in 50 100 500; do
        echo -e "\n[B.${SIZE}MB] tearing down + rebuilding cluster (independence)..." | tee -a "$SUMMARY"
        cluster_down
        cluster_up
        cluster_wait_healthy 120
        OUT="$RESULTS_DIR/B_scan_${SIZE}MB.txt"
        (cd "$CODEBASE" && PYTHONPATH=. python scripts/bench_scan_sizes.py --size "$SIZE" --out "$OUT") 2>&1 | tee -a "$SUMMARY"
    done
    cluster_down
fi

# ---------------- C: upload throughput / matrix ----------------
if phase_in_only C; then
    echo -e "\n========== Phase C: upload + indexing throughput ==========\n" | tee -a "$SUMMARY"

    # C1: file-size × upload-workers matrix at default INDEXER_WORKERS=4
    for SIZE in $UPLOAD_SIZES; do
        # file selection: 50 MB / 100 MB / 500 MB
        case "$SIZE" in
            50)  FILE="$CODEBASE/data/wiki/big-text-files/2pptmlurtb.txt" ;;
            100) FILE="$CODEBASE/data/wiki/synthetic/100mb-concat.txt" ;;
            500) FILE="$CODEBASE/data/wiki/bigger-text-files/4xo3y9xm0n.txt" ;;
            *) echo "unknown size: $SIZE"; continue ;;
        esac
        for W in $UPLOAD_WORKERS; do
            echo -e "\n[C.${SIZE}MB.uw=${W}] teardown + rebuild..." | tee -a "$SUMMARY"
            flask_down
            cluster_down
            cluster_up
            cluster_wait_healthy 120
            flask_up
            OUT="$RESULTS_DIR/C_upload_${SIZE}MB_uw${W}.txt"
            {
                echo "Bench C · upload throughput vs index throughput"
                echo "size=${SIZE}MB  upload_workers=${W}  INDEXER_WORKERS=$(grep '^INDEXER_WORKERS' "$CODEBASE/common/config.py")  file=$(basename "$FILE")"
                echo "timestamp: $(date)"
                echo
                (cd "$CODEBASE" && PYTHONPATH=. python scripts/bench_upload.py \
                    --file "$FILE" --mode direct --workers "$W" --cleanup --name "bench-${SIZE}mb-uw${W}.txt")
            } 2>&1 | tee "$OUT" | tee -a "$SUMMARY"
        done
    done

    # C2: INDEXER_WORKERS variation at fixed upload_workers=4, single size (100 MB)
    if [[ "$SKIP_INDEX_VAR" != "1" ]]; then
        echo -e "\n[C.indexer-workers] varying INDEXER_WORKERS at fixed upload_workers=4, 100 MB..." | tee -a "$SUMMARY"
        CONFIG="$CODEBASE/common/config.py"
        # backup
        cp "$CONFIG" "$CONFIG.bak"
        trap 'mv "$CONFIG.bak" "$CONFIG" 2>/dev/null || true' EXIT
        for IW in $INDEX_WORKERS_LIST; do
            echo -e "\n[C.iw=${IW}] setting INDEXER_WORKERS=${IW}, full rebuild..." | tee -a "$SUMMARY"
            python -c "
import re, pathlib
p = pathlib.Path('$CONFIG.bak')
text = p.read_text()
text = re.sub(r'^INDEXER_WORKERS\s*=\s*\d+', f'INDEXER_WORKERS = $IW', text, flags=re.M)
pathlib.Path('$CONFIG').write_text(text)
"
            grep '^INDEXER_WORKERS' "$CONFIG" | tee -a "$SUMMARY"
            flask_down
            cluster_down
            # FORCE no-cache rebuild because the source file changed
            (docker compose -f "$CODEBASE/docker/docker-compose.yml" build --no-cache 2>&1 | tail -5 | sed 's/^/    /') | tee -a "$SUMMARY" || true
            cluster_up
            cluster_wait_healthy 150
            flask_up
            OUT="$RESULTS_DIR/C_indexvar_iw${IW}.txt"
            FILE="$CODEBASE/data/wiki/synthetic/100mb-concat.txt"
            {
                echo "Bench C2 · INDEXER_WORKERS variation"
                echo "INDEXER_WORKERS=${IW}  upload_workers=4  size=100MB"
                echo "timestamp: $(date)"
                echo
                (cd "$CODEBASE" && PYTHONPATH=. python scripts/bench_upload.py \
                    --file "$FILE" --mode direct --workers 4 --cleanup --name "bench-iw${IW}.txt")
            } 2>&1 | tee "$OUT" | tee -a "$SUMMARY"
        done
        # restore
        mv "$CONFIG.bak" "$CONFIG"
        trap - EXIT
        echo "[C.indexer-workers] restored common/config.py" | tee -a "$SUMMARY"
        # Rebuild image once more so subsequent phases use restored config
        (docker compose -f "$CODEBASE/docker/docker-compose.yml" build 2>&1 | tail -3 | sed 's/^/    /') || true
    fi

    flask_down
    cluster_down
fi

# ---------------- D: concurrent search ----------------
if phase_in_only D; then
    echo -e "\n========== Phase D: concurrent search load ==========\n" | tee -a "$SUMMARY"
    flask_down
    cluster_down
    cluster_up
    cluster_wait_healthy 120
    flask_up

    # Upload 4 × 50 MB files via direct mode (faster than Flask)
    BIG="$CODEBASE/data/wiki/big-text-files"
    FILES=()
    i=0
    for f in "$BIG"/*.txt; do
        if [ "$(stat -f%z "$f")" -gt 40000000 ]; then
            FILES+=("$f")
            i=$((i+1))
            [ "$i" -ge 4 ] && break
        fi
    done
    echo "[D] uploading 4 corpus files..." | tee -a "$SUMMARY"
    NAMES=()
    for src in "${FILES[@]}"; do
        n="search-$(basename "$src")"
        NAMES+=("$n")
        (cd "$CODEBASE" && PYTHONPATH=. python scripts/bench_upload.py \
            --file "$src" --mode direct --workers 4 --name "$n") 2>&1 \
            | tee -a "$SUMMARY"
    done

    OUT="$RESULTS_DIR/D_concurrent_search.txt"
    (cd "$CODEBASE" && PYTHONPATH=. python scripts/bench_concurrent_search.py \
        --files "${NAMES[@]}" --out "$OUT" \
        --levels 1 4 16 64 --per-worker 8) 2>&1 | tee -a "$SUMMARY"
    flask_down
    cluster_down
fi

echo -e "\n========== DONE ==========" | tee -a "$SUMMARY"
echo "Results dir: $RESULTS_DIR"
ls -la "$RESULTS_DIR" | tee -a "$SUMMARY"
