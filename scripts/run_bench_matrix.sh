#!/bin/bash
set -e

# Bench matrix: runs scripts/bench_upload.py at multiple worker counts.
# Activates conda env "dfs". Run from any cwd.
#
# Override defaults with env vars:
#   SIZE=500 WORKERS_LIST="1 4 8" ./scripts/run_bench_matrix.sh

CODEBASE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$CODEBASE"

source /opt/anaconda3/etc/profile.d/conda.sh
conda activate dfs

WORKERS_LIST="${WORKERS_LIST:-1 4 8}"
SIZE="${SIZE:-500}"
RESULTS="bench_results_$(date +%Y%m%d_%H%M%S).txt"

{
  echo "Bench matrix: size=${SIZE}MB, workers={${WORKERS_LIST}}"
  echo "Started at $(date)"
  echo "INDEXER_WORKERS:"
  grep "^INDEXER_WORKERS" common/config.py
  echo "PIPELINE_QUEUE_MAXSIZE:"
  grep "^PIPELINE_QUEUE_MAXSIZE" common/config.py
  echo ""
} | tee "$RESULTS"

for W in $WORKERS_LIST; do
  echo "=================================================" | tee -a "$RESULTS"
  echo "  workers=$W   size=${SIZE}MB" | tee -a "$RESULTS"
  echo "=================================================" | tee -a "$RESULTS"
  PYTHONPATH=. python scripts/bench_upload.py \
    --size "$SIZE" --mode direct --workers "$W" --cleanup \
    2>&1 | tee -a "$RESULTS"
  echo "" | tee -a "$RESULTS"
  sleep 5
done

echo "Done. Full log: $RESULTS"
