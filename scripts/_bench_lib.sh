#!/usr/bin/env bash
# Shared helpers for the benchmark suite. Source from other shell scripts.

set -euo pipefail

CODEBASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="$(cd "$CODEBASE/.." && pwd)"
COMPOSE=(docker compose -f "$CODEBASE/docker/docker-compose.yml")

# Fully tear the cluster down (containers + volumes) so each test starts cold.
cluster_down() {
    echo "[harness] tearing down cluster (with volumes)..."
    "${COMPOSE[@]}" down -v --remove-orphans 2>&1 | sed 's/^/    /' || true
}

# Build (re-uses cache; full rebuild only when Dockerfile / context changes) and start.
cluster_up() {
    echo "[harness] building + starting cluster..."
    "${COMPOSE[@]}" up -d --build 2>&1 | tail -20 | sed 's/^/    /'
}

# Block until all 4 containers are running, master is accepting gRPC, and all
# 3 chunkservers have heartbeated at least once.
cluster_wait_healthy() {
    local timeout="${1:-90}"
    echo "[harness] waiting for cluster ready (timeout ${timeout}s)..."
    local t0
    t0=$(date +%s)
    while true; do
        local elapsed=$(( $(date +%s) - t0 ))
        if [ "$elapsed" -gt "$timeout" ]; then
            echo "[harness] ERROR: cluster failed to become ready in ${timeout}s" >&2
            "${COMPOSE[@]}" ps 2>&1 | sed 's/^/    /' >&2
            return 1
        fi

        # Probe via the master ListFiles RPC — succeeds only when master is
        # accepting connections AND has seen its first heartbeats.
        if (cd "$CODEBASE" && PYTHONPATH=. python -c "
import grpc, sys
from common.config import GRPC_OPTIONS
from proto import master_pb2, master_pb2_grpc
try:
    ch = grpc.insecure_channel('localhost:5050', options=GRPC_OPTIONS)
    stub = master_pb2_grpc.MasterServiceStub(ch)
    stub.ListFiles(master_pb2.ListFilesRequest(), timeout=2)
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null); then
            # Now wait for 3 chunkservers to heartbeat. Easiest signal: each chunkserver
            # logs "[ChunkServer] starting on port" once gRPC server is up.
            local ready_count
            ready_count=$("${COMPOSE[@]}" ps --format '{{.State}}' 2>/dev/null | grep -c "running" || true)
            if [ "$ready_count" -ge 4 ]; then
                # Give the heartbeat thread a couple of seconds to ping
                sleep 6
                echo "[harness] cluster ready (after ${elapsed}s)"
                return 0
            fi
        fi
        sleep 2
    done
}

# Run an arbitrary command; teardown afterward
cluster_run_then_down() {
    cluster_down
    cluster_up
    cluster_wait_healthy 120
    "$@"
    local rc=$?
    cluster_down
    return $rc
}

FLASK_PID_FILE="/tmp/dfs_bench_flask.pid"

flask_up() {
    if flask_running; then
        echo "[harness] flask already running (pid $(cat $FLASK_PID_FILE))"
        return 0
    fi
    echo "[harness] starting flask client (background)..."
    (cd "$CODEBASE" && PYTHONPATH=. nohup python -m client.app >/tmp/dfs_bench_flask.log 2>&1 &
     echo $! > "$FLASK_PID_FILE")
    # wait until flask responds
    local t0
    t0=$(date +%s)
    while true; do
        if curl -sf -m 1 http://localhost:8080/api/files >/dev/null 2>&1; then
            echo "[harness] flask ready (pid $(cat $FLASK_PID_FILE))"
            return 0
        fi
        if [ "$(($(date +%s) - t0))" -gt 30 ]; then
            echo "[harness] ERROR: flask did not become ready in 30s" >&2
            return 1
        fi
        sleep 1
    done
}

flask_running() {
    [ -f "$FLASK_PID_FILE" ] && kill -0 "$(cat $FLASK_PID_FILE)" 2>/dev/null
}

flask_down() {
    if [ -f "$FLASK_PID_FILE" ]; then
        local pid
        pid=$(cat "$FLASK_PID_FILE")
        echo "[harness] stopping flask (pid $pid)..."
        # python -m client.app forks one worker. Kill the whole process group.
        pkill -P "$pid" 2>/dev/null || true
        kill "$pid" 2>/dev/null || true
        sleep 1
        # also kill any lingering child python -m client.app
        pkill -f "python -m client.app" 2>/dev/null || true
        rm -f "$FLASK_PID_FILE"
    fi
}
