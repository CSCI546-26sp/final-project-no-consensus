"""
End-to-end upload + indexing benchmark.

Run with the Docker stack already up:
    # User flow (POST /api/upload via Flask)
    python scripts/bench_upload.py --size 50 --mode flask

    # White-box (skip Flask, go direct to master + chunkservers via gRPC)
    python scripts/bench_upload.py --size 50 --mode direct

    # Use an existing file instead of synthesizing
    python scripts/bench_upload.py --file data/wiki/.../big.txt --mode direct

    # Clean up after
    python scripts/bench_upload.py --size 50 --mode direct --cleanup
"""

import argparse
import io
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import requests

from common.config import CHUNK_SIZE
from proto import master_pb2, master_pb2_grpc, chunkserver_pb2, chunkserver_pb2_grpc


CLIENT_URL = "http://localhost:8080"
MASTER_ADDRESS = "localhost:5050"
STREAM_SIZE = 512 * 1024
POLL_INTERVAL = 0.5
INDEX_TIMEOUT = 600
DEFAULT_WORKERS = 4

WORDS = (
    "the quick brown fox jumps over the lazy dog history of italy mussolini "
    "physics chemistry biology mathematics computer science network distributed "
    "system file storage replication chunkserver master indexing trigram inverted "
    "tokenization parallel concurrent thread heartbeat protocol benchmark latency "
    "throughput memory cpu disk network bandwidth gigabyte megabyte byte "
    "wikipedia article paragraph sentence word document corpus dataset query"
).split()


def synthesizeFile(sizeMb):
    target = sizeMb * 1024 * 1024
    rng = random.Random(42)
    out = io.BytesIO()
    while out.tell() < target:
        line = " ".join(rng.choice(WORDS) for _ in range(20)) + "\n"
        out.write(line.encode("utf-8"))
    return out.getvalue()[:target]


def translateAddress(addr):
    port = addr.split(":")[1]
    return f"localhost:{port}"


def benchmarkFlask(filename, data):
    t0 = time.perf_counter()
    resp = requests.post(
        f"{CLIENT_URL}/api/upload",
        files={"file": (filename, data, "text/plain")},
        timeout=INDEX_TIMEOUT,
    )
    uploadTime = time.perf_counter() - t0
    if resp.status_code != 200:
        raise RuntimeError(f"Upload failed ({resp.status_code}): {resp.text}")
    return uploadTime, resp.json().get("chunks", 0)


def benchmarkDirect(filename, data, workers):
    """Measure master.UploadFile, channel setup, and per-chunk WriteChunk separately.

    Chunks are uploaded in parallel via a ThreadPoolExecutor so that different
    primaries can be hit simultaneously. The per-chunk timings remain meaningful
    (each thread times its own RPC) but the *wall* time of the parallel section
    is what determines real throughput.
    """
    masterChan = grpc.insecure_channel(MASTER_ADDRESS)
    masterStub = master_pb2_grpc.MasterServiceStub(masterChan)

    t0 = time.perf_counter()
    upRes = masterStub.UploadFile(
        master_pb2.UploadFileRequest(filename=filename, file_size=len(data))
    )
    masterTime = time.perf_counter() - t0
    if not upRes.success:
        raise RuntimeError(f"UploadFile failed: {upRes.message}")

    def uploadOne(assignment):
        chunkIndex = assignment.chunk_index
        start = chunkIndex * CHUNK_SIZE
        chunkData = data[start:start + CHUNK_SIZE]

        addresses = list(assignment.server_addresses)
        primary = translateAddress(addresses[0])
        forwards = addresses[1:]

        t1 = time.perf_counter()
        chan = grpc.insecure_channel(primary)
        stub = chunkserver_pb2_grpc.ChunkServerServiceStub(chan)
        setupT = time.perf_counter() - t1

        def iterator():
            for i in range(0, len(chunkData), STREAM_SIZE):
                yield chunkserver_pb2.WriteChunkRequest(
                    chunk_handle=assignment.chunk_handle,
                    data=chunkData[i:i + STREAM_SIZE],
                    forward_addresses=forwards,
                )

        t2 = time.perf_counter()
        wresp = stub.WriteChunk(iterator())
        sendT = time.perf_counter() - t2

        if not wresp.success:
            raise RuntimeError(f"Chunk {chunkIndex} write failed: {wresp.message}")
        return setupT, sendT

    tWall = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(uploadOne, upRes.assignments))
    wallTime = time.perf_counter() - tWall

    setupTimes = [r[0] for r in results]
    sendTimes = [r[1] for r in results]
    return masterTime, setupTimes, sendTimes, wallTime, len(upRes.assignments)


def waitForIndexing(filename):
    t0 = time.perf_counter()
    last = (-1, -1)
    lastError = None
    errorReportedAt = 0.0
    while time.perf_counter() - t0 < INDEX_TIMEOUT:
        elapsed = time.perf_counter() - t0
        try:
            resp = requests.get(
                f"{CLIENT_URL}/api/index-status/{filename}", timeout=5
            )
            if resp.status_code == 200:
                body = resp.json()
                cur = (body["indexed_chunks"], body["total_chunks"])
                if cur != last:
                    print(f"  indexed {cur[0]}/{cur[1]}  (t={elapsed:.1f}s)", flush=True)
                    last = cur
                if body["done"]:
                    return time.perf_counter() - t0
                lastError = None
            else:
                err = f"HTTP {resp.status_code}: {resp.text[:200]}"
                if err != lastError or elapsed - errorReportedAt > 5:
                    print(f"  poll error (t={elapsed:.1f}s): {err}", flush=True)
                    lastError = err
                    errorReportedAt = elapsed
        except Exception as ex:
            err = f"{type(ex).__name__}: {ex}"
            if err != lastError or elapsed - errorReportedAt > 5:
                print(f"  poll exception (t={elapsed:.1f}s): {err}", flush=True)
                lastError = err
                errorReportedAt = elapsed
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"indexing didn't complete in {INDEX_TIMEOUT}s")


def stats(values):
    if not values:
        return "n/a"
    s = sorted(values)
    avg = sum(s) / len(s)
    return f"avg={avg*1000:.0f}ms  min={s[0]*1000:.0f}  p50={s[len(s)//2]*1000:.0f}  p95={s[int(len(s)*0.95)]*1000:.0f}  max={s[-1]*1000:.0f}"


def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--size", type=int, help="synthesize a file of N megabytes")
    src.add_argument("--file", type=str, help="upload an existing file from disk")
    ap.add_argument("--mode", choices=["flask", "direct"], default="flask")
    ap.add_argument("--cleanup", action="store_true")
    ap.add_argument("--name", type=str, default=None, help="override remote filename")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"parallel chunk uploads in direct mode (default {DEFAULT_WORKERS})")
    args = ap.parse_args()

    if args.file:
        with open(args.file, "rb") as f:
            data = f.read()
        sourceLabel = args.file
    else:
        print(f"Synthesizing {args.size} MB of text...")
        data = synthesizeFile(args.size)
        sourceLabel = f"synthetic ({args.size} MB)"

    actualMb = len(data) / (1024 * 1024)
    filename = args.name or f"bench-{int(actualMb)}mb-{int(time.time())}.txt"
    print(f"Source:   {sourceLabel}")
    print(f"Size:     {actualMb:.1f} MB")
    print(f"Filename: {filename}")
    print(f"Mode:     {args.mode}")
    print()

    if args.mode == "flask":
        uploadTime, chunks = benchmarkFlask(filename, data)
        print(f"Flask upload:  {uploadTime:.2f}s   throughput {actualMb/uploadTime:.1f} MB/s   chunks {chunks}")
        uploadTotal = uploadTime
    else:
        masterTime, setupTimes, sendTimes, wallTime, chunks = benchmarkDirect(
            filename, data, args.workers
        )
        uploadTotal = masterTime + wallTime
        print(f"master.UploadFile:    {masterTime*1000:.0f} ms")
        print(f"channel setup × {chunks}:   total {sum(setupTimes)*1000:.0f} ms   {stats(setupTimes)}")
        print(f"WriteChunk × {chunks} (per-thread): {stats(sendTimes)}")
        print(f"Parallel section wall ({args.workers}-way): {wallTime:.2f}s")
        print(f"Direct upload total:  {uploadTotal:.2f}s   throughput {actualMb/uploadTotal:.1f} MB/s")
    print()

    print("Waiting for indexing...")
    indexTime = waitForIndexing(filename)
    print(f"Indexing:     {indexTime:.2f}s   throughput {actualMb/indexTime:.1f} MB/s")
    print()
    print(f"=== Total wall (upload→searchable):  {uploadTotal + indexTime:.2f}s ===")

    if args.cleanup:
        try:
            requests.delete(f"{CLIENT_URL}/api/files/{filename}", timeout=10)
            print(f"Cleaned up {filename}")
        except Exception as e:
            print(f"Cleanup failed: {e}")


if __name__ == "__main__":
    main()
